/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.drelephant.tuning;

import com.avaje.ebean.Expr;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.tuning.obt.OptimizationAlgoFactory;
import com.linkedin.drelephant.tuning.obt.ParameterGenerateManagerOBT;
import com.linkedin.drelephant.util.Utils;
import controllers.AutoTuningMetricsController;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import models.FlowDefinition;
import models.FlowExecution;
import models.JobDefinition;
import models.JobExecution;
import models.JobExecution.ExecutionState;
import models.JobSuggestedParamSet;
import models.JobSuggestedParamSet.ParamSetStatus;
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningAlgorithm.OptimizationAlgo;
import models.TuningJobDefinition;
import models.TuningJobExecutionParamSet;
import models.TuningParameter;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;


/**
 * This class processes the API requests and returns param suggestion as response
 */
public class AutoTuningAPIHelper {

  private static final String ALLOWED_MAX_RESOURCE_USAGE_PERCENT_DEFAULT =
      "autotuning.default.allowed_max_resource_usage_percent";
  private static final String ALLOWED_MAX_EXECUTION_TIME_PERCENT_DEFAULT =
      "autotuning.default.allowed_max_execution_time_percent";
  private static final Logger logger = Logger.getLogger(AutoTuningAPIHelper.class);
  boolean debugEnabled = logger.isDebugEnabled();

  /**
   * For a job, returns the best parameter set of the given job if it exists else  the default parameter set
   * @param jobDefId Sting JobDefId of the job
   * @return JobSuggestedParamSet the best parameter set of the given job if it exists else  the default parameter set
   */
  private JobSuggestedParamSet getBestParamSet(String jobDefId) {
    JobSuggestedParamSet jobSuggestedParamSetBestParamSet = JobSuggestedParamSet.find.select("*")
        .where()
        .eq(JobSuggestedParamSet.TABLE.jobDefinition + "." + JobDefinition.TABLE.jobDefId, jobDefId)
        .eq(JobSuggestedParamSet.TABLE.isParamSetBest, true)
        .setMaxRows(1)
        .findUnique();

    if (jobSuggestedParamSetBestParamSet == null) {
      jobSuggestedParamSetBestParamSet = JobSuggestedParamSet.find.select("*")
          .where()
          .eq(JobSuggestedParamSet.TABLE.jobDefinition + "." + JobDefinition.TABLE.jobDefId, jobDefId)
          .eq(JobSuggestedParamSet.TABLE.isParamSetDefault, true)
          .setMaxRows(1)
          .findUnique();
    }
    return jobSuggestedParamSetBestParamSet;
  }

  /**
   * For a job, returns the Manually tuned param set
   * @param jobDefId Sting JobDefId of the job
   * @return JobSuggestedParamSet the best parameter set of the given job if it exists else  the default parameter set
   */
  private JobSuggestedParamSet getManuallyTunedParamSet(String jobDefId) {
    JobSuggestedParamSet jobSuggestedParamSetBestParamSet = JobSuggestedParamSet.find.select("*")
        .where()
        .eq(JobSuggestedParamSet.TABLE.jobDefinition + "." + JobDefinition.TABLE.jobDefId, jobDefId)
        .eq(JobSuggestedParamSet.TABLE.isManuallyOverridenParameter, true)
        .setMaxRows(1)
        .findUnique();
    return jobSuggestedParamSetBestParamSet;
  }

  /**
   * Returns the param values corresponding to the given param set id
   * @param paramSetId Long parameter set id
   * @return List<JobSuggestedParamValue> list of parameters
   */
  private List<JobSuggestedParamValue> getParamSetValues(Long paramSetId) {
    List<JobSuggestedParamValue> jobSuggestedParamValues = JobSuggestedParamValue.find.where()
        .eq(JobSuggestedParamValue.TABLE.jobSuggestedParamSet + '.' + JobSuggestedParamSet.TABLE.id, paramSetId)
        .findList();
    return jobSuggestedParamValues;
  }

  /**
   * Sets the max allowed increase percentage for metrics: execution time and resource usage if not provided in API call
   * @param tuningInput TuningInput
   */
  private void setMaxAllowedMetricIncreasePercentage(TuningInput tuningInput) {
    Configuration configuration = ElephantContext.instance().getAutoTuningConf();
    if (tuningInput.getAllowedMaxExecutionTimePercent() == null) {
      Double allowedMaxExecutionTimePercent =
          new Double(Utils.getNonNegativeInt(configuration, ALLOWED_MAX_EXECUTION_TIME_PERCENT_DEFAULT, 150));
      tuningInput.setAllowedMaxExecutionTimePercent(allowedMaxExecutionTimePercent);
    }
    if (tuningInput.getAllowedMaxResourceUsagePercent() == null) {
      Double allowedMaxResourceUsagePercent =
          new Double(Utils.getNonNegativeInt(configuration, ALLOWED_MAX_RESOURCE_USAGE_PERCENT_DEFAULT, 150));
      tuningInput.setAllowedMaxResourceUsagePercent(allowedMaxResourceUsagePercent);
    }
  }

  /**
   * Applies penalty to the param set corresponding to the given execution
   * @param jobExecId String job execution id/url of the execution whose parameter set has to be penalized
   * Assumption: Best param set will never be penalized
   */
  private void applyPenalty(String jobExecId) {
    Integer penaltyConstant = 3;
    logger.info("Execution " + jobExecId + " failed/cancelled. Applying penalty");

    TuningJobExecutionParamSet tuningJobExecutionParamSet = TuningJobExecutionParamSet.find.where()
        .eq(TuningJobExecutionParamSet.TABLE.jobExecution + '.' + JobExecution.TABLE.jobExecId, jobExecId)
        .setMaxRows(1)
        .findUnique();

    JobSuggestedParamSet jobSuggestedParamSet = tuningJobExecutionParamSet.jobSuggestedParamSet;
    JobExecution jobExecution = tuningJobExecutionParamSet.jobExecution;
    JobDefinition jobDefinition = jobExecution.job;

    TuningJobDefinition tuningJobDefinition = TuningJobDefinition.find.where()
        .eq(TuningJobDefinition.TABLE.job + '.' + JobDefinition.TABLE.id, jobDefinition.id)
        .findUnique();
    /**
     * Deferring applying penalty to FitnessCompute. Based on the exception
     * fingerprinting outcome , it will be decided whether to apply penalty or not
     * todo: Do the same logic for OBT .
     */
    if (tuningJobDefinition.tuningAlgorithm.optimizationAlgo == OptimizationAlgo.HBT) {
      //For HBT we are using score as fitness. setting it to high value
      //  jobSuggestedParamSet.fitness = 10000D;
      tuningJobExecutionParamSet.isRetried = true;
      tuningJobExecutionParamSet.save();
    }
    else {
      Double averageResourceUsagePerGBInput =
          tuningJobDefinition.averageResourceUsage * FileUtils.ONE_GB / tuningJobDefinition.averageInputSizeInBytes;
      Double maxDesiredResourceUsagePerGBInput =
          averageResourceUsagePerGBInput * tuningJobDefinition.allowedMaxResourceUsagePercent / 100.0;
      jobSuggestedParamSet.fitness = penaltyConstant * maxDesiredResourceUsagePerGBInput;
      jobSuggestedParamSet.paramSetState = ParamSetStatus.FITNESS_COMPUTED;
      jobSuggestedParamSet.fitnessJobExecution = jobExecution;
      jobSuggestedParamSet.update();
      jobExecution.resourceUsage = 0D;
      jobExecution.executionTime = 0D;
      jobExecution.inputSizeInBytes = 1D;
      jobExecution.save();
    }
  }

  /**
   * Returns flow definition corresponding to the given tuning input if it exists, else creates one and returns it
   * @param tuningInput TuningInput containing the flow definition id corresponding to which flow definition
   *                    is to be returned
   * @return FlowDefinition flow definition
   */
  private FlowDefinition getFlowDefinition(TuningInput tuningInput) {
    FlowDefinition flowDefinition =
        FlowDefinition.find.where().eq(FlowDefinition.TABLE.flowDefId, tuningInput.getFlowDefId()).findUnique();
    if (flowDefinition == null) {
      flowDefinition = new FlowDefinition();
      flowDefinition.flowDefId = tuningInput.getFlowDefId();
      flowDefinition.flowDefUrl = tuningInput.getFlowDefUrl();
      flowDefinition.save();
    }
    return flowDefinition;
  }

  /**
   * Returns flow execution corresponding to the given tuning input if it exists, else creates one and returns it
   * @param tuningInput TuningInput containing the flow execution id corresponding to which flow execution
   *                    is to be returned
   * @return FlowExecution flow execution
   */
  @VisibleForTesting
   FlowExecution getFlowExecution(TuningInput tuningInput) {
    if (debugEnabled) {
      logger.debug(" Thread name " + Thread.currentThread().getName());
    }
    synchronized (AutoTuningAPIHelper.class) {
      if (debugEnabled) {
        logger.debug(" Thread inside synchronize block " + Thread.currentThread().getName());
      }
      FlowExecution flowExecution =
          FlowExecution.find.where().eq(FlowExecution.TABLE.flowExecId, tuningInput.getFlowExecId()).findUnique();
      if (flowExecution == null) {
        logger.info("Creating flow execution for " + tuningInput.getJobExecId());
        flowExecution = new FlowExecution();
        flowExecution.flowExecId = tuningInput.getFlowExecId();
        flowExecution.flowExecUrl = tuningInput.getFlowExecUrl();
        flowExecution.flowDefinition = getFlowDefinition(tuningInput);
        flowExecution.save();
      }
      if (debugEnabled) {
        logger.debug(" Thread exiting synchronize block " + Thread.currentThread().getName());
      }
      return flowExecution;
    }
  }

  /**
   * Adds new job for tuning
   * @param tuningInput Tuning input parameters
   * @return Job
   */
  private TuningJobDefinition addNewJobForTuning(TuningInput tuningInput) {
    logger.info("Adding new job for tuning, job id: " + tuningInput.getJobDefId());
    FlowDefinition flowDefinition = getFlowDefinition(tuningInput);
    JobDefinition job =
        JobDefinition.find.select("*").where().eq(JobDefinition.TABLE.jobDefId, tuningInput.getJobDefId()).findUnique();

    if (job == null) {
      job = new JobDefinition();
      job.jobDefId = tuningInput.getJobDefId();
      job.scheduler = tuningInput.getScheduler();
      job.username = tuningInput.getUserName();
      job.jobName = tuningInput.getJobName();
      job.jobDefUrl = tuningInput.getJobDefUrl();
      job.flowDefinition = flowDefinition;
      job.save();
    }
    String client = tuningInput.getClient();
    Map<String, Double> defaultParams = null;
    try {
      defaultParams = tuningInput.getDefaultParams();
      logger.info(" Default parameters first time " + defaultParams);
    } catch (IOException e) {
      logger.error("Error in getting default parameters from request. ", e);
    }
    TuningJobDefinition tuningJobDefinition = new TuningJobDefinition();
    tuningJobDefinition.job = job;
    tuningJobDefinition.client = client;
    tuningJobDefinition.tuningAlgorithm = getTuningAlgorithmForfirstTime(tuningInput);
    tuningJobDefinition.tuningEnabled = true;
    tuningJobDefinition.allowedMaxExecutionTimePercent = tuningInput.getAllowedMaxExecutionTimePercent();
    tuningJobDefinition.allowedMaxResourceUsagePercent = tuningInput.getAllowedMaxResourceUsagePercent();
    tuningJobDefinition.save();
    insertParamSet(job, tuningInput.getTuningAlgorithm(), defaultParams);
    logger.info("Added job: " + tuningInput.getJobDefId() + " for tuning");
    return tuningJobDefinition;
  }

  /**
   * This method will be used to get tuning algorithm for the first execution of the job.
   * Currently for backward compatibility the tuning algorithm is Azkaban version depended.
   * (See Version in Application.java)
   * .
   * But in future HBT ,will be the by default tuning algorithm for first time.
   * .eq(TuningAlgorithm.TABLE.optimizationAlgo, "HBT")
   * @param tuningInput
   * @return
   */
  private TuningAlgorithm getTuningAlgorithmForfirstTime(TuningInput tuningInput) {
    logger.info("Version " + tuningInput.getVersion() + "\t" + tuningInput.getOptimizationAlgo());
    TuningAlgorithm tuningAlgorithm = TuningAlgorithm.find.select("*")
        .where()
        .eq(TuningAlgorithm.TABLE.jobType, tuningInput.getJobType())
        .eq(TuningAlgorithm.TABLE.optimizationMetric, "RESOURCE")
        .eq(TuningAlgorithm.TABLE.optimizationAlgo, tuningInput.getOptimizationAlgo())
        .findUnique();
    if (tuningAlgorithm == null) {
      throw new IllegalArgumentException("Wrong job type " + tuningInput.getJobType());
    }
    tuningInput.setTuningAlgorithm(tuningAlgorithm);
    return tuningAlgorithm;
  }

  /**
   * Returns the job definition corresponding to the given tuning input if it exists, else creates one and returns it
   * @param tuningInput Tuning Input corresponding to which job definition is to be returned
   * @return JobDefinition corresponding to the given tuning input
   */
  private JobDefinition getJobDefinition(TuningInput tuningInput) {

    String jobDefId = tuningInput.getJobDefId();

    TuningJobDefinition tuningJobDefinition = TuningJobDefinition.find.select("*")
        .fetch(TuningJobDefinition.TABLE.job, "*")
        .where()
        .eq(TuningJobDefinition.TABLE.job + "." + JobDefinition.TABLE.jobDefId, jobDefId)
        .setMaxRows(1)
        .orderBy(TuningJobDefinition.TABLE.createdTs + " desc")
        .findUnique();

    if (tuningJobDefinition == null) {
      // Job new to tuning
      logger.debug("Registering job: " + tuningInput.getJobName() + " for auto tuning tuning");
      AutoTuningMetricsController.markNewAutoTuningJob();
      tuningJobDefinition = addNewJobForTuning(tuningInput);
    }
    return tuningJobDefinition.job;
  }

  /**
   * Creates a new job execution entry corresponding to the given tuning input
   * @param tuningInput Input corresponding to which job execution is to be created
   * @return JobExecution: the newly created job execution
   */
  private JobExecution addNewExecution(TuningInput tuningInput) {
    JobDefinition jobDefinition = getJobDefinition(tuningInput);
    FlowExecution flowExecution = getFlowExecution(tuningInput);
    JobExecution jobExecution = new JobExecution();
    jobExecution.jobExecId = tuningInput.getJobExecId();
    jobExecution.jobExecUrl = tuningInput.getJobExecUrl();
    jobExecution.job = jobDefinition;
    jobExecution.executionState = ExecutionState.IN_PROGRESS;
    jobExecution.flowExecution = flowExecution;
    jobExecution.save();
    return jobExecution;
  }

  /**
   * Returns the job execution corresponding to the given tuning input if it exists, else creates one and returns it
   * @param tuningInput Tuning Input corresponding to which job execution is to be returned
   * @return JobExecution corresponding to the given tuning input
   */
  private JobExecution getJobExecution(TuningInput tuningInput) {
    JobExecution jobExecution = JobExecution.find.select("*")
        .fetch(JobExecution.TABLE.job, "*")
        .where()
        .eq(JobExecution.TABLE.jobExecId, tuningInput.getJobExecId())
        .findUnique();

    if (jobExecution == null) {
      logger.info(" New job execution is added "+tuningInput.getJobExecId());
      jobExecution = addNewExecution(tuningInput);
    }
    return jobExecution;
  }

  /**
   * Handles the api request and returns param suggestions as response
   * @param tuningInput Rest api parameters
   * @return Parameter Suggestion
   */
  public Map<String, Double> getCurrentRunParameters(TuningInput tuningInput) throws Exception {
    logger.info("Parameter set request received from execution: " + tuningInput.getJobExecId());
    if (tuningInput.getAllowedMaxExecutionTimePercent() == null
        || tuningInput.getAllowedMaxResourceUsagePercent() == null) {
      setMaxAllowedMetricIncreasePercentage(tuningInput);
    }
    String jobDefId = tuningInput.getJobDefId();
    logger.info(" Job definition id is "+jobDefId);
    TuningJobDefinition tuningJobDefinition = TuningJobDefinition.find.select("*")
        .fetch(TuningJobDefinition.TABLE.job, "*")
        .where()
        .eq(TuningJobDefinition.TABLE.job + "." + JobDefinition.TABLE.jobDefId, jobDefId)
        .setMaxRows(1)
        .orderBy(TuningJobDefinition.TABLE.createdTs + " desc")
        .findUnique();
    JobExecution jobExecution = getJobExecution(tuningInput);
    if (tuningJobDefinition == null) {
      tuningInput.setTuningAlgorithm(getTuningAlgorithmForfirstTime(tuningInput));
      logger.info(" Tuning Algorithm Type " + tuningInput.getTuningAlgorithm().optimizationAlgo.name());
      initializeOptimizationAlgoPrerequisite(tuningInput);
      return processForFirstExecution(tuningInput, jobExecution);
    } else {
      tuningInput.setTuningAlgorithm(tuningJobDefinition.tuningAlgorithm);
      logger.info(" Tuning Algorithm Type " + tuningInput.getTuningAlgorithm().optimizationAlgo.name());
      initializeOptimizationAlgoPrerequisite(tuningInput);
      return processForSubsequentExecutions(tuningJobDefinition, tuningInput, jobExecution);
    }
  }

  /**
   * This method will be called for the first execution of the job
   * @param tuningInput
   * @param jobExecution
   * @return Map of property name and value
   */
  private Map<String, Double> processForFirstExecution(TuningInput tuningInput, JobExecution jobExecution) {
    logger.info(
        " New Job. Hence  Not checking for AutoTuning . Running with default Parameters " + tuningInput.getJobExecId());
    JobSuggestedParamSet jobSuggestedParamSet = getDefaultParameters(jobExecution.job);
    markParameterSetSent(jobSuggestedParamSet);
    addNewTuningJobExecutionParamSet(jobSuggestedParamSet, jobExecution);
    List<JobSuggestedParamValue> jobSuggestedParamValues = getParamSetValues(jobSuggestedParamSet.id);
    logger.debug("Number of output parameters for execution " + tuningInput.getJobExecId() + " = "
        + jobSuggestedParamValues.size());
    logger.info("Finishing getCurrentRunParameters");
    return new HashMap<String, Double>();
  }

  /**
   * This method will be called for all the next execution of the job (except the first one). Execution path would
   * be different for first execution and subsequent execution . Since user cannot enable auto apply before first execution
   * . If user doesn't enable after seeing in Dr elephant , then suggested parameters will not be applied instead default
   * parameters will be applied.
   * @param tuningJobDefinition
   * @param tuningInput
   * @param jobExecution
   * @return
   */
  private Map<String, Double> processForSubsequentExecutions(TuningJobDefinition tuningJobDefinition,
      TuningInput tuningInput, JobExecution jobExecution) {
    logger.info(" Not a New Job . Hence check for autoTuning" + tuningInput.getJobExecId());
    if (tuningJobDefinition.autoApply) {
      logger.info(" Auto Tuning Enabled . Hence Apply generated Parameter ,generated based on tuning algorithm");
      List<JobSuggestedParamValue> jobSuggestedParamValues = processParameterTuningEnabled(tuningInput, jobExecution);
      return jobSuggestedParamValueListToMap(jobSuggestedParamValues);
    } else {
      logger.info("Not a new Job . Auto Tuning Disabled . Send default parameters");
      return sendDefaultParameters(tuningInput, jobExecution);
    }
  }

  /**
   * If auto apply is enabled , then get the suggested parameters . Retry if the job is failed .
   * @param tuningInput
   * @param jobExecution
   * @return
   */
  private List<JobSuggestedParamValue> processParameterTuningEnabled(TuningInput tuningInput,
      JobExecution jobExecution) {
    JobSuggestedParamSet jobSuggestedParamSet;
    logger.debug("Finding parameter suggestion for job: " + jobExecution.job.jobName);
    if (tuningInput.getRetry()) {
      logger.info(" Retry ");
      applyPenalty(tuningInput.getJobExecId());
      jobSuggestedParamSet = getBestParamSet(tuningInput.getJobDefId());
    } else {
      logger.debug("Finding parameter suggestion for job: " + jobExecution.job.jobName);
      jobSuggestedParamSet = getNewSuggestedParamSet(jobExecution.job, tuningInput.getTuningAlgorithm());
      markParameterSetSent(jobSuggestedParamSet);
    }
    addNewTuningJobExecutionParamSet(jobSuggestedParamSet, jobExecution);
    List<JobSuggestedParamValue> jobSuggestedParamValues = getParamSetValues(jobSuggestedParamSet.id);
    logger.debug("Number of output parameters for execution " + tuningInput.getJobExecId() + " = "
        + jobSuggestedParamValues.size());
    logger.info("Finishing getCurrentRunParameters");
    return jobSuggestedParamValues;
  }

  /**
   * Since Auto Tuning disabled , no parameter suggestion .
   * @param tuningInput
   * @param jobExecution
   * @return
   */
  private Map<String, Double> sendDefaultParameters(TuningInput tuningInput, JobExecution jobExecution) {
    JobSuggestedParamSet jobSuggestedParamSet;
    logger.info(" Auto Tuning Disabled . Hence no parameter suggestion. Tagging execution with default values");
    updateDataBaseWithDefaultValues(tuningInput);
    jobSuggestedParamSet = getDefaultParameters(jobExecution.job);
    markParameterSetSent(jobSuggestedParamSet);
    addNewTuningJobExecutionParamSet(jobSuggestedParamSet, jobExecution);
    discardGeneratedParameter(jobExecution.job);
    return new HashMap<String, Double>();
  }

  /**
   * Update the data base with default values of the parameters
   * @param tuningInput
   */
  public void updateDataBaseWithDefaultValues(TuningInput tuningInput) {
    JobDefinition job =
        JobDefinition.find.select("*").where().eq(JobDefinition.TABLE.jobDefId, tuningInput.getJobDefId()).findUnique();
    Map<String, Double> defaultParams = null;
    try {
      defaultParams = tuningInput.getDefaultParams();
      logger.info("Default values " + defaultParams);
    } catch (IOException e) {
      logger.error("Error in getting default parameters from request. ", e);
    }
    insertParamSetForDefault(job, tuningInput.getTuningAlgorithm(), defaultParams);
  }

  /**
   * Get the default parameters which will be send .
   * @param jobDefinition
   * @return
   */
  public JobSuggestedParamSet getDefaultParameters(JobDefinition jobDefinition) {
    JobSuggestedParamSet jobSuggestedParamSet = JobSuggestedParamSet.find.select("*")
        .fetch(JobSuggestedParamSet.TABLE.jobDefinition, "*")
        .where()
        .eq(JobSuggestedParamSet.TABLE.jobDefinition + "." + JobDefinition.TABLE.id, jobDefinition.id)
        .eq(JobSuggestedParamSet.TABLE.paramSetState, ParamSetStatus.CREATED)
        .order()
        .desc(JobSuggestedParamSet.TABLE.id)
        .setMaxRows(1)
        .findUnique();
    return jobSuggestedParamSet;
  }

  /**
   * If auto apply is disabled then , we have to discard all the generated parameters
   * and send default values to the execution.
   * @param jobDefinition
   */
  public void discardGeneratedParameter(JobDefinition jobDefinition) {
    List<JobSuggestedParamSet> jobSuggestedParamSets = JobSuggestedParamSet.find.select("*")
        .fetch(JobSuggestedParamSet.TABLE.jobDefinition, "*")
        .where()
        .eq(JobSuggestedParamSet.TABLE.jobDefinition + "." + JobDefinition.TABLE.id, jobDefinition.id)
        .eq(JobSuggestedParamSet.TABLE.paramSetState, ParamSetStatus.CREATED)
        .findList();
    if (jobSuggestedParamSets != null && jobSuggestedParamSets.size() >= 1) {
      logger.info(" Discarding Generated Parameters , as auto tuning off." + jobSuggestedParamSets.size());
      for (JobSuggestedParamSet jobSuggestedParamSet : jobSuggestedParamSets) {
        jobSuggestedParamSet.paramSetState = ParamSetStatus.DISCARDED;
        jobSuggestedParamSet.update();
      }
    }
  }

  /**
   * Inserted data in JobSuggestedParamSet .
   * @param job
   * @param tuningAlgorithm
   * @param paramValueMap
   */
  private void insertParamSetForDefault(JobDefinition job, TuningAlgorithm tuningAlgorithm,
      Map<String, Double> paramValueMap) {
    logger.debug("Inserting default parameter set for job: " + job.jobName);
    JobSuggestedParamSet jobSuggestedParamSet = new JobSuggestedParamSet();
    jobSuggestedParamSet.jobDefinition = job;
    jobSuggestedParamSet.tuningAlgorithm = tuningAlgorithm;
    jobSuggestedParamSet.paramSetState = ParamSetStatus.CREATED;
    jobSuggestedParamSet.isParamSetDefault = false;
    jobSuggestedParamSet.areConstraintsViolated = false;
    jobSuggestedParamSet.isParamSetBest = false;
    jobSuggestedParamSet.isManuallyOverridenParameter = false;
    jobSuggestedParamSet.isParamSetSuggested = false;
    jobSuggestedParamSet.save();
    insertParameterValues(jobSuggestedParamSet, paramValueMap, tuningAlgorithm);
    logger.debug("Default parameter set inserted for job: " + job.jobName);
  }

  /**
   * Adds a new entry to the "tuning_job_execution_param_set"  for the given param set and job execution
   * @param jobSuggestedParamSet JobSuggestedParamSet: param set
   * @param jobExecution JobExecution
   */
  private void addNewTuningJobExecutionParamSet(JobSuggestedParamSet jobSuggestedParamSet, JobExecution jobExecution) {
    TuningJobExecutionParamSet tuningJobExecutionParamSet = new TuningJobExecutionParamSet();
    tuningJobExecutionParamSet.jobSuggestedParamSet = jobSuggestedParamSet;
    tuningJobExecutionParamSet.jobExecution = jobExecution;

    TuningJobDefinition tuningJobDefinition = TuningJobDefinition.find.where()
        .eq(TuningJobDefinition.TABLE.job + '.' + JobDefinition.TABLE.id, jobExecution.job.id)
        .order()
        .desc(TuningJobDefinition.TABLE.createdTs)
        .setMaxRows(1)
        .findUnique();
    tuningJobExecutionParamSet.tuningEnabled = tuningJobDefinition.tuningEnabled;
    tuningJobExecutionParamSet.save();
  }

  /**
   * Returns a parameter set in "CREATED" state corresponding to the given job definition if it exists, else returns
   * the best parameter set
   * @param jobDefinition jobDefinition for which param set is to be returned
   * @return JobSuggestedParamSet corresponding to the given job definition
   */
  private JobSuggestedParamSet getNewSuggestedParamSet(JobDefinition jobDefinition, TuningAlgorithm tuningAlgorithm) {
    JobSuggestedParamSet jobSuggestedParamSet = getManuallyTunedParamSet(jobDefinition.jobDefId);
    if (jobSuggestedParamSet == null) {
      logger.info(" No Manually overriden parameter ");
      makeOtherAlgoParamGeneratedDiscarded(jobDefinition, tuningAlgorithm);

      jobSuggestedParamSet = JobSuggestedParamSet.find.select("*")
          .fetch(JobSuggestedParamSet.TABLE.jobDefinition, "*")
          .where()
          .eq(JobSuggestedParamSet.TABLE.jobDefinition + "." + JobDefinition.TABLE.id, jobDefinition.id)
          .eq(JobSuggestedParamSet.TABLE.paramSetState, ParamSetStatus.CREATED)
          .eq(JobSuggestedParamSet.TABLE.tuningAlgorithm, tuningAlgorithm)
          .order()
          .asc(JobSuggestedParamSet.TABLE.id)
          .setMaxRows(1)
          .findUnique();

      if (jobSuggestedParamSet == null) {
        logger.info("Returning best parameter set as no parameter suggestion found for job: " + jobDefinition.jobName);
        jobSuggestedParamSet = getBestParamSet(jobDefinition.jobDefId);
      }
    }
    return jobSuggestedParamSet;
  }

  /**
   * If the algorithm type is changed from HBT to OBT or vice versa ,
   * then discard all the parameters which was generated by  previous algorithm type.
   * @param jobDefinition
   * @param tuningAlgorithm
   */
  private void makeOtherAlgoParamGeneratedDiscarded(JobDefinition jobDefinition, TuningAlgorithm tuningAlgorithm) {
    logger.info("Making other tuning type suggested as discarded , if tuning type is changed");
    List<JobSuggestedParamSet> jobSuggestedParamSets = JobSuggestedParamSet.find.select("*")
        .fetch(JobSuggestedParamSet.TABLE.jobDefinition, "*")
        .where()
        .eq(JobSuggestedParamSet.TABLE.jobDefinition + "." + JobDefinition.TABLE.id, jobDefinition.id)
        .eq(JobSuggestedParamSet.TABLE.paramSetState, ParamSetStatus.CREATED)
        .not(Expr.eq(JobSuggestedParamSet.TABLE.tuningAlgorithm, tuningAlgorithm))
        .findList();
    if (jobSuggestedParamSets != null && jobSuggestedParamSets.size() > 0) {
      logger.info(" other algorithm parameter created " + jobSuggestedParamSets.size());
      for (JobSuggestedParamSet jobSuggestedParamSet : jobSuggestedParamSets) {
        jobSuggestedParamSet.paramSetState = ParamSetStatus.DISCARDED;
        jobSuggestedParamSet.save();
      }
    }
  }

  /**
   * Returns the list of JobSuggestedParamValue as Map of String to Double
   * @param jobSuggestedParamValues List of JobSuggestedParamValue
   * @return Map of string to double containing the parameter name and corresponding value
   */
  private Map<String, Double> jobSuggestedParamValueListToMap(List<JobSuggestedParamValue> jobSuggestedParamValues) {
    Map<String, Double> paramValues = new HashMap<String, Double>();
    if (jobSuggestedParamValues != null) {
      for (JobSuggestedParamValue jobSuggestedParamValue : jobSuggestedParamValues) {
        logger.debug("Param Name is " + jobSuggestedParamValue.tuningParameter.paramName + " And value is "
            + jobSuggestedParamValue.paramValue);
        paramValues.put(jobSuggestedParamValue.tuningParameter.paramName, jobSuggestedParamValue.paramValue);
      }
    }
    return paramValues;
  }

  /**
   *Updates parameter set state to SENT if it is in CREATED state
   * @param jobSuggestedParamSet JobSuggestedParamSet which is to be updated
   */
  private void markParameterSetSent(JobSuggestedParamSet jobSuggestedParamSet) {
    if (jobSuggestedParamSet.paramSetState.equals(ParamSetStatus.CREATED)) {
      logger.info("Marking paramSetID: " + jobSuggestedParamSet.id + " SENT");
      jobSuggestedParamSet.paramSetState = ParamSetStatus.SENT;
      jobSuggestedParamSet.save();
    }
  }

  /**
   * Inserts a parameter set in database
   * @param job Job
   */
  private void insertParamSet(JobDefinition job, TuningAlgorithm tuningAlgorithm, Map<String, Double> paramValueMap) {
    logger.debug("Inserting default parameter set for job: " + job.jobName);
    JobSuggestedParamSet jobSuggestedParamSet = new JobSuggestedParamSet();
    jobSuggestedParamSet.jobDefinition = job;
    jobSuggestedParamSet.tuningAlgorithm = tuningAlgorithm;
    jobSuggestedParamSet.paramSetState = ParamSetStatus.CREATED;
    jobSuggestedParamSet.isParamSetDefault = true;
    jobSuggestedParamSet.areConstraintsViolated = false;
    jobSuggestedParamSet.isParamSetBest = false;
    jobSuggestedParamSet.isManuallyOverridenParameter = false;
    jobSuggestedParamSet.isParamSetSuggested = false;
    jobSuggestedParamSet.save();
    insertParameterValues(jobSuggestedParamSet, paramValueMap, tuningAlgorithm);
    logger.debug("Default parameter set inserted for job: " + job.jobName);
  }

  /**
   * Inserts parameter values in database
   * @param jobSuggestedParamSet Set of the parameters which is to be inserted
   * @param paramValueMap Map of parameter values as string
   */
  @SuppressWarnings("unchecked")
  private void insertParameterValues(JobSuggestedParamSet jobSuggestedParamSet, Map<String, Double> paramValueMap,
      TuningAlgorithm tuningAlgorithm) {
    ObjectMapper mapper = new ObjectMapper();
    if (paramValueMap != null) {
      for (Map.Entry<String, Double> paramValue : paramValueMap.entrySet()) {
        insertParameterValue(jobSuggestedParamSet, paramValue.getKey(), paramValue.getValue(), tuningAlgorithm);
      }
    } else {
      logger.warn("ParamValueMap is null ");
    }
  }

  /**
   * This is specifically used in IPSO ,where tuning parameter constraint
   * table is populated with default boundaries of the parameters .
   * @param tuningInput
   */
  private void initializeOptimizationAlgoPrerequisite(TuningInput tuningInput) {
    String jobDefId = tuningInput.getJobDefId();
    TuningJobDefinition tuningJobDefinition = TuningJobDefinition.find.select("*")
        .fetch(TuningJobDefinition.TABLE.job, "*")
        .where()
        .eq(TuningJobDefinition.TABLE.job + "." + JobDefinition.TABLE.jobDefId, jobDefId)
        .setMaxRows(1)
        .orderBy(TuningJobDefinition.TABLE.createdTs + " desc")
        .findUnique();
    TuningAlgorithm tuningAlgorithm = tuningInput.getTuningAlgorithm();
    logger.info("Inserting parameter constraint " + tuningAlgorithm.optimizationAlgo.name());
    ParameterGenerateManagerOBT manager = OptimizationAlgoFactory.getOptimizationAlogrithm(tuningAlgorithm);
    if (manager != null) {
      manager.initializePrerequisite(tuningAlgorithm, tuningJobDefinition.job);
    }
  }

  /**
   * Inserts parameter value in database
   * @param jobSuggestedParamSet Parameter set to which the parameter belongs
   * @param paramName Parameter name
   * @param paramValue Parameter value
   */
  private void insertParameterValue(JobSuggestedParamSet jobSuggestedParamSet, String paramName, Double paramValue,
      TuningAlgorithm tuningAlgorithm) {
    logger.debug("Starting insertParameterValue");
    JobSuggestedParamValue jobSuggestedParamValue = new JobSuggestedParamValue();
    jobSuggestedParamValue.jobSuggestedParamSet = jobSuggestedParamSet;
    TuningParameter tuningParameter = TuningParameter.find.where()
        .eq(TuningParameter.TABLE.paramName, paramName)
        .eq(TuningParameter.TABLE.tuningAlgorithm, tuningAlgorithm)
        .findUnique();
    if (tuningParameter != null) {
      jobSuggestedParamValue.tuningParameter = tuningParameter;
      jobSuggestedParamValue.paramValue = paramValue;
      jobSuggestedParamValue.save();
    } else {
      logger.warn("TuningAlgorithm param null " + paramName);
    }
  }
}
