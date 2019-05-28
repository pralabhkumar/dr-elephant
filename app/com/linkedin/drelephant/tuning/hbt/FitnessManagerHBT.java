package com.linkedin.drelephant.tuning.hbt;

import com.linkedin.drelephant.tuning.TuningHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import models.AppHeuristicResult;
import models.AppResult;
import models.JobDefinition;
import models.JobExecution;
import models.JobSuggestedParamSet;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import models.TuningJobExecutionParamSet;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.avaje.ebean.Expr;
import com.linkedin.drelephant.AutoTuner;
import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.tuning.AbstractFitnessManager;
import com.linkedin.drelephant.util.Utils;


public class FitnessManagerHBT extends AbstractFitnessManager {
  private final Logger logger = Logger.getLogger(getClass());
  private boolean isDebugEnabled = logger.isDebugEnabled();
  private final int MINIMUM_HBT_EXECUTION = 50;

  public FitnessManagerHBT() {
    Configuration configuration = ElephantContext.instance().getAutoTuningConf();

    // Time duration to wait for computing the fitness of a param set once the corresponding execution is completed
    fitnessComputeWaitInterval =
        Utils.getNonNegativeLong(configuration, FITNESS_COMPUTE_WAIT_INTERVAL, 5 * AutoTuner.ONE_MIN);
    logger.info("Fitness wait time " + fitnessComputeWaitInterval);

    // Time duration to wait for metrics (resource usage, execution time) of an execution to be computed before
    // discarding it for fitness computation
    ignoreExecutionWaitInterval =
        Utils.getNonNegativeLong(configuration, IGNORE_EXECUTION_WAIT_INTERVAL, 2 * 60 * AutoTuner.ONE_MIN);

    // #executions after which tuning will stop even if parameters don't converge
    maxTuningExecutions = Utils.getNonNegativeInt(configuration, MAX_TUNING_EXECUTIONS, 10);

    // #executions before which tuning cannot stop even if parameters converge
    minTuningExecutions = Utils.getNonNegativeInt(configuration, MIN_TUNING_EXECUTIONS, 2);
  }

  @Override
  protected List<TuningJobExecutionParamSet> detectJobsForFitnessComputation() {
    logger.info("Fetching completed executions whose fitness are yet to be computed");
    List<TuningJobExecutionParamSet> completedJobExecutionParamSet = new ArrayList<TuningJobExecutionParamSet>();

    List<TuningJobExecutionParamSet> tuningJobExecutionParamSets = TuningJobExecutionParamSet.find.select("*")
        .fetch(TuningJobExecutionParamSet.TABLE.jobExecution, "*")
        .fetch(TuningJobExecutionParamSet.TABLE.jobSuggestedParamSet, "*")
        .where()
        .or(Expr.or(Expr.eq(TuningJobExecutionParamSet.TABLE.jobExecution + '.' + JobExecution.TABLE.executionState,
            JobExecution.ExecutionState.SUCCEEDED),
            Expr.eq(TuningJobExecutionParamSet.TABLE.jobExecution + '.' + JobExecution.TABLE.executionState,
                JobExecution.ExecutionState.FAILED)),
            Expr.eq(TuningJobExecutionParamSet.TABLE.jobExecution + '.' + JobExecution.TABLE.executionState,
                JobExecution.ExecutionState.CANCELLED))
        .isNull(TuningJobExecutionParamSet.TABLE.jobExecution + '.' + JobExecution.TABLE.resourceUsage)
        .eq(TuningJobExecutionParamSet.TABLE.jobSuggestedParamSet + "." + JobSuggestedParamSet.TABLE.tuningAlgorithm
            + "." + TuningAlgorithm.TABLE.optimizationAlgo, TuningAlgorithm.OptimizationAlgo.HBT.name())
        .findList();

    logger.info("#completed executions whose metrics are not computed: " + tuningJobExecutionParamSets.size());

    getCompletedExecution(tuningJobExecutionParamSets, completedJobExecutionParamSet);

    logger.info(" Final jobs for fitness Computation " + completedJobExecutionParamSet.size());
    return completedJobExecutionParamSet;
  }

  @Override
  protected void calculateAndUpdateFitness(JobExecution jobExecution, List<AppResult> results,
      TuningJobDefinition tuningJobDefinition, JobSuggestedParamSet jobSuggestedParamSet, boolean isRetried) {
    logger.debug("calculateAndUpdateFitness");
    Double totalResourceUsed = 0D;
    Double totalInputBytesInBytes = 0D;
    Double score = 0D;
    logger.info(" Job Execution ID is " + jobExecution.jobExecId);
    for (AppResult appResult : results) {
      logger.info(" Apps are " + appResult.id);
      totalResourceUsed += appResult.resourceUsed;
      totalInputBytesInBytes += appResult.getTotalInputBytes();
      score += appResult.score;
    }

    Long totalRunTime = Utils.getTotalRuntime(results);
    Long totalDelay = Utils.getTotalWaittime(results);
    Long totalExecutionTime = totalRunTime - totalDelay;

    if (totalExecutionTime != 0) {
      jobExecution.score = score;
      updateJobExecution(jobExecution, totalResourceUsed, totalInputBytesInBytes, totalExecutionTime);
    }

    if (tuningJobDefinition.averageResourceUsage == null && totalExecutionTime != 0) {
      updateTuningJobDefinition(tuningJobDefinition, jobExecution);
    }
    try {
      if (isRetried) {
        logger.info(" Retried execution " + jobExecution.id + " for parameter " + jobSuggestedParamSet.id);
        handleRetryScenarios(jobSuggestedParamSet, jobExecution);
      } else {
        logger.info(" Non Retried execution " + jobExecution.id + " for parameter " + jobSuggestedParamSet.id);
        handleNonRetryScenarios(jobSuggestedParamSet, jobExecution);
      }
      logger.info(
          " Calculating Fitness for " + jobSuggestedParamSet.id + " " + jobSuggestedParamSet.fitnessJobExecution.id
              + " " + jobExecution.id);
    } catch (Exception e) {
      logger.error("Exception while calculating fitness for " + jobSuggestedParamSet.id + " " + jobExecution.id, e);
    }
  }

  private void handleNonRetryScenarios(JobSuggestedParamSet jobSuggestedParamSet, JobExecution jobExecution) {
    if (alreadyFitnessComputed(jobSuggestedParamSet)) {
      logger.info(" Fitness is already computed for this parameter " + jobSuggestedParamSet.id);
      TuningHelper.updateJobExecution(jobExecution);
    } else {
      logger.info(" Fitness not computed " + jobSuggestedParamSet.id);
      TuningHelper.updateJobExecution(jobExecution);
      updateJobSuggestedParamSetSucceededExecution(jobExecution, jobSuggestedParamSet, null);
    }
    logger.info("Updated job execution " + jobSuggestedParamSet.fitnessJobExecution.id + " " + jobExecution.id);
  }

  private void handleRetryScenarios(JobSuggestedParamSet jobSuggestedParamSet, JobExecution jobExecution) {
    if (jobExecution.executionState.equals(JobExecution.ExecutionState.SUCCEEDED)) {
      logger.info(
          "Job is succeeded after retry : job execution" + jobExecution.id + " parameter " + jobSuggestedParamSet.id);
      handleJobSucceededAfterRetryScenarios(jobSuggestedParamSet, jobExecution);
    } else {
      logger.info(
          "Job is Failed after retry : job execution" + jobExecution.id + " parameter " + jobSuggestedParamSet.id);
      handleJobFailedAfterRetryScenarios(jobSuggestedParamSet, jobExecution);
    }
  }

  private void handleJobFailedAfterRetryScenarios(JobSuggestedParamSet jobSuggestedParamSet,
      JobExecution jobExecution) {
    if (alreadyFitnessComputed(jobSuggestedParamSet)) {
      assignDefaultValuesToJobExecution(jobExecution);
      TuningHelper.updateJobExecution(jobExecution);
    } else {
      resetParamSetToCreated(jobSuggestedParamSet, jobExecution);
      TuningHelper.updateJobExecution(jobExecution);
      TuningHelper.updateJobSuggestedParamSet(jobSuggestedParamSet, jobExecution);
    }
  }

  private void handleJobSucceededAfterRetryScenarios(JobSuggestedParamSet jobSuggestedParamSet,
      JobExecution jobExecution) {
    FailureHandlerContext failureHandlerContext = new FailureHandlerContext();
    if (jobExecution.autoTuningFault) {
      logger.info(" Job execution was failed because of autotuning but retry worked" + jobExecution.id);
      failureHandlerContext.setFailureHandler(new AutoTuningFailureHandler());
    } else {
      logger.info(" Job execution was failed because of other reasons but retry worked" + jobExecution.id);
      failureHandlerContext.setFailureHandler(new NonAutoTuningFailureHandler());
    }
    failureHandlerContext.execute(jobExecution, jobSuggestedParamSet, this);
  }

  protected void updateJobSuggestedParamSetSucceededExecution(JobExecution jobExecution,
      JobSuggestedParamSet jobSuggestedParamSet, TuningJobDefinition tuningJobDefinition) {
    logger.info("Updating Job Suggested param set " + jobSuggestedParamSet.id);
    jobSuggestedParamSet.fitness = jobExecution.score;
    jobSuggestedParamSet.paramSetState = JobSuggestedParamSet.ParamSetStatus.FITNESS_COMPUTED;
    jobSuggestedParamSet.fitnessJobExecution = jobExecution;
    jobSuggestedParamSet = updateBestJobSuggestedParamSet(jobSuggestedParamSet);
    TuningHelper.updateJobSuggestedParamSet(jobSuggestedParamSet, jobExecution);
  }

  @Override
  protected void checkToDisableTuning(Set<JobDefinition> jobDefinitionSet) {
    Long currentTimeBefore = System.currentTimeMillis();
    for (JobDefinition jobDefinition : jobDefinitionSet) {
      List<TuningJobExecutionParamSet> tuningJobExecutionParamSets =
          TuningHelper.getTuningJobExecutionFromDefinition(jobDefinition);
      int numberOfValidSuggestedParamExecution =
          TuningHelper.getNumberOfValidSuggestedParamExecution(tuningJobExecutionParamSets);
      if (disableTuningforUserSpecifiedIterations(jobDefinition, numberOfValidSuggestedParamExecution)
          || disableTuningforHeuristicsPassed(jobDefinition, tuningJobExecutionParamSets,
          numberOfValidSuggestedParamExecution)) {
        logger.debug(" Tuning Disabled for Job " + jobDefinition.id);
      }
    }
    Long currentTimeAfter = System.currentTimeMillis();
    logger.info(" Total time taken to check for disabling tuning " + (currentTimeAfter - currentTimeBefore));
  }

  private boolean disableTuningforHeuristicsPassed(JobDefinition jobDefinition,
      List<TuningJobExecutionParamSet> tuningJobExecutionParamSets, int numberOfAppliedSuggestedParamExecution) {
    //Minimum three execution needed for HBT to do some resource optimization
    if (areHeuristicsPassed(tuningJobExecutionParamSets)
        && numberOfAppliedSuggestedParamExecution >= MINIMUM_HBT_EXECUTION) {
      disableTuning(jobDefinition, "All Heuristics Passed");
    }
    return true;
  }

  private boolean areHeuristicsPassed(List<TuningJobExecutionParamSet> tuningJobExecutionParamSets) {
    logger.debug(" Testing All Heuristics ");
    if (tuningJobExecutionParamSets != null && tuningJobExecutionParamSets.size() >= 1) {
      logger.debug("tuningJobExecutionParamSets have some values");
      TuningJobExecutionParamSet tuningJobExecutionParamSet = tuningJobExecutionParamSets.get(0);
      JobExecution jobExecution = tuningJobExecutionParamSet.jobExecution;
      logger.info("Job execution id is "  + jobExecution.id);
      List<AppResult> results = getAppResult(jobExecution);
      if (results != null) {
        logger.debug(" Results are not null ");
        return areAppResultsHaveSeverity(results);
      } else {
        logger.debug(" App Results are null ");
        return false;
      }
    } else {
      logger.debug(" Tuning Job Execution Param Set is null ");
      return false;
    }
  }

  private boolean areAppResultsHaveSeverity(List<AppResult> results) {
    List<String> heuristicsWithHighSeverity = new ArrayList<String>();
    for (AppResult appResult : results) {
      if (appResult.yarnAppHeuristicResults != null) {
        for (AppHeuristicResult appHeuristicResult : appResult.yarnAppHeuristicResults) {
          if (appHeuristicResult.severity.getValue() == 3 || appHeuristicResult.severity.getValue() == 4) {
            heuristicsWithHighSeverity.add(
                appResult.id + "\t" + appHeuristicResult.heuristicName + "\t" + "have high severity" + "\t"
                    + appHeuristicResult.severity.getValue());
          }
        }
      } else {
        if (isDebugEnabled) {
          logger.debug(appResult.id + " " + appResult.jobDefId + " have yarn app result null ");
        }
        return true;
      }
    }
    return checkHeuriticsforSeverity(heuristicsWithHighSeverity);
  }

  private boolean checkHeuriticsforSeverity(List<String> heuristicsWithHighSeverity) {
    if (heuristicsWithHighSeverity.size() == 0) {
      logger.debug(" No severe heursitics ");
      return true;
    } else {
      for (String failedHeuristics : heuristicsWithHighSeverity) {
        logger.debug(failedHeuristics);
      }
      return false;
    }
  }

  private List<AppResult> getAppResult(JobExecution jobExecution) {
    List<AppResult> results = null;
    try {
      results = AppResult.find.select("*")
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, "*")
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS + "." + AppHeuristicResult.TABLE.APP_HEURISTIC_RESULT_DETAILS,
              "*")
          .where()
          .eq(AppResult.TABLE.FLOW_EXEC_ID, jobExecution.flowExecution.flowExecId)
          .eq(AppResult.TABLE.JOB_EXEC_ID, jobExecution.jobExecId)
          .findList();
    } catch (Exception e) {
      logger.warn(" Job Analysis is not completed . ");
    }
    return results;
  }

  @Override
  public String getManagerName() {
    return "FitnessManagerHBT";
  }

  @Override
  protected JobSuggestedParamSet updateBestJobSuggestedParamSet(JobSuggestedParamSet jobSuggestedParamSet) {
    logger.debug("Checking if a new best param set is found for job: " + jobSuggestedParamSet.jobDefinition.jobDefId);
    JobSuggestedParamSet currentBestJobSuggestedParamSet = JobSuggestedParamSet.find.where()
        .eq(JobSuggestedParamSet.TABLE.jobDefinition + "." + JobDefinition.TABLE.id,
            jobSuggestedParamSet.jobDefinition.id)
        .eq(JobSuggestedParamSet.TABLE.isParamSetBest, 1)
        .findUnique();
    if (currentBestJobSuggestedParamSet != null) {
      if (currentBestJobSuggestedParamSet.fitness > jobSuggestedParamSet.fitness) {
        logger.debug("Param set: " + jobSuggestedParamSet.id
            + " is the new best param set for job because of better because of better fitness: "
            + jobSuggestedParamSet.jobDefinition.jobDefId);
        currentBestJobSuggestedParamSet.isParamSetBest = false;
        jobSuggestedParamSet.isParamSetBest = true;
        currentBestJobSuggestedParamSet.save();
      } else if (currentBestJobSuggestedParamSet.fitness.longValue() == jobSuggestedParamSet.fitness.longValue()) {
        if (TuningHelper.isNewParamBestParam(jobSuggestedParamSet, currentBestJobSuggestedParamSet)) {
          logger.debug("Param set: " + jobSuggestedParamSet.id
              + " is the new best param set for job because of better resource usage: "
              + jobSuggestedParamSet.jobDefinition.jobDefId);
          currentBestJobSuggestedParamSet.isParamSetBest = false;
          jobSuggestedParamSet.isParamSetBest = true;
          currentBestJobSuggestedParamSet.save();
        }
      }
    } else {
      logger.debug("No best param set found for job: " + jobSuggestedParamSet.jobDefinition.jobDefId
          + ". Marking current param set " + jobSuggestedParamSet.id + " as best");
      jobSuggestedParamSet.isParamSetBest = true;
    }
    return jobSuggestedParamSet;
  }
}
