package com.linkedin.drelephant.tuning.hbt;

import com.avaje.ebean.Expr;
import com.linkedin.drelephant.AutoTuner;
import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.tuning.AbstractFitnessManager;
import com.linkedin.drelephant.util.Utils;
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
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import scala.App;


public class FitnessManagerHBT extends AbstractFitnessManager {
  private final Logger logger = Logger.getLogger(getClass());

  public FitnessManagerHBT() {
    Configuration configuration = ElephantContext.instance().getAutoTuningConf();

    // Time duration to wait for computing the fitness of a param set once the corresponding execution is completed
    fitnessComputeWaitInterval =
        Utils.getNonNegativeLong(configuration, FITNESS_COMPUTE_WAIT_INTERVAL, 5 * AutoTuner.ONE_MIN);

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

    return completedJobExecutionParamSet;
  }

  @Override
  protected void calculateAndUpdateFitness(JobExecution jobExecution, List<AppResult> results,
      TuningJobDefinition tuningJobDefinition, JobSuggestedParamSet jobSuggestedParamSet) {
    logger.info("calculateAndUpdateFitness");
    Double totalResourceUsed = 0D;
    Double totalInputBytesInBytes = 0D;
    for (AppResult appResult : results) {
      totalResourceUsed += appResult.resourceUsed;
      totalInputBytesInBytes += getTotalInputBytes(appResult);
    }

    Long totalRunTime = Utils.getTotalRuntime(results);
    Long totalDelay = Utils.getTotalWaittime(results);
    Long totalExecutionTime = totalRunTime - totalDelay;

    if (totalExecutionTime != 0) {
      updateJobExecution(jobExecution, totalResourceUsed, totalInputBytesInBytes, totalExecutionTime);
    }

    if (tuningJobDefinition.averageResourceUsage == null && totalExecutionTime != 0) {
      updateTuningJobDefinition(tuningJobDefinition, jobExecution);
    }

    //Compute fitness
    computeFitness(jobSuggestedParamSet, jobExecution, tuningJobDefinition, results);
  }

  protected void computeFitness(JobSuggestedParamSet jobSuggestedParamSet, JobExecution jobExecution,
      TuningJobDefinition tuningJobDefinition, List<AppResult> results) {
    if (!jobSuggestedParamSet.paramSetState.equals(JobSuggestedParamSet.ParamSetStatus.FITNESS_COMPUTED)
        || !jobSuggestedParamSet.paramSetState.equals(JobSuggestedParamSet.ParamSetStatus.DISCARDED)) {
      if (jobExecution.executionState.equals(JobExecution.ExecutionState.SUCCEEDED)) {
        logger.info("Execution id: " + jobExecution.id + " succeeded");
        updateJobSuggestedParamSetSucceededExecution(jobExecution, jobSuggestedParamSet, tuningJobDefinition);
      } else {
        // Resetting param set to created state because this case captures the scenarios when
        // either the job failed for reasons other than auto tuning or was killed/cancelled/skipped etc.
        // In all the above scenarios, fitness cannot be computed for the param set correctly.
        // Note that the penalty on failures caused by auto tuning is applied when the job execution is retried
        // after failure.
        logger.info("HBT Execution id: " + jobExecution.id + " was not successful for reason other than tuning."
            + "Resetting param set: " + jobSuggestedParamSet.id + " to CREATED state");
        resetParamSetToCreated(jobSuggestedParamSet);
      }
    }
  }

  /**
   * Resets the param set to CREATED state if its fitness is not already computed
   * @param jobSuggestedParamSet Param set which is to be reset
   */
  protected void resetParamSetToCreated(JobSuggestedParamSet jobSuggestedParamSet) {
    if (!jobSuggestedParamSet.paramSetState.equals(JobSuggestedParamSet.ParamSetStatus.FITNESS_COMPUTED)
        && !jobSuggestedParamSet.paramSetState.equals(JobSuggestedParamSet.ParamSetStatus.DISCARDED)) {
      logger.info("Resetting parameter set to created: " + jobSuggestedParamSet.id);
      jobSuggestedParamSet.paramSetState = JobSuggestedParamSet.ParamSetStatus.CREATED;
      jobSuggestedParamSet.save();
    }
  }

  @Override
  protected void checkToDisableTuning(Set<JobDefinition> jobDefinitionSet) {
    for (JobDefinition jobDefinition : jobDefinitionSet) {
      List<TuningJobExecutionParamSet> tuningJobExecutionParamSets =
          TuningJobExecutionParamSet.find.fetch(TuningJobExecutionParamSet.TABLE.jobSuggestedParamSet, "*")
              .fetch(TuningJobExecutionParamSet.TABLE.jobExecution, "*")
              .where()
              .eq(TuningJobExecutionParamSet.TABLE.jobSuggestedParamSet + '.' + JobSuggestedParamSet.TABLE.jobDefinition
                  + '.' + JobDefinition.TABLE.id, jobDefinition.id)
              .order()
              .desc("job_execution_id")
              .findList();

      if (reachToNumberOfThresholdIterations(tuningJobExecutionParamSets, jobDefinition)) {
        disableTuning(jobDefinition, "User Specified Iterations reached");
      }
      if (areHeuristicsPassed(tuningJobExecutionParamSets)) {
        disableTuning(jobDefinition, "All Heuristics Passed1");
      }
    }
  }

  private boolean areHeuristicsPassed(List<TuningJobExecutionParamSet> tuningJobExecutionParamSets) {
    logger.info(" Testing All Heuristics ");
    if (tuningJobExecutionParamSets != null && tuningJobExecutionParamSets.size() >= 1) {
      logger.info("tuningJobExecutionParamSets have some values");
      TuningJobExecutionParamSet tuningJobExecutionParamSet = tuningJobExecutionParamSets.get(0);
      JobExecution jobExecution = tuningJobExecutionParamSet.jobExecution;
      List<AppResult> results = getAppResult(jobExecution);
      if (results != null) {
        logger.info(" Results are not null ");
        return areAppResultsHaveSeverity(results);
      } else {
        logger.info(" App Results are null ");
        return false;
      }
    } else {
      logger.info(" Tuning Job Execution Param Set is null ");
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
      }
    }
    return checkHeuriticsforSeverity(heuristicsWithHighSeverity);
  }

  private boolean checkHeuriticsforSeverity(List<String> heuristicsWithHighSeverity) {
    if (heuristicsWithHighSeverity.size() == 0) {
      logger.info(" No sever heursitics ");
      return true;
    } else {
      for (String failedHeuristics : heuristicsWithHighSeverity) {
        logger.info(failedHeuristics);
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
}
