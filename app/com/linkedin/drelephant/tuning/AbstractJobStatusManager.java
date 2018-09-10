package com.linkedin.drelephant.tuning;

import controllers.AutoTuningMetricsController;
import java.util.List;
import models.JobExecution;
import models.JobSuggestedParamSet;
import models.TuningJobExecutionParamSet;
import org.apache.log4j.Logger;


/**
 * Abstract Job Status Manager provides the functionality to check the job status of the job. This should have implementation specific to schduler
 */
public abstract class AbstractJobStatusManager implements Manager {
  private final Logger logger = Logger.getLogger(getClass());

  /**
   * This method find out the completed execution of the job and update the database.
   * This method is schduler dependent.
   * @param inProgressExecutionParamSet : Jobs for which status have to find out.
   * @return
   */
  protected abstract boolean analyzeCompletedJobsExecution(
      List<TuningJobExecutionParamSet> inProgressExecutionParamSet);

  protected List<TuningJobExecutionParamSet> detectJobsExecutionInProgress() {
    logger.info("Fetching the executions which are in progress");
    List<TuningJobExecutionParamSet> tuningJobExecutionParamSets =
        TuningJobExecutionParamSet.find.fetch(TuningJobExecutionParamSet.TABLE.jobExecution)
            .fetch(TuningJobExecutionParamSet.TABLE.jobSuggestedParamSet)
            .where()
            .eq(TuningJobExecutionParamSet.TABLE.jobExecution + '.' + JobExecution.TABLE.executionState,
                JobExecution.ExecutionState.IN_PROGRESS)
            .findList();

    logger.info("Number of executions which are in progress: " + tuningJobExecutionParamSets.size());
    return tuningJobExecutionParamSets;
  }

  protected boolean updateDataBase(List<TuningJobExecutionParamSet> jobs) {
    for (TuningJobExecutionParamSet job : jobs) {
      JobSuggestedParamSet jobSuggestedParamSet = job.jobSuggestedParamSet;
      JobExecution jobExecution = job.jobExecution;
      if (isJobCompleted(jobExecution)) {
        jobExecution.update();
        jobSuggestedParamSet.update();
        logger.info("Execution " + jobExecution.jobExecId + " is completed");
      } else {
        logger.info("Execution " + jobExecution.jobExecId + " is still in running state");
      }
    }
    return true;
  }

  private Boolean isJobCompleted(JobExecution jobExecution) {
    if (jobExecution.executionState.equals(JobExecution.ExecutionState.SUCCEEDED) || jobExecution.executionState.equals(
        JobExecution.ExecutionState.FAILED) || jobExecution.executionState.equals(
        JobExecution.ExecutionState.CANCELLED)) {
      return true;
    } else {
      return false;
    }
  }

  protected boolean updateMetrics(List<TuningJobExecutionParamSet> completedJobs) {
    for (TuningJobExecutionParamSet completedJob : completedJobs) {
      JobExecution jobExecution = completedJob.jobExecution;
      if (jobExecution.executionState.equals(JobExecution.ExecutionState.SUCCEEDED)) {
        AutoTuningMetricsController.markSuccessfulJobs();
      } else if (jobExecution.executionState.equals(JobExecution.ExecutionState.FAILED)) {
        AutoTuningMetricsController.markFailedJobs();
      }
    }
    return true;
  }

  @Override
  public final boolean execute() {
    logger.info("Executing Job Status Manager");
    boolean calculateCompletedJobExDone = false, databaseUpdateDone = false, updateMetricsDone = false;
    List<TuningJobExecutionParamSet> tuningJobExecutionParamSet = detectJobsExecutionInProgress();
    if (tuningJobExecutionParamSet != null && tuningJobExecutionParamSet.size() >= 1) {
      logger.info("Calculating  Completed Jobs");
      calculateCompletedJobExDone = analyzeCompletedJobsExecution(tuningJobExecutionParamSet);
    }
    if (calculateCompletedJobExDone) {
      logger.info("Updating Database");
      databaseUpdateDone = updateDataBase(tuningJobExecutionParamSet);
    }
    if (databaseUpdateDone) {
      logger.info("Updating Metrics");
      updateMetricsDone = updateMetrics(tuningJobExecutionParamSet);
    }
    logger.info("Baseline Done");
    return updateMetricsDone;
  }
}
