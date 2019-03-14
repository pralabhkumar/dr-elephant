package com.linkedin.drelephant.tuning;

import com.linkedin.drelephant.tuning.hbt.FitnessManagerHBT;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import models.AppHeuristicResult;
import models.AppResult;
import models.JobDefinition;
import models.JobExecution;
import models.JobSuggestedParamSet;
import models.TuningJobDefinition;
import models.TuningJobExecutionParamSet;

import static common.DBTestUtil.*;
import static org.junit.Assert.*;
import static play.test.Helpers.*;
import static common.DBTestUtil.*;


public class FitnessManagerTestRunner implements Runnable {

  private void populateTestData() {
    try {
      initFitnessComputation();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void run() {
    populateTestData();
    testFitnessManagerHBT();
    testRetryAndNonAutotuningFault();
    testRetryAutoTuningFault();
    testNoRetryNoAutoTuningFault();
  }

  private void resetData() {
    JobExecution jobExecution = JobExecution.find.select("*").where().eq(JobExecution.TABLE.id, 1541).findUnique();
    jobExecution.resourceUsage = null;
    jobExecution.save();

    JobSuggestedParamSet newJobSuggestedParamSet =
        JobSuggestedParamSet.find.select("*").where().eq(JobSuggestedParamSet.TABLE.id, 1137).findUnique();

    newJobSuggestedParamSet.paramSetState = JobSuggestedParamSet.ParamSetStatus.EXECUTED;
    newJobSuggestedParamSet.fitness = 0.0;
    newJobSuggestedParamSet.isParamSetBest = false;
    newJobSuggestedParamSet.save();
  }

  private void testFitnessManagerHBT() {
    AbstractFitnessManager fitnessManager = new FitnessManagerHBT();
    List<TuningJobExecutionParamSet> jobsForFitnessComputation = fitnessManager.detectJobsForFitnessComputation();
    assertTrue(" Jobs for fitness computation HBT " + jobsForFitnessComputation.size(),
        jobsForFitnessComputation.size() == 1);

    JobSuggestedParamSet jobSuggestedParamSet =
        JobSuggestedParamSet.find.select("*").where().eq(JobSuggestedParamSet.TABLE.id, 1137).findUnique();

    assertTrue("Job Suggested Param Set Before " + jobSuggestedParamSet.paramSetState.name(),
        jobSuggestedParamSet.paramSetState.name().equals(JobSuggestedParamSet.ParamSetStatus.EXECUTED.name()));

    assertTrue("Fitness Calculated ", fitnessManager.calculateFitness(jobsForFitnessComputation));

    JobSuggestedParamSet jobSuggestedParamSet1 =
        JobSuggestedParamSet.find.select("*").where().eq(JobSuggestedParamSet.TABLE.id, 1137).findUnique();

    assertTrue("Job Suggested Param Set After " + jobSuggestedParamSet1.paramSetState.name(),
        jobSuggestedParamSet1.paramSetState.name().equals(JobSuggestedParamSet.ParamSetStatus.FITNESS_COMPUTED.name()));

    testTuningDisabled(jobsForFitnessComputation, fitnessManager);
    testFitnessDelay(jobsForFitnessComputation, fitnessManager);
  }

  private void createDataForRetryTesting(boolean isRetried, boolean isAutoTuningFault) {
    AbstractFitnessManager fitnessManager = new FitnessManagerHBT();
    TuningJobExecutionParamSet tuningJobExecutionParamSet = TuningJobExecutionParamSet.find.select("*")
        .where()
        .eq(TuningJobExecutionParamSet.TABLE.jobSuggestedParamSet + "." + JobSuggestedParamSet.TABLE.id, 1137)
        .findUnique();
    List<TuningJobExecutionParamSet> list = new ArrayList<TuningJobExecutionParamSet>();
    list.add(tuningJobExecutionParamSet);
    tuningJobExecutionParamSet.isRetried = isRetried;
    tuningJobExecutionParamSet.jobExecution.autoTuningFault = isAutoTuningFault;
    tuningJobExecutionParamSet.save();
    fitnessManager.calculateFitness(list);
  }

  private JobSuggestedParamSet getJobSuggestedParamSet() {
    return JobSuggestedParamSet.find.select("*").where().eq(JobSuggestedParamSet.TABLE.id, 1137).findUnique();
  }

  private JobExecution getJobExecution() {
    return JobExecution.find.select("*").where().eq(JobExecution.TABLE.id, 1541).findUnique();
  }

  /**
   * Test in case of execution is retried but its not autotuning fault.
   * In that case parameter should be changed to created state
   */
  private void testRetryAndNonAutotuningFault() {
    resetData();
    createDataForRetryTesting(true, false);
    JobSuggestedParamSet jobSuggestedParamSet = getJobSuggestedParamSet();
    JobExecution jobExceution = getJobExecution();
    assertTrue("Job Suggested Param Set After " + jobSuggestedParamSet.paramSetState.name(),
        jobSuggestedParamSet.paramSetState.name().equals(JobSuggestedParamSet.ParamSetStatus.CREATED.name()));
    assertTrue("Job Suggested Fitness " + jobSuggestedParamSet.fitness, jobSuggestedParamSet.fitness == 0.0);
    assertTrue("Job  Resource Usage", jobExceution.resourceUsage == 0D);
    assertTrue("Job execution time ", jobExceution.executionTime == 0D);
    assertTrue("Job Input size in bytes  ", jobExceution.inputSizeInBytes == 1D);
  }


  /**
   * Test in case of retry but if there is autotuning fault.
   * Parameter should be penalized.
   */
  private void testRetryAutoTuningFault() {
    resetData();
    createDataForRetryTesting(true, true);
    JobSuggestedParamSet jobSuggestedParamSet = getJobSuggestedParamSet();
    JobExecution jobExceution = getJobExecution();
    assertTrue("Job Suggested Param Set After " + jobSuggestedParamSet.paramSetState.name(),
        jobSuggestedParamSet.paramSetState.name().equals(JobSuggestedParamSet.ParamSetStatus.FITNESS_COMPUTED.name()));
    assertTrue("Job Suggested Fitness " + jobSuggestedParamSet.fitness, jobSuggestedParamSet.fitness == 10000D);
    assertTrue("Job  Resource Usage", jobExceution.resourceUsage == 0D);
    assertTrue("Job execution time ", jobExceution.executionTime == 0D);
    assertTrue("Job Input size in bytes  ", jobExceution.inputSizeInBytes == 1D);
  }

  /**
   * If there is no retry and no autotuning fault.
   * Process should run as it is.
   */
  private void testNoRetryNoAutoTuningFault() {
    resetData();
    createDataForRetryTesting(false, false);
    JobSuggestedParamSet jobSuggestedParamSet = getJobSuggestedParamSet();
    JobExecution jobExceution = getJobExecution();
    assertTrue("Job Suggested Param Set After " + jobSuggestedParamSet.paramSetState.name(),
        jobSuggestedParamSet.paramSetState.name().equals(JobSuggestedParamSet.ParamSetStatus.FITNESS_COMPUTED.name()));
    assertTrue("Job Suggested Fitness " + jobSuggestedParamSet.fitness, jobSuggestedParamSet.fitness == 1);
    assertTrue("Job  Resource Usage", jobExceution.resourceUsage > 0D);
    assertTrue("Job execution time ", jobExceution.executionTime > 0D);
  }

  private void testTuningDisabled(List<TuningJobExecutionParamSet> jobsForFitnessComputation,
      AbstractFitnessManager fitnessManager) {
    JobDefinition jobDefinition = jobsForFitnessComputation.get(0).jobSuggestedParamSet.jobDefinition;
    assertTrue(" Number of executions reach to threshold ",
        !fitnessManager.disableTuningforUserSpecifiedIterations(jobDefinition, 1));
    TuningJobDefinition tuningJobDefinition = TuningJobDefinition.find.select("*")
        .where()
        .eq(TuningJobDefinition.TABLE.job + "." + JobDefinition.TABLE.id, 100003)
        .eq(TuningJobDefinition.TABLE.tuningEnabled, 1)
        .findUnique();

    tuningJobDefinition.numberOfIterations = 1;
    tuningJobDefinition.save();
    assertTrue(" Number of executions reach to threshold ",
        fitnessManager.disableTuningforUserSpecifiedIterations(jobDefinition, 1));
    assertTrue(" Tuning Enabled ", tuningJobDefinition.tuningEnabled);
    fitnessManager.disableTuning(jobDefinition, "User Specified Iterations reached");
    TuningJobDefinition tuningJobDefinition1 = TuningJobDefinition.find.select("*")
        .where()
        .eq(TuningJobDefinition.TABLE.job + "." + JobDefinition.TABLE.id, 100003)
        .findUnique();
    assertTrue(" Tuning Disabled reason ",
        tuningJobDefinition1.tuningDisabledReason.equals("User Specified Iterations reached"));
    assertTrue(" Tuning Disabled reason " + tuningJobDefinition1.tuningEnabled, !tuningJobDefinition1.tuningEnabled);
  }

  private void testFitnessDelay(List<TuningJobExecutionParamSet> jobsForFitnessComputation,
      AbstractFitnessManager fitnessManager) {
    TuningJobExecutionParamSet tuningJobExecutionParamSet = jobsForFitnessComputation.get(0);
    tuningJobExecutionParamSet.jobExecution.updatedTs.setTime(System.currentTimeMillis());
    List<TuningJobExecutionParamSet> completedJobExecutionParamSet = new ArrayList<TuningJobExecutionParamSet>();
    fitnessManager.getCompletedExecution(jobsForFitnessComputation, completedJobExecutionParamSet);
    assertTrue("Current Time in millis: " + System.currentTimeMillis() + ", Job execution last updated time "
        + tuningJobExecutionParamSet.jobExecution.updatedTs.getTime() + " FitnessComputeInterval "
        + fitnessManager.fitnessComputeWaitInterval, completedJobExecutionParamSet.size() == 0);
  }
}
