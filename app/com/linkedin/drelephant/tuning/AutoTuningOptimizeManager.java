package com.linkedin.drelephant.tuning;

import java.util.List;
import models.AppResult;
import models.JobDefinition;
import models.JobSuggestedParamSet;
import models.JobSuggestedParamValue;
import models.TuningParameter;

/*
Class should extend interface to incorporate optimization in Tuning  algorithm
 */

public interface AutoTuningOptimizeManager {
  /*
    Intialize any prequisite require for Optimizer
    Calls once in lifetime of the flow
   */
  public void intializePrerequisite(JobSuggestedParamSet jobSuggestedParamSet);

  /*
    Extract parameter Information of previous executions
    calls after each exectuion of flow
   */
  public void extractParameterInformation(List<AppResult> appResults);

  /*
    Optimize search space
    call after each execution of flow
   */
  public void parameterOptimizer(Integer jobID);

  /*
    apply Intelligence on Parameter.
    calls after swarm size number of executions
   */
  public void applyIntelligenceOnParameter(List<TuningParameter> tuningParameterList, JobDefinition job);

  /*
    Constraint violation check on optimizations
    calls after swarm size number of executions
   */
  public int numberOfConstraintsViolated(List<JobSuggestedParamValue> jobSuggestedParamValueList);
}
