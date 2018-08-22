package com.linkedin.drelephant.tuning.hbt;

import com.linkedin.drelephant.tuning.AbstractTuningTypeManager;
import com.linkedin.drelephant.tuning.JobTuningInfo;
import com.linkedin.drelephant.tuning.ExecutionEngine;
import java.util.List;
import java.util.Map;
import models.AppResult;
import models.JobDefinition;
import models.JobSuggestedParamValue;
import models.TuningJobDefinition;
import models.TuningParameter;


public class TuningTypeManagerHBT extends AbstractTuningTypeManager {

  public TuningTypeManagerHBT(ExecutionEngine executionEngine) {
    tuningType = "HBT";
    this._executionEngine = executionEngine;
  }

  @Override
  protected List<JobTuningInfo> detectJobsForParameterGeneration() {
    return null;
  }

  @Override
  protected JobTuningInfo generateParamSet(JobTuningInfo jobTuningInfo) {
    return null;
  }

  @Override
  protected Boolean updateDatabase(List<JobTuningInfo> tuningJobDefinitions) {
    return null;
  }

  @Override
  protected void updateBoundryConstraint(List<TuningParameter> tuningParameterList,
      TuningJobDefinition tuningJobDefinition, JobDefinition job) {

  }

  @Override
  public boolean isParamConstraintViolated(List<JobSuggestedParamValue> jobSuggestedParamValues) {
    return false;
  }

  @Override
  public Map<String, Map<String, Double>> extractParameterInformation(List<AppResult> appResults) {
    return null;
  }

  @Override
  public String getManagerName() {
    return "TuningTypeManagerHBT";
  }
}
