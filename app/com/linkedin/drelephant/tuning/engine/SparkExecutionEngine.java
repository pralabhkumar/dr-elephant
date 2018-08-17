package com.linkedin.drelephant.tuning.engine;

import com.linkedin.drelephant.tuning.foundation.ExecutionEngine;
import java.util.List;
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningParameter;


public class SparkExecutionEngine implements ExecutionEngine {
  private String executionEngineName = "SPARK";
  @Override
  public String getExecutionEngineName() {
    return this.executionEngineName;
  }

  @Override
  public void computeValuesOfDerivedConfigurationParameters(List<TuningParameter> derivedParameterList,
      List<JobSuggestedParamValue> jobSuggestedParamValue) {

  }

  @Override
  public Boolean isParamConstraintViolated(List<JobSuggestedParamValue> jobSuggestedParamValueList,
      TuningAlgorithm tuningAlgorithm) {
    return null;
  }
}
