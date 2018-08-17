package com.linkedin.drelephant.tuning.foundation;

import java.util.List;
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningParameter;


public interface ExecutionEngine {

  String getExecutionEngineName();
  void computeValuesOfDerivedConfigurationParameters(List<TuningParameter> derivedParameterList,
      List<JobSuggestedParamValue> jobSuggestedParamValue);

   Boolean isParamConstraintViolated(List<JobSuggestedParamValue> jobSuggestedParamValueList,
      TuningAlgorithm tuningAlgorithm);
}
