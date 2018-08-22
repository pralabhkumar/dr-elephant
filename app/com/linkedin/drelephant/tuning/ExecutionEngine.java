package com.linkedin.drelephant.tuning;

import com.avaje.ebean.ExpressionList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import models.AppResult;
import models.JobSuggestedParamSet;
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import models.TuningParameter;
import models.TuningParameterConstraint;


public interface ExecutionEngine {

  void computeValuesOfDerivedConfigurationParameters(List<TuningParameter> derivedParameterList,
      List<JobSuggestedParamValue> jobSuggestedParamValue);

  ExpressionList<JobSuggestedParamSet> getPendingJobs();

  ExpressionList<TuningJobDefinition> getTuningJobDefinitionsForParameterSuggestion();
  /*
  PSO related methods
   */
  Boolean isParamConstraintViolatedPSO(List<JobSuggestedParamValue> jobSuggestedParamValueList);

  Boolean isParamConstraintViolatedIPSO(List<JobSuggestedParamValue> jobSuggestedParamValueList);

  /*
  IPSO related Methods
   */

  Map<String, Map<String, Double>> collectUsageDataPerApplicationIPSO(AppResult appResult);

  Map<String, Map<String, Double>> intializeUsageCounterValuesIPSO();

  public void parameterOptimizerIPSO(Integer jobID, Map<String, Map<String, Double>> previousUsedMetrics,
      List<TuningParameterConstraint> parameterConstraints);
}
