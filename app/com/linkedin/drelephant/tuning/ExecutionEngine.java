package com.linkedin.drelephant.tuning;

import com.avaje.ebean.ExpressionList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import models.AppResult;
import models.JobDefinition;
import models.JobExecution;
import models.JobSuggestedParamSet;
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import models.TuningParameter;
import models.TuningParameterConstraint;


/**
 * Exeuction Engine is the interface of different execution engines. Currently it have two implementations
 * MRExecutionEngine : Handle parameters related to Map Reduce.
 * SparkExecutionEngine : Handle parameters related to Spark
 */

public interface ExecutionEngine {

  /**
   * This method is used to compute the values of derived parameters . These parameter values have not been suggested by tuning algorithm
   *
   * @param derivedParameterList Derived Parameter List
   * @param jobSuggestedParamValue Update job suggested param value with the derived parameter value list.
   *
   */
  void computeValuesOfDerivedConfigurationParameters(List<TuningParameter> derivedParameterList,
      List<JobSuggestedParamValue> jobSuggestedParamValue);

  /**
   * This method is used to get the pending JobSuggestedParamSet , param set which are not in FitnessCompute & discarded jobs
   *
   * @return Return pending jobs
   */

  ExpressionList<JobSuggestedParamSet> getPendingJobs();

  /**
   * This method is used to get jobs for which tuning enabled. Subtraction of this set with the above set
   * gives us the jobs for which parameter have to be generated
   *
   * @return Return Tuning enabled jobs
   */
  ExpressionList<TuningJobDefinition> getTuningJobDefinitionsForParameterSuggestion();

  /**
   *
   * @param functionType Extract usage information of the application to the list
   * @param usageDataGlobal
   * @return
   */
  List<Double> extractUsageParameter(String functionType, Map<String, Map<String, Double>> usageDataGlobal);

  /**
   *  Extract App results to Map ,it contains key (map and reduce for MR Execution engine) and then previous execution engine
   * @param appResults
   * @return
   */
  Map<String, Map<String, Double>> extractParameterInformation(List<AppResult> appResults);
}
