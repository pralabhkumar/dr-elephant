package com.linkedin.drelephant.tuning.engine;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import models.AppResult;
import models.JobExecution;
import models.JobSuggestedParamSet;
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import models.TuningParameter;
import org.apache.log4j.Logger;
import com.avaje.ebean.Expr;
import com.avaje.ebean.ExpressionList;
import com.linkedin.drelephant.tuning.ExecutionEngine;


/**
 * This class represents Spark Execution Engine. It handles all the cases releated to Spark Engine
 */
public class SparkExecutionEngine implements ExecutionEngine {
  private final Logger logger = Logger.getLogger(getClass());

  @Override
  public void computeValuesOfDerivedConfigurationParameters(List<TuningParameter> derivedParameterList,
      List<JobSuggestedParamValue> jobSuggestedParamValue) {

  }

  @Override
  public ExpressionList<JobSuggestedParamSet> getPendingJobs() {
    return JobSuggestedParamSet.find
        .select("*")
        .fetch(JobSuggestedParamSet.TABLE.jobDefinition, "*")
        .where()
        .or(Expr.or(Expr.eq(JobSuggestedParamSet.TABLE.paramSetState, JobSuggestedParamSet.ParamSetStatus.CREATED),
            Expr.eq(JobSuggestedParamSet.TABLE.paramSetState, JobSuggestedParamSet.ParamSetStatus.SENT)),
            Expr.eq(JobSuggestedParamSet.TABLE.paramSetState, JobSuggestedParamSet.ParamSetStatus.EXECUTED))
       // .eq(JobSuggestedParamSet.TABLE.isParamSetDefault, 0)
        .eq(JobSuggestedParamSet.TABLE.tuningAlgorithm + "." + TuningAlgorithm.TABLE.jobType,
            TuningAlgorithm.JobType.SPARK.name()).eq(JobSuggestedParamSet.TABLE.isParamSetBest, 0);
  }

  @Override
  public ExpressionList<TuningJobDefinition> getTuningJobDefinitionsForParameterSuggestion() {
    return TuningJobDefinition.find
        .select("*")
        .fetch(TuningJobDefinition.TABLE.job, "*")
        .where()
        .eq(TuningJobDefinition.TABLE.tuningEnabled, 1)
        .eq(TuningJobDefinition.TABLE.tuningAlgorithm + "." + TuningAlgorithm.TABLE.jobType,
            TuningAlgorithm.JobType.SPARK);
  }

  @Override
  public List<Double> extractUsageParameter(String functionType, Map<String, Map<String, Double>> usageDataGlobal) {
    return null;
  }

  @Override
  public Map<String, Map<String, Double>> extractParameterInformation(List<AppResult> appResults) {
    return null;
  }


}
