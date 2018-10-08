package com.linkedin.drelephant.tuning.engine;

import java.util.HashMap;
import java.util.List;
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
 * This class represents Spark Exectuion Engine. It handles all the cases releated to Spark Engine
 */
public class SparkExecutionEngine implements ExecutionEngine {
  private final Logger logger = Logger.getLogger(getClass());
  private String executionEngineName = "SPARK";

  public String getExecutionEngineName() {
    return this.executionEngineName;
  }

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
        .eq(JobSuggestedParamSet.TABLE.isParamSetDefault, 0)
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
  public Boolean isParamConstraintViolatedPSO(List<JobSuggestedParamValue> jobSuggestedParamValueList) {
    return null;
  }

  @Override
  public Boolean isParamConstraintViolatedIPSO(List<JobSuggestedParamValue> jobSuggestedParamValueList) {
    return null;
  }

  @Override
  public void parameterOptimizerIPSO(List<AppResult> results, JobExecution jobExecution) {

  }

  @Override
  public String parameterGenerationsHBT(List<AppResult> results, List<TuningParameter> tuningParameters) {
    if (results != null && results.size() > 0) {
      SparkHBTParamRecommender sparkHBTParamRecommender = new SparkHBTParamRecommender(results.get(0));
      HashMap<String, Double> suggestedParameters = sparkHBTParamRecommender.getHBTSuggestion();
      StringBuffer idParameters = new StringBuffer();
      for (TuningParameter tuningParameter : tuningParameters) {
        if (suggestedParameters.containsKey(tuningParameter.paramName)) {
          idParameters.append(tuningParameter.id).append("\t")
              .append(suggestedParameters.get(tuningParameter.paramName));
          idParameters.append("\n");
        }
      }
      return idParameters.toString();
    }
    return null;
  }

  @Override
  public Boolean isParamConstraintViolatedHBT(List<JobSuggestedParamValue> jobSuggestedParamValueList) {
    return false;
  }

}
