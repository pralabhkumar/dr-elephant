package com.linkedin.drelephant.tuning.hbt;

import com.linkedin.drelephant.tuning.engine.SparkExecutionEngine;
import java.util.HashMap;
import java.util.List;
import models.AppResult;
import models.JobSuggestedParamSet;
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import models.TuningParameter;
import org.apache.log4j.Logger;
import play.libs.Json;
import org.apache.commons.io.FileUtils;


public class ParameterGenerateManagerHBTSpark<T extends SparkExecutionEngine> extends ParameterGenerateManagerHBT<T> {
  private final Logger logger = Logger.getLogger(getClass());
  T sparkExecutionEngine;

  public ParameterGenerateManagerHBTSpark(T sparkExecutionEngine) {
    this.sparkExecutionEngine = sparkExecutionEngine;
  }

  @Override
  protected List<JobSuggestedParamSet> getPendingParamSets() {
    List<JobSuggestedParamSet> pendingParamSetList = sparkExecutionEngine.getPendingJobs()
        .eq(JobSuggestedParamSet.TABLE.tuningAlgorithm + "." + TuningAlgorithm.TABLE.optimizationAlgo,
            TuningAlgorithm.OptimizationAlgo.HBT.name())
        // .eq(JobSuggestedParamSet.TABLE.isParamSetDefault, 0)
        .findList();
    logger.debug(
        " Number of Pending Jobs for parameter suggestion " + sparkExecutionEngine + " " + pendingParamSetList.size());
    return pendingParamSetList;
  }

  @Override
  protected List<TuningJobDefinition> getTuningJobDefinitions() {
    List<TuningJobDefinition> totalJobs = sparkExecutionEngine.getTuningJobDefinitionsForParameterSuggestion()
        .eq(TuningJobDefinition.TABLE.tuningAlgorithm + "." + TuningAlgorithm.TABLE.optimizationAlgo,
            TuningAlgorithm.OptimizationAlgo.HBT.name())
        .findList();

    logger.debug(" Number of Total Jobs " + sparkExecutionEngine + " " + totalJobs.size());
    return totalJobs;
  }

  @Override
  public boolean isParamConstraintViolated(List<JobSuggestedParamValue> jobSuggestedParamValues) {
    return false;
  }

  @Override
  public String parameterGenerations(List<AppResult> results, List<TuningParameter> tuningParameters) {
    if (results != null && results.size() > 0) {
      SparkHBTParamRecommender sparkHBTParamRecommender = new SparkHBTParamRecommender(results.get(0));
      HashMap<String, Double> suggestedParameters = sparkHBTParamRecommender.getHBTSuggestion();
      StringBuffer idParameters = new StringBuffer();
      for (TuningParameter tuningParameter : tuningParameters) {
        if (suggestedParameters.containsKey(tuningParameter.paramName)) {
          idParameters.append(tuningParameter.id)
              .append("\t")
              .append(suggestedParameters.get(tuningParameter.paramName));
          idParameters.append("\n");
        }
      }
      return idParameters.toString();
    }
    return null;
  }

  @Override
  public void computeValuesOfDerivedConfigurationParameters(List<TuningParameter> derivedParameterList,
      List<JobSuggestedParamValue> jobSuggestedParamValueList) {
    sparkExecutionEngine.computeValuesOfDerivedConfigurationParameters(derivedParameterList,
        jobSuggestedParamValueList);
  }
}
