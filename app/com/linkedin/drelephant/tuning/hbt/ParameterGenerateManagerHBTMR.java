package com.linkedin.drelephant.tuning.hbt;

import com.linkedin.drelephant.mapreduce.heuristics.CommonConstantsHeuristic;
import com.linkedin.drelephant.tuning.ExecutionEngine;
import com.linkedin.drelephant.tuning.engine.MRExecutionEngine;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import models.AppHeuristicResult;
import models.AppResult;
import models.JobSuggestedParamSet;
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import models.TuningParameter;
import org.apache.log4j.Logger;
import play.libs.Json;
import org.apache.commons.io.FileUtils;

import static java.lang.Math.*;


public class ParameterGenerateManagerHBTMR<T extends MRExecutionEngine> extends ParameterGenerateManagerHBT<T> {
  private final Logger logger = Logger.getLogger(getClass());
  T mrExecutionEngine;

  public ParameterGenerateManagerHBTMR(T mrExecutionEngine) {
    this.mrExecutionEngine = mrExecutionEngine;
  }

  @Override
  protected List<JobSuggestedParamSet> getPendingParamSets() {
    List<JobSuggestedParamSet> pendingParamSetList = mrExecutionEngine.getPendingJobs()
        .eq(JobSuggestedParamSet.TABLE.tuningAlgorithm + "." + TuningAlgorithm.TABLE.optimizationAlgo,
            TuningAlgorithm.OptimizationAlgo.HBT.name())
        // .eq(JobSuggestedParamSet.TABLE.isParamSetDefault, 0)
        .findList();
    logger.debug(
        " Number of Pending Jobs for parameter suggestion " + mrExecutionEngine + " " + pendingParamSetList.size());
    return pendingParamSetList;
  }

  @Override
  protected List<TuningJobDefinition> getTuningJobDefinitions() {
    List<TuningJobDefinition> totalJobs = mrExecutionEngine.getTuningJobDefinitionsForParameterSuggestion()
        .eq(TuningJobDefinition.TABLE.tuningAlgorithm + "." + TuningAlgorithm.TABLE.optimizationAlgo,
            TuningAlgorithm.OptimizationAlgo.HBT.name())
        .findList();

    logger.debug(" Number of Total Jobs " + mrExecutionEngine + " " + totalJobs.size());
    return totalJobs;
  }

  @Override
  public boolean isParamConstraintViolated(List<JobSuggestedParamValue> jobSuggestedParamValues) {
    return false;
  }

 /* private Map<String, Map<String, Double>> extractParameterInformation(List<AppResult> appResults) {
    logger.info(" Extract Parameter Information for MR HBT");
    return mrExecutionEngine.extractParameterInformation(appResults);
  }*/

  @Override
  public String parameterGenerations(List<AppResult> results, List<TuningParameter> tuningParameters) {
    MRJob mrJob = new MRJob(results, mrExecutionEngine);
    mrJob.analyzeAllApplications();
    mrJob.processJobForParameter();
    Map<String, Double> suggestedParameter = mrJob.getJobSuggestedParameter();
    StringBuffer idParameters = new StringBuffer();
    for (TuningParameter tuningParameter : tuningParameters) {
      Double paramValue = suggestedParameter.get(tuningParameter.paramName);
      if (paramValue != null && paramValue != 0.0) {
        idParameters.append(tuningParameter.id).append("\t").append(paramValue).append("\n");
      }
    }
    logger.info(" New Suggested Parameter " + idParameters);
    return idParameters.toString();
  }

  @Override
  public void computeValuesOfDerivedConfigurationParameters(List<TuningParameter> derivedParameterList,
      List<JobSuggestedParamValue> jobSuggestedParamValueList) {
    //  mrExecutionEngine.computeValuesOfDerivedConfigurationParameters(derivedParameterList, jobSuggestedParamValueList);
  }
}