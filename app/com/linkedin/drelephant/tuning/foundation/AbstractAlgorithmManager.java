package com.linkedin.drelephant.tuning.foundation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.drelephant.tuning.AutoTuningOptimizeManager;
import com.linkedin.drelephant.tuning.JobTuningInfo;
import com.linkedin.drelephant.tuning.OptimizationAlgoFactory;
import com.linkedin.drelephant.tuning.Particle;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import models.JobDefinition;
import models.JobExecution;
import models.JobSavedState;
import models.JobSuggestedParamSet;
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import models.TuningParameter;
import org.apache.log4j.Logger;
import play.libs.Json;


public abstract class AbstractAlgorithmManager implements Manager {
  protected final String JSON_CURRENT_POPULATION_KEY = "current_population";
  private final Logger logger = Logger.getLogger(getClass());
  protected abstract List<JobTuningInfo> detectJobsForParameterGeneration();
  protected ExecutionEngine _executionEngine;

  protected  Boolean generateParameters(List<JobTuningInfo> jobsForParameterSuggestion){
    List<JobTuningInfo> updatedJobTuningInfoList = new ArrayList<JobTuningInfo>();
    for (JobTuningInfo jobTuningInfo : jobsForParameterSuggestion) {
      JobTuningInfo newJobTuningInfo = generateParamSet(jobTuningInfo);
      updatedJobTuningInfoList.add(newJobTuningInfo);
    }
    return true;
  }


  protected abstract JobTuningInfo generateParamSet(JobTuningInfo jobTuningInfo);


  protected abstract Boolean updateDatabase(List<JobTuningInfo> tuningJobDefinitions);

  public final Boolean execute() {
    logger.info("Executing Tuning Algorithm");
    Boolean parameterGenerationDone = false, databaseUpdateDone = false, updateMetricsDone = false;
    List<JobTuningInfo> jobTuningInfo = detectJobsForParameterGeneration();
    if (jobTuningInfo != null && jobTuningInfo.size() >= 1) {
      logger.info("Generating Parameters ");
      parameterGenerationDone = generateParameters(jobTuningInfo);
    }
    if (parameterGenerationDone) {
      logger.info("Updating Database");
      databaseUpdateDone = updateDatabase(jobTuningInfo);
    }
    logger.info("Param Generation Done");
    return databaseUpdateDone;
  }
}
