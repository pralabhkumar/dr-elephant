package com.linkedin.drelephant.tuning;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;


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
