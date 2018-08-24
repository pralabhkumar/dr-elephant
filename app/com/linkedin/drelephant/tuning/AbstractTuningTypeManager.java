package com.linkedin.drelephant.tuning;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import models.AppResult;
import models.JobDefinition;
import models.JobSuggestedParamSet;
import models.JobSuggestedParamValue;
import models.TuningJobDefinition;
import models.TuningParameter;
import org.apache.log4j.Logger;


public abstract class AbstractTuningTypeManager implements Manager {
  protected final String JSON_CURRENT_POPULATION_KEY = "current_population";
  private final Logger logger = Logger.getLogger(getClass());
  protected ExecutionEngine _executionEngine;
  protected  String tuningType = null;
  protected String tuningAlgorithm = null;

  protected abstract List<JobTuningInfo> detectJobsForParameterGeneration();

  protected List<JobTuningInfo>  generateParameters(List<JobTuningInfo> jobsForParameterSuggestion) {
    List<JobTuningInfo> updatedJobTuningInfoList = new ArrayList<JobTuningInfo>();
    for (JobTuningInfo jobTuningInfo : jobsForParameterSuggestion) {
      JobTuningInfo newJobTuningInfo = generateParamSet(jobTuningInfo);
      updatedJobTuningInfoList.add(newJobTuningInfo);
    }
    return updatedJobTuningInfoList;
  }

  protected abstract JobTuningInfo generateParamSet(JobTuningInfo jobTuningInfo);

  protected abstract Boolean updateDatabase(List<JobTuningInfo> tuningJobDefinitions);

  public final Boolean execute() {
    logger.info("Executing Tuning Algorithm");
    Boolean parameterGenerationDone = false, databaseUpdateDone = false, updateMetricsDone = false;
    List<JobTuningInfo> jobTuningInfo = detectJobsForParameterGeneration();
    if (jobTuningInfo != null && jobTuningInfo.size() >= 1) {
      logger.info("Generating Parameters ");
      List<JobTuningInfo> updatedJobTuningInfoList= generateParameters(jobTuningInfo);
      logger.info("Updating Database");
      databaseUpdateDone = updateDatabase(updatedJobTuningInfoList);
    }
    logger.info("Param Generation Done");
    return databaseUpdateDone;
  }

  protected abstract void updateBoundryConstraint(List<TuningParameter> tuningParameterList,
      TuningJobDefinition tuningJobDefinition, JobDefinition job);


  public  abstract boolean isParamConstraintViolated(List<JobSuggestedParamValue> jobSuggestedParamValues);

  public abstract Map<String, Map<String, Double>> extractParameterInformation(List<AppResult> appResults);


}
