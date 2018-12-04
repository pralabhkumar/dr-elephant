package com.linkedin.drelephant.tuning.hbt;

import com.linkedin.drelephant.tuning.engine.MRExecutionEngine;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import models.AppHeuristicResult;
import models.AppHeuristicResultDetails;
import models.AppResult;
import org.apache.log4j.Logger;


public class MRJob {
  private final Logger logger = Logger.getLogger(getClass());
  private boolean isDebugEnabled = logger.isDebugEnabled();
  private Map<String, Double> suggestedParameter = null;
  private List<MRApplicationData> applications = null;
  private Map<String, String> appliedParameter = null;
  private Set<String> suggestedParameterNames = null;
  private List<AppResult> applicationResults = null;
  public MRJob(List<AppResult> results, MRExecutionEngine mrExecutionEngine) {
    this.applicationResults=results;
    this.appliedParameter = new HashMap<String,String>();
    setAppliedParameter(results);
    applications = new ArrayList<MRApplicationData>();
    suggestedParameter = new HashMap<String, Double>();
    suggestedParameterNames = new HashSet<String>();
  }

  private void setAppliedParameter(List<AppResult> results) {
    if (results != null && results.size() >= 0) {
      AppResult appResult = results.get(0);
      for (AppHeuristicResult appHeuristicResult : appResult.yarnAppHeuristicResults) {
        if (appHeuristicResult != null && appHeuristicResult.heuristicName.equals("MapReduceConfiguration")) {
          for (AppHeuristicResultDetails appHeuristicResultDetails : appHeuristicResult.yarnAppHeuristicResultDetails) {
            appliedParameter.put(appHeuristicResultDetails.name, appHeuristicResultDetails.value);
          }
        }
      }
    }
  }

  public Map<String, String> getAppliedParameter(){
    return this.appliedParameter;
  }

  public List<MRApplicationData> getApplicationAnalyzedData(){
    return applications;
  }

  public void analyzeAllApplications() {
    for (AppResult result : this.applicationResults) {
      MRApplicationData mrApplicationData = new MRApplicationData(result, appliedParameter);
      suggestedParameterNames.addAll(mrApplicationData.getSuggestedParameter().keySet());
      applications.add(mrApplicationData);
    }
  }

  public void processJobForParameter() {
    initialize();
    for (MRApplicationData mrApplicationData : applications) {
      Map<String, Double> applicationSuggestedParameter = mrApplicationData.getSuggestedParameter();
      for (String parameterName : applicationSuggestedParameter.keySet()) {
        Double valueSoFar = suggestedParameter.get(parameterName);
        Double valueAsperCurrentApplication = applicationSuggestedParameter.get(parameterName);
        suggestedParameter.put(parameterName, Math.max(valueSoFar, valueAsperCurrentApplication));
      }
    }
  }

  public Map<String, Double> getSuggestedParameter() {
    return this.suggestedParameter;
  }

  private void initialize() {
    for (String name : suggestedParameterNames) {
      suggestedParameter.put(name, 0.0);
    }
  }
}
