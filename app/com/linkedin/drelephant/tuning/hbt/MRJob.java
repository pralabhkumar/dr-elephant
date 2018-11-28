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


public class MRJob {
  private Map<String, Double> suggestedParameter = null;
  private List<MRApplicationData> applications = null;
  private Map<String, String> appliedParameter = null;
  private Set<String> suggestedParameterNames = null;
  MRJob(List<AppResult> results, MRExecutionEngine mrExecutionEngine) {
    setAppliedParameter(results);
    parsedApplication(results);
    applications = new ArrayList<MRApplicationData>();
    suggestedParameter = new HashMap<String,Double>();
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

  public void parsedApplication(List<AppResult> results) {
    for (AppResult result : results) {
      MRApplicationData mrApplicationData = new MRApplicationData(result, appliedParameter);
      mrApplicationData.processForSuggestedParameter();
      suggestedParameterNames.addAll(mrApplicationData.getSuggestedParameter().keySet());
      applications.add(mrApplicationData);
    }
  }

  public Map<String, Double> getSuggestedParameter() {
    intialize();
    for (MRApplicationData mrApplicationData : applications) {
      Map<String,Double> applicationSuggestedParameter = mrApplicationData.getSuggestedParameter();
      for(String parameterName : applicationSuggestedParameter.keySet()){
        Double valueSoFar = suggestedParameter.get(parameterName);
        Double valueAsperCurrentApplication = applicationSuggestedParameter.get(parameterName);
        suggestedParameter.put(parameterName,Math.max(valueSoFar,valueAsperCurrentApplication));
      }
    }
    return null;
  }

  private void intialize(){
    for(String name : suggestedParameterNames){
      suggestedParameter.put(name,0.0);
    }
  }
}
