/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.drelephant.tuning.hbt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import models.AppHeuristicResult;
import models.AppHeuristicResultDetails;
import models.AppResult;
import org.apache.log4j.Logger;


/**
 * This class represents Map Reduce Job and the suggested parameter and the Map Reduce Job level.
 */
public class MRJob {
  private final Logger logger = Logger.getLogger(getClass());
  private Map<String, Double> suggestedParameter = null;
  private List<MRApplicationData> applications = null;
  private Map<String, String> appliedParameter = null;
  private List<AppResult> applicationResults = null;

  public MRJob(List<AppResult> results) {
    this.applicationResults = results;
    this.appliedParameter = new HashMap<String, String>();
    setAppliedParameter(results);
    applications = new ArrayList<MRApplicationData>();
    suggestedParameter = new HashMap<String, Double>();
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

  public Map<String, String> getAppliedParameter() {
    return this.appliedParameter;
  }

  public List<MRApplicationData> getApplicationAnalyzedData() {
    return applications;
  }

  public void analyzeAllApplications() {
    for (AppResult result : this.applicationResults) {
      MRApplicationData mrApplicationData = new MRApplicationData(result, appliedParameter);
      applications.add(mrApplicationData);
    }
  }

  public void processJobForParameter() {
    for (MRApplicationData mrApplicationData : applications) {
      Map<String, Double> applicationSuggestedParameter = mrApplicationData.getSuggestedParameter();
      for (String parameterName : applicationSuggestedParameter.keySet()) {
        Double valueSoFar = suggestedParameter.get(parameterName);
        if (valueSoFar == null) {
          suggestedParameter.put(parameterName, applicationSuggestedParameter.get(parameterName));
        } else {
          Double valueAsperCurrentApplication = applicationSuggestedParameter.get(parameterName);
          suggestedParameter.put(parameterName, Math.max(valueSoFar, valueAsperCurrentApplication));
        }
      }
    }
  }

  public Map<String, Double> getJobSuggestedParameter() {
    return this.suggestedParameter;
  }
}
