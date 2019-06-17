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

import com.linkedin.drelephant.tuning.TuningHelper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import models.AppHeuristicResult;
import models.AppHeuristicResultDetails;
import models.AppResult;
import org.apache.log4j.Logger;

import static com.linkedin.drelephant.mapreduce.heuristics.CommonConstantsHeuristic.ParameterKeys.*;


/**
 * This class represents Map Reduce Job and the suggested parameter and the Map Reduce Job level.
 */
public class MRJob {
  private final Logger logger = Logger.getLogger(getClass());
  private boolean debugEnabled = logger.isDebugEnabled();
  private Map<String, Double> suggestedParameter = null;
  private List<MRApplicationData> applications = null;
  private Map<String, Double> appliedParameter = null;
  private List<AppResult> applicationResults = null;

  public MRJob(List<AppResult> results) {
    this.applicationResults = results;
    this.appliedParameter = new HashMap<String, Double>();
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
            try {
              if (appHeuristicResultDetails.name.equals(MAPPER_HEAP_HEURISTICS_CONF.getValue())
                  || appHeuristicResultDetails.name.equals(REDUCER_HEAP_HEURISTICS_CONF.getValue())) {
                appliedParameter.put(appHeuristicResultDetails.name,
                    TuningHelper.parseMaxHeapSizeInMB(appHeuristicResultDetails.value));
              } else {
                appliedParameter.put(appHeuristicResultDetails.name,
                    Double.parseDouble(appHeuristicResultDetails.value));
              }
            } catch (NumberFormatException e) {
              logger.error("Unable to parse configuration " + appHeuristicResultDetails.name, e);
            } catch (RuntimeException e) {
              logger.error(" Unknown error while parsing configuration " + appHeuristicResultDetails.name, e);
            }
          }
        }
      }
    }
  }

  public Map<String, Double> getAppliedParameter() {
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

  /**
   * Recommending parameter at the job level ,
   * by taking max of each suggested parameter across all
   * jobs
   */
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
    printInformation();
  }

  public Map<String, Double> getJobSuggestedParameter() {
    return this.suggestedParameter;
  }

  /**
   * This information will be changed to debug mode in future (once system is stable).
   */
  private void printInformation() {
    for (String parameter : appliedParameter.keySet()) {
      logger.info(" Parameter Applied  " + appliedParameter.get(parameter));
    }
    for (String suggested : suggestedParameter.keySet()) {
      logger.info(" Suggested Parameter " + suggestedParameter.get(suggested));
    }
  }
}
