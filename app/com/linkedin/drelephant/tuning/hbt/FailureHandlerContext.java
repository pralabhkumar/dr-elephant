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

import com.linkedin.drelephant.tuning.AbstractFitnessManager;
import models.JobExecution;
import models.JobSuggestedParamSet;


/**
 * Context class , which will intialize FailureHandle and call corresponding
 * calculate Fintness
 */
public class FailureHandlerContext {
  private FailureHandler _failureHandler;

  public void setFailureHandler(FailureHandler failureHandler){
    this._failureHandler=failureHandler;
  }

  public void execute(JobExecution jobExecution, JobSuggestedParamSet jobSuggestedParamSet,AbstractFitnessManager abstractFitnessManager){
    _failureHandler.calculateFitness(jobExecution,jobSuggestedParamSet,abstractFitnessManager);
  }


}
