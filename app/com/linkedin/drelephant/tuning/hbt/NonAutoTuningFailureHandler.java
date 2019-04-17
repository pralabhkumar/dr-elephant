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
import java.util.List;
import models.JobExecution;
import models.JobSuggestedParamSet;
import models.TuningJobExecutionParamSet;
import org.apache.log4j.Logger;


/**
 * This class will handle the scenario if the failure has
 * happened because of other reasons than autotuning  .
 */
public class NonAutoTuningFailureHandler  implements FailureHandler  {
  private final Logger logger = Logger.getLogger(getClass());
  private JobExecution jobExecution;
  private JobSuggestedParamSet jobSuggestedParamSet;
  private AbstractFitnessManager _abstractFitnessManager;
  private final int PARAMETER_RETRY_THRESHOLD = 2;

  @Override
  public void calculateFitness(JobExecution jobExecution, JobSuggestedParamSet jobSuggestedParamSet, AbstractFitnessManager fitnessManager) {
    this.jobExecution = jobExecution;
    this.jobSuggestedParamSet = jobSuggestedParamSet;
    this._abstractFitnessManager = fitnessManager;
    handleFailure();
  }
  private void handleFailure(){
    if(_abstractFitnessManager.alreadyFitnessComputed(jobSuggestedParamSet)){
      logger.info(" Fitness for parameter is already computed "+jobSuggestedParamSet.id);
      _abstractFitnessManager.assignDefaultValuesToJobExecution(jobExecution);
      this.jobExecution.save();
    }
    else {
      logger.info(" Retry is not because of AutoTuning  ");
      List<TuningJobExecutionParamSet> tuningJobExecutionParamSets = TuningJobExecutionParamSet.find.select("*")
          .where()
          .eq(TuningJobExecutionParamSet.TABLE.jobSuggestedParamSet + "." + JobSuggestedParamSet.TABLE.id,
              jobSuggestedParamSet.id)
          .eq(TuningJobExecutionParamSet.TABLE.isRetried, true)
          .findList();
      logger.info("Number of times this parameter retried " + tuningJobExecutionParamSets.size());
      if (tuningJobExecutionParamSets.size() >= PARAMETER_RETRY_THRESHOLD) {
        _abstractFitnessManager.applyPenalty(jobSuggestedParamSet, jobExecution);
      } else {
        logger.info(" Since parameter have no fault , no need to apply penalty ");
        _abstractFitnessManager.resetParamSetToCreated(jobSuggestedParamSet, jobExecution);
      }
      this.jobExecution.update();
      this.jobSuggestedParamSet.update();
    }
  }
}
