package com.linkedin.drelephant.tuning.hbt;

import com.linkedin.drelephant.tuning.AbstractFitnessManager;
import java.util.List;
import models.AppResult;
import models.JobExecution;
import models.JobSuggestedParamSet;
import models.TuningJobDefinition;
import models.TuningJobExecutionParamSet;


public class FitnessManagerHBT extends AbstractFitnessManager {
  @Override
  protected List<TuningJobExecutionParamSet> detectJobsForFitnessComputation() {
    return null;
  }

  @Override
  protected void calculateAndUpdateFitness(JobExecution jobExecution, List<AppResult> results,
      TuningJobDefinition tuningJobDefinition, JobSuggestedParamSet jobSuggestedParamSet) {

  }



  @Override
  public String getManagerName() {
    return "FitnessManagerHBT";
  }
}
