package com.linkedin.drelephant.tuning.hbt;

import com.linkedin.drelephant.tuning.JobTuningInfo;
import com.linkedin.drelephant.tuning.foundation.AbstractAlgorithmManager;
import com.linkedin.drelephant.tuning.foundation.ExecutionEngine;
import java.util.List;
import models.TuningJobDefinition;


public class AlgorithmManagerHBT extends AbstractAlgorithmManager{
  private ExecutionEngine _executionEngine;
  public AlgorithmManagerHBT(ExecutionEngine executionEngine){
    this._executionEngine=executionEngine;
  }

  @Override
  protected List<JobTuningInfo> detectJobsForParameterGeneration() {
    return null;
  }

  @Override
  protected JobTuningInfo generateParamSet(JobTuningInfo jobTuningInfo) {
    return null;
  }

  @Override
  protected Boolean updateDatabase(List<JobTuningInfo> tuningJobDefinitions) {
    return null;
  }
}
