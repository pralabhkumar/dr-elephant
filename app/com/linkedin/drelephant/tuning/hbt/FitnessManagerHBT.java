package com.linkedin.drelephant.tuning.hbt;

import com.linkedin.drelephant.tuning.foundation.AbstractFitnessManager;
import java.util.List;
import models.TuningJobExecutionParamSet;


public class FitnessManagerHBT extends AbstractFitnessManager {
  @Override
  protected List<TuningJobExecutionParamSet> detectJobsForFitnessComputation() {
    return null;
  }
}
