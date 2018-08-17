package com.linkedin.drelephant.tuning.hbt;

import com.linkedin.drelephant.tuning.AbstractBaselineManager;
import java.util.List;
import models.TuningJobDefinition;


public class BaselineManagerHBT extends AbstractBaselineManager {
  @Override
  protected List<TuningJobDefinition> detectJobsForBaseLineComputation() {
    return null;
  }
}
