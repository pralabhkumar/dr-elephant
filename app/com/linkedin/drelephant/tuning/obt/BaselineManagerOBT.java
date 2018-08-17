package com.linkedin.drelephant.tuning.obt;

import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.tuning.AbstractBaselineManager;
import com.linkedin.drelephant.util.Utils;
import java.util.List;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;


public class BaselineManagerOBT extends AbstractBaselineManager {
  private final Logger logger = Logger.getLogger(getClass());

  public BaselineManagerOBT() {
    NUM_JOBS_FOR_BASELINE_DEFAULT = 30;
    Configuration configuration = ElephantContext.instance().getAutoTuningConf();
    _numJobsForBaseline =
        Utils.getNonNegativeInt(configuration, super.BASELINE_EXECUTION_COUNT, NUM_JOBS_FOR_BASELINE_DEFAULT);
  }

  @Override
  protected List<TuningJobDefinition> detectJobsForBaseLineComputation() {
    logger.info("Fetching jobs for which baseline metrics need to be computed");
    List<TuningJobDefinition> tuningJobDefinitions = TuningJobDefinition.find.where()
        .eq(TuningJobDefinition.TABLE.averageResourceUsage, null)
        .eq(TuningJobDefinition.TABLE.tuningAlgorithm, TuningAlgorithm.OptimizationAlgo.PSO.name())
        .findList();
    return tuningJobDefinitions;
  }
}
