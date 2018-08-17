package com.linkedin.drelephant.tuning.obt;

import models.TuningAlgorithm;
import org.apache.log4j.Logger;


public class OptimizationAlgoFactory {
  private static final Logger logger = Logger.getLogger(OptimizationAlgoFactory.class);

  public static AutoTuningOptimizeManager getOptimizationAlogrithm(TuningAlgorithm tuningAlgorithm) {
    if (tuningAlgorithm.optimizationAlgo.name().equals(TuningAlgorithm.OptimizationAlgo.PSO_IPSO.name())) {
      logger.info("OPTIMIZATION ALGORITHM PSO_IPSO");
      return new IPSOManager();
    }
    logger.info("OPTIMIZATION ALGORITHM PSO");
    return null;
  }
}
