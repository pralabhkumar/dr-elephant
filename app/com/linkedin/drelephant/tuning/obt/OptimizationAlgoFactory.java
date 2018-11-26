package com.linkedin.drelephant.tuning.obt;

import com.linkedin.drelephant.tuning.engine.MRExecutionEngine;
import com.linkedin.drelephant.tuning.engine.SparkExecutionEngine;
import models.TuningAlgorithm;
import org.apache.log4j.Logger;


/**
 * Optimization Algo Factory . Based on the Tuning Type , algorithm type & execution engine type.
 * Factory will produce one of the tuningtype manager object
 */
public class OptimizationAlgoFactory {
  private static final Logger logger = Logger.getLogger(OptimizationAlgoFactory.class);

  public static ParameterGenerateManagerOBT getOptimizationAlogrithm(TuningAlgorithm tuningAlgorithm) {
    if (tuningAlgorithm.optimizationAlgo.name().equals(TuningAlgorithm.OptimizationAlgo.PSO_IPSO.name())
        && tuningAlgorithm.jobType.name().equals(TuningAlgorithm.JobType.PIG.name())) {
      logger.info("OPTIMIZATION ALGORITHM PSO_IPSO MR");
      return new ParameterGenerateManagerOBTAlgoPSOIPSOMRImpl<MRExecutionEngine>(new MRExecutionEngine());
    }
    if (tuningAlgorithm.optimizationAlgo.name().equals(TuningAlgorithm.OptimizationAlgo.PSO_IPSO.name())
        && tuningAlgorithm.jobType.name().equals(TuningAlgorithm.JobType.SPARK.name())) {
      logger.info("OPTIMIZATION ALGORITHM PSO_IPSO SPARK");
      return new ParameterGenerateManagerOBTAlgoPSOIPSOSparkImpl<SparkExecutionEngine>(new SparkExecutionEngine());
    }
    if (tuningAlgorithm.optimizationAlgo.name().equals(TuningAlgorithm.OptimizationAlgo.PSO.name())
        && tuningAlgorithm.jobType.name().equals(TuningAlgorithm.JobType.PIG.name())) {
      logger.info("OPTIMIZATION ALGORITHM PSO PIG");
      return new ParameterGenerateManagerOBTAlgoPSOMRImpl<MRExecutionEngine>(new MRExecutionEngine());
    }
    if (tuningAlgorithm.optimizationAlgo.name().equals(TuningAlgorithm.OptimizationAlgo.PSO.name())
        && tuningAlgorithm.jobType.name().equals(TuningAlgorithm.JobType.SPARK.name())) {
      logger.info("OPTIMIZATION ALGORITHM PSO SPARK");
      return new ParameterGenerateManagerOBTAlgoPSOSparkImpl<SparkExecutionEngine>(new SparkExecutionEngine());
    }
    logger.info("OPTIMIZATION ALGORITHM HBT");
    return null;
  }
}
