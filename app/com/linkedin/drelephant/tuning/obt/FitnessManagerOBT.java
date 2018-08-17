package com.linkedin.drelephant.tuning.obt;

import com.avaje.ebean.Expr;
import com.linkedin.drelephant.AutoTuner;
import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.tuning.foundation.AbstractFitnessManager;
import com.linkedin.drelephant.util.Utils;
import java.util.ArrayList;
import java.util.List;
import models.JobExecution;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import models.TuningJobExecutionParamSet;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;


public class FitnessManagerOBT extends AbstractFitnessManager {
  private final Logger logger = Logger.getLogger(getClass());

  public FitnessManagerOBT() {
    Configuration configuration = ElephantContext.instance().getAutoTuningConf();

    // Time duration to wait for computing the fitness of a param set once the corresponding execution is completed
    fitnessComputeWaitInterval =
        Utils.getNonNegativeLong(configuration, FITNESS_COMPUTE_WAIT_INTERVAL, 5 * AutoTuner.ONE_MIN);

    // Time duration to wait for metrics (resource usage, execution time) of an execution to be computed before
    // discarding it for fitness computation
    ignoreExecutionWaitInterval =
        Utils.getNonNegativeLong(configuration, IGNORE_EXECUTION_WAIT_INTERVAL, 2 * 60 * AutoTuner.ONE_MIN);

    // #executions after which tuning will stop even if parameters don't converge
    maxTuningExecutions = Utils.getNonNegativeInt(configuration, MAX_TUNING_EXECUTIONS, 39);

    // #executions before which tuning cannot stop even if parameters converge
    minTuningExecutions = Utils.getNonNegativeInt(configuration, MIN_TUNING_EXECUTIONS, 18);
  }

  @Override
  protected List<TuningJobExecutionParamSet> detectJobsForFitnessComputation() {
    logger.info("Fetching completed executions whose fitness are yet to be computed");
    List<TuningJobExecutionParamSet> completedJobExecutionParamSet = new ArrayList<TuningJobExecutionParamSet>();

    List<TuningJobExecutionParamSet> tuningJobExecutionParamSets = TuningJobExecutionParamSet.find.select("*")
        .fetch(TuningJobExecutionParamSet.TABLE.jobExecution, "*")
        .fetch(TuningJobExecutionParamSet.TABLE.jobSuggestedParamSet, "*")
        .where()
        .or(Expr.or(Expr.eq(TuningJobExecutionParamSet.TABLE.jobExecution + '.' + JobExecution.TABLE.executionState,
            JobExecution.ExecutionState.SUCCEEDED),
            Expr.eq(TuningJobExecutionParamSet.TABLE.jobExecution + '.' + JobExecution.TABLE.executionState,
                JobExecution.ExecutionState.FAILED)),
            Expr.eq(TuningJobExecutionParamSet.TABLE.jobExecution + '.' + JobExecution.TABLE.executionState,
                JobExecution.ExecutionState.CANCELLED))
        .isNull(TuningJobExecutionParamSet.TABLE.jobExecution + '.' + JobExecution.TABLE.resourceUsage)
        .eq(TuningJobDefinition.TABLE.tuningAlgorithm, TuningAlgorithm.OptimizationAlgo.PSO.name())
        .findList();

    logger.info("#completed executions whose metrics are not computed: " + tuningJobExecutionParamSets.size());

    for (TuningJobExecutionParamSet tuningJobExecutionParamSet : tuningJobExecutionParamSets) {
      JobExecution jobExecution = tuningJobExecutionParamSet.jobExecution;
      long diff = System.currentTimeMillis() - jobExecution.updatedTs.getTime();
      logger.info("Current Time in millis: " + System.currentTimeMillis() + ", Job execution last updated time "
          + jobExecution.updatedTs.getTime());
      if (diff < fitnessComputeWaitInterval) {
        logger.info("Delaying fitness compute for execution: " + jobExecution.jobExecId);
      } else {
        logger.info("Adding execution " + jobExecution.jobExecId + " to fitness computation queue");
        completedJobExecutionParamSet.add(tuningJobExecutionParamSet);
      }
    }
    logger.info(
        "Number of completed execution fetched for fitness computation: " + completedJobExecutionParamSet.size());
    return completedJobExecutionParamSet;
  }
}
