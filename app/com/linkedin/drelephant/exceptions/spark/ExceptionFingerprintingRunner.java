package com.linkedin.drelephant.exceptions.spark;

import com.linkedin.drelephant.analysis.AnalyticJob;
import com.linkedin.drelephant.analysis.HadoopApplicationData;
import java.util.List;
import models.AppResult;
import org.apache.log4j.Logger;

import static com.linkedin.drelephant.exceptions.spark.Constant.*;


/**
 * This class is actually responsible to run exception fingerprinting . Class which
 * want to run exception fingerprinting should call this call and run as seperate thread;
 */

public class ExceptionFingerprintingRunner implements Runnable {
  private static final Logger logger = Logger.getLogger(ExceptionFingerprintingRunner.class);
  private AnalyticJob _analyticJob;
  private AppResult _appResult;
  private HadoopApplicationData data;
  private ExecutionEngineTypes executionTypes;

  public ExceptionFingerprintingRunner(AnalyticJob analyticJob, AppResult appResult, HadoopApplicationData data,
      ExecutionEngineTypes exceutionTypes) {
    this._analyticJob = analyticJob;
    this._appResult = appResult;
    this.data = data;
    this.executionTypes = exceutionTypes;
  }

  @Override
  public void run() {
    long startTime = System.nanoTime();
    try {
      logger.info(" Exception Fingerprinting thread started for app " + _analyticJob.getAppId());
      ExceptionFingerprinting exceptionFingerprinting =
          ExceptionFingerprintingFactory.getExceptionFingerprinting(executionTypes, data);
      List<ExceptionInfo> exceptionInfos = exceptionFingerprinting.processRawData(_analyticJob);
      LogClass logclass = exceptionFingerprinting.classifyException(exceptionInfos);
      boolean isAutoTuningFault = false;
      if (logclass.equals(LogClass.AUTOTUNING_ENABLED)) {
        isAutoTuningFault = true;
      }
      if (isAutoTuningFault) {
        logger.info(" Since auto tuning fault , saving information into db for execution id " + _appResult.jobExecId);
        exceptionFingerprinting.saveData(_appResult.jobExecId);
      }
    } catch (Exception e) {
      logger.error(" Error while processing exception fingerprinting for app " + _analyticJob.getAppId(), e);
    }
    long endTime = System.nanoTime();
    logger.info(
        "Total time spent in exception fingerprinting in  " + _analyticJob.getAppId() + " " + + (endTime-startTime)*1.0/(1000000000.0) +"s");
  }
}
