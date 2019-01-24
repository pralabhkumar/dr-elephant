package com.linkedin.drelephant.exceptions.spark;

import com.linkedin.drelephant.analysis.AnalyticJob;
import com.linkedin.drelephant.analysis.HadoopApplicationData;
import com.linkedin.drelephant.spark.fetchers.statusapiv1.StageData;
import java.util.List;
import models.AppResult;
import org.apache.log4j.Logger;

import static com.linkedin.drelephant.exceptions.spark.ExceptionFingerprintingFactory.*;
import static com.linkedin.drelephant.exceptions.spark.Classifier.LogClass;


public class ExceptionFingerprintingRunner implements Runnable {
  private static final Logger logger = Logger.getLogger(ExceptionFingerprintingRunner.class);
  private AnalyticJob _analyticJob;
  private AppResult _appResult;
  private HadoopApplicationData data;
  private ExecutionEngineTypes exceutionTypes;

  public ExceptionFingerprintingRunner(AnalyticJob analyticJob, AppResult appResult, HadoopApplicationData data,
      ExecutionEngineTypes exceutionTypes) {
    this._analyticJob = analyticJob;
    this._appResult = appResult;
    this.data = data;
    this.exceutionTypes = exceutionTypes;
  }

  @Override
  public void run() {
    try {
      logger.info( " Exception Fingerprinting thread started for app " + _analyticJob.getAppId());
      ExceptionFingerprinting exceptionFingerprinting =
          ExceptionFingerprintingFactory.getExceptionFingerprinting(exceutionTypes,data);
      List<ExceptionInfo> exceptionInfos = exceptionFingerprinting.processRawData(_analyticJob);
      LogClass logclass = exceptionFingerprinting.classifyException(exceptionInfos);
      boolean isAutoTuningFault = false;
      if (logclass.equals(LogClass.AUTOTUINING_ENABLED)) {
        isAutoTuningFault = true;
      }
      if (isAutoTuningFault) {
        logger.info(" Since auto tuning fault , saving information into db for execution id "+ _appResult.jobExecId);
        exceptionFingerprinting.saveData(_appResult.jobExecId);
      }
    } catch (Exception e) {
      logger.error(" Error while processing exception fingerprinting for app " + _analyticJob.getAppId(), e);
    }
  }
}
