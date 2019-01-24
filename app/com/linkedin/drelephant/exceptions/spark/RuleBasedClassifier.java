package com.linkedin.drelephant.exceptions.spark;

import java.util.List;
import org.apache.log4j.Logger;


public class RuleBasedClassifier implements Classifier {
  private static final Logger logger = Logger.getLogger(ExceptionFingerprintingSpark.class);
  private List<ExceptionInfo> dataToClassify = null;
  @Override
  public LogClass classify(List<ExceptionInfo> exceptions) {
    this.dataToClassify = exceptions;
   return outOfMemoryEarlyExitRule();
  }

  /**
   * This rule checks for Out of Memory and even if one log have Out of Memory. It will classify the
   * exceptions to Auto Tune Enabled exception . Another variation of this rule is to check how many
   * logs have out of memory exception/error and based on that decide whether its autotuning error or not.
   * @return : If its out of memory error
   */
  private LogClass outOfMemoryEarlyExitRule() {
    for (ExceptionInfo exceptionInfo : dataToClassify) {
      if((exceptionInfo.getExceptionName()+" "+exceptionInfo.getExcptionStackTrace()).contains("java.lang.OutOfMemoryError")){
        logger.info(" AutoTuning Fault ");
        return LogClass.AUTOTUINING_ENABLED;
      }
    }
    logger.info(" User Fault ");
    return LogClass.USER_ENABLED;
  }
}
