package com.linkedin.drelephant.exceptions.util;



/**
 * Thic class have Constants which are used across
 * exception fingerprinting classes or configuration properties name
 */
public final class Constant {

  /**
   * There are two possible type of Classifier , one is RULE Based
   * and another one is ML Based (which can involve supervised classification)
   */
  public enum ClassifierTypes {
    RULE_BASE_CLASSIFIER, ML_BASED_CLASSIFIER
  }

  /**
   * Classes in which classifier should classify the exceptions
   * . It can have more classes in future releases , which can further
   * classify user enabled class
   */
  public enum LogClass {
    USER_ENABLED, AUTOTUNING_ENABLED
  }

  /**
   *  Exception Fingerprinting will depend on execution engines .
   *  So different type of execution engines .
   */
  public enum ExecutionEngineTypes {
    SPARK, MR
  }

  public enum RulePriority {LOW, MEDIUM, HIGH}

  static final String REGEX_FOR_EXCEPTION_IN_LOGS_NAME = "ef.regex.for.exception";
  static final String REGEX_AUTO_TUNING_FAULT_NAME = "ef.regex.for.autotuning.fault";
  static final String FIRST_THRESHOLD_LOG_LENGTH_NAME = "ef.first.threshold.loglength";
  static final String LAST_THRESHOLD_LOG_LENGTH_NAME = "ef.last.threshold.loglength";
  static final String THRESHOLD_PERCENTAGE_OF_LOG_TO_READ_NAME = "ef.threshold.percentage.log";
  static final String THRESHOLD_LOG_INDEX_FROM_END_IN_BYTES_NAME = "ef.threshold.log.index";
  static final String MINIMUM_LOG_LENGTH_TO_SKIP_IN_BYTES_NAME = "ef.minimum.loglength.skip.start";
  static final String NUMBER_OF_STACKTRACE_LINE_NAME = "ef.stacktrace.lines";
  static final String JHS_TIME_OUT_NAME = "ef.jhs.timeout";
  static final String THRESHOLD_LOG_LINE_LENGTH_NAME = "ef.log.line.threshold";
}
