package com.linkedin.drelephant.exceptions.spark;

/**
 * Thic class have Constants which are used across
 * exception fingerprinting classes or which can be more often modifieable
 * by developers
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

  public static final String[] REGEX_FOR_EXCEPTION_IN_LOGS =
      {"^.+Exception.*", "^.+Error.*", ".*Container\\s+killed.*"};

  public static final String[] REGEX_AUTO_TUNING_FAULT =
      {".*java.lang.OutOfMemoryError.*", ".*Container .* is running beyond virtual memory limits.*",
          ".*Container killed on request. Exit code is 103.*", ".*Container killed on request. Exit code is 104.*",
          ".*exitCode=103.*", ".*exitCode=104.*"};

  public enum RulePriority {LOW, MEDIUM, HIGH}

  /**
   * Less than this , read complete log
   */
  public static final long FIRST_THRESHOLD_LOG_LENGTH_IN_BYTES = 260059;
  /**
   *  If log length is between first and last then read last five percentile .
   *  If the log length is greater than Last threshold then read THRESHOLD_LOG_INDEX_FROM_END_IN_BYTES
   *
   */
  public static final long LAST_THRESHOLD_LOG_LENGTH_IN_BYTES = 1000000;
  public static final float THRESHOLD_PERCENTAGE_OF_LOG_TO_READ_FROM_LAST = 0.95f;
  public static final long THRESHOLD_LOG_INDEX_FROM_END_IN_BYTES = 50000;
  public static final long MINIMUM_LOG_LENGTH_IN_BYTES_TO_SKIP=1000;
}
