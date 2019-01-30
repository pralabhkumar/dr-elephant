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
      {"^.+Exception.*[\n]+", "^.+Error.*[\n]+", ".*Container\\s+killed.*[\n]+"};

  public static final String[] REGEX_AUTO_TUNING_FAULT =
      {".*java.lang.OutOfMemoryError.*", ".*Container .* is running beyond virtual memory limits.*",
          ".*Container killed on request. Exit code is 103.*", ".*Container killed on request. Exit code is 104.*",
          ".*Container killed on request. Exit code is 143.*"};

  public enum RulePriority {LOW, MEDIUM, HIGH}
}
