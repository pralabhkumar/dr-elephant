package com.linkedin.drelephant.exceptions.core;

import com.linkedin.drelephant.exceptions.Classifier;
import org.apache.log4j.Logger;

import static com.linkedin.drelephant.exceptions.util.Constant.*;


/**
 * This is factory class to produce the classifier object based on
 * the ClassifierType input
 */

public class ClassifierFactory {
  private static final Logger logger = Logger.getLogger(ClassifierFactory.class);

  /**
   *
   * @param classifierTypes
   * @return Return classifier object based on the type provided
   */
  public static Classifier getClassifier(ClassifierTypes classifierTypes) {
    switch (classifierTypes) {
      case RULE_BASE_CLASSIFIER:
        logger.info(" Rule Based classifier is called ");
        return new RuleBasedClassifier();
      case ML_BASED_CLASSIFIER:
        logger.info(" ML Based classifier is called ");
        return null;
      default:
        logger.error(" Unknown classifier type is passed ");
    }
    return null;
  }
}
