package com.linkedin.drelephant.exceptions.spark;
import org.apache.log4j.Logger;

public class ClassifierFactory {
  private static final Logger logger = Logger.getLogger(ClassifierFactory.class);
  public enum ClassifierTypes {RuleBaseClassifier, MLBasedClassifer}
  public static Classifier getClassifier(ClassifierTypes classiferTypes) {
    switch (classiferTypes) {
      case RuleBaseClassifier:
        logger.info(" Rule Based classifier is called ");
        return new RuleBasedClassifier();
      case MLBasedClassifer:
        logger.info(" ML Based classifier is called ");
        return null;
    }
    return null;
  }
}
