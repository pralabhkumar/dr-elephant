package com.linkedin.drelephant.exceptions.spark;

import com.linkedin.drelephant.analysis.HadoopApplicationData;
import com.linkedin.drelephant.spark.data.SparkApplicationData;

import static com.linkedin.drelephant.exceptions.spark.Constant.*;

import org.apache.log4j.Logger;
import scala.collection.convert.WrapAsJava$;


/**
 * Factory class to produce ExceptionFingerprinting object based on execution engine type.
 */
public class ExceptionFingerprintingFactory {
  private static final Logger logger = Logger.getLogger(ExceptionFingerprintingFactory.class);

  public static ExceptionFingerprinting getExceptionFingerprinting(ExecutionEngineTypes executionEngineTypes,
      HadoopApplicationData data) {
    switch (executionEngineTypes) {
      case SPARK:
        logger.info(" Spark Exception Fingerprinting is called ");
        return new ExceptionFingerprintingSpark(
            WrapAsJava$.MODULE$.seqAsJavaList(((SparkApplicationData) data).stagesWithFailedTasks()));
      case MR:
        logger.info(" MR Exception Fingerprinting  is called ");
        return null;
      default :
        logger.error(" Unknown execution engine type ");
    }
    return null;
  }
}
