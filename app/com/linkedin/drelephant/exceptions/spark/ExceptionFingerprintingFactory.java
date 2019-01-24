package com.linkedin.drelephant.exceptions.spark;

import com.linkedin.drelephant.analysis.HadoopApplicationData;
import com.linkedin.drelephant.spark.data.SparkApplicationData;
import com.linkedin.drelephant.spark.fetchers.statusapiv1.StageData;
import java.util.List;
import org.apache.log4j.Logger;
import scala.collection.convert.WrapAsJava$;


public class ExceptionFingerprintingFactory {
  private static final Logger logger = Logger.getLogger(ExceptionFingerprintingFactory.class);

  public enum ExecutionEngineTypes {SPARK, MR}

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
    }
    return null;
  }
}
