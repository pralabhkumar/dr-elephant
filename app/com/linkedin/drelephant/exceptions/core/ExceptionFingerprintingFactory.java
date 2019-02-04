package com.linkedin.drelephant.exceptions.core;

import com.linkedin.drelephant.analysis.HadoopApplicationData;
import com.linkedin.drelephant.exceptions.ExceptionFingerprinting;
import com.linkedin.drelephant.spark.data.SparkApplicationData;

import static com.linkedin.drelephant.exceptions.util.Constant.*;

import com.linkedin.drelephant.spark.fetchers.statusapiv1.StageData;
import org.apache.log4j.Logger;
import scala.collection.Seq;
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
        Seq<StageData> stagesWithFailedTasks = ((SparkApplicationData) data).stagesWithFailedTasks();
        if (stagesWithFailedTasks != null) {
          logger.info(
              " Size of stages with failed task " + stagesWithFailedTasks.size());
          return new ExceptionFingerprintingSpark(WrapAsJava$.MODULE$.seqAsJavaList(stagesWithFailedTasks));
        }else{
          return new ExceptionFingerprintingSpark(null);
        }

      case MR:
        logger.info(" MR Exception Fingerprinting  is called ");
        return null;
      default:
        logger.error(" Unknown execution engine type ");
    }
    return null;
  }
}
