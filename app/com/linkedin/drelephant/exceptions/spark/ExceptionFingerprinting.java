package com.linkedin.drelephant.exceptions.spark;

import com.linkedin.drelephant.analysis.AnalyticJob;
import java.util.List;

import static com.linkedin.drelephant.exceptions.spark.Constant.*;


/**
 * Every class who wants to do exception fingerprinting should extend this interface .
 * ExcpetionFingerprintingSpark is the one implementation for this interface.
 */

public interface ExceptionFingerprinting {
  /**
   * This method is used to process the raw data , provided by
   * analyticsJob . It will process the data and look for exception .
   * Creates the List of exceptionInfo .
   * @param analyticJob
   * @return
   */
  List<ExceptionInfo> processRawData(AnalyticJob analyticJob);

  /**
   *
   * @param exceptionInformation
   * @return Based on the exception information , it classifies failure into one of the LogClass
   * classes
   */
  LogClass classifyException(List<ExceptionInfo> exceptionInformation);

  /**
   *  Once the information is classified , this method will be used to save/persisit the information into DB
   * @param jobExecutionID
   * @return true if the information is successfully saved .
   * @throws Exception : Return exception if information is not been saved successfully.
   */
  boolean saveData(String jobExecutionID) throws Exception;
}
