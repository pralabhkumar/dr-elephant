package com.linkedin.drelephant.exceptions.spark;

import com.linkedin.drelephant.analysis.AnalyticJob;
import com.linkedin.drelephant.spark.fetchers.statusapiv1.StageData;
import java.util.List;
import models.AppResult;
import static com.linkedin.drelephant.exceptions.spark.Classifier.LogClass;


public interface ExceptionFingerprinting {
  List<ExceptionInfo>  processRawData(AnalyticJob analyticJob);
  LogClass classifyException(List<ExceptionInfo> exceptionInformation);
  boolean saveData (String jobExecutionID) throws Exception;
}
