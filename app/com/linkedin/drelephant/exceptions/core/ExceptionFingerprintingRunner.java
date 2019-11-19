/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.drelephant.exceptions.core;

import com.linkedin.drelephant.analysis.AnalyticJob;
import com.linkedin.drelephant.analysis.HadoopApplicationData;
import com.linkedin.drelephant.exceptions.ExceptionFingerprinting;
import com.linkedin.drelephant.exceptions.HadoopException;
import com.linkedin.drelephant.exceptions.util.ExceptionInfo;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.persistence.PersistenceException;
import models.AppResult;
import models.JobsExceptionFingerPrinting;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import static com.linkedin.drelephant.exceptions.util.Constant.*;
import static com.linkedin.drelephant.exceptions.util.ExceptionUtils.ConfigurationBuilder.*;


/**
 * This class is actually responsible to run exception fingerprinting . Class which
 * want to run exception fingerprinting should call this .;
 * It implemented Runnable because it will run as separate thread in future versions
 */
public class ExceptionFingerprintingRunner implements Runnable {
  private static final Logger logger = Logger.getLogger(ExceptionFingerprintingRunner.class);
  private AnalyticJob _analyticJob;
  private AppResult _appResult;
  private HadoopApplicationData data;
  private ExecutionEngineType executionType;
  private String jobNameRegex = ".*&job=(.*)&.*";
  private Pattern jobNamePattern = Pattern.compile(jobNameRegex);

  public ExceptionFingerprintingRunner(AnalyticJob analyticJob, AppResult appResult, HadoopApplicationData data,
      ExecutionEngineType executionType) {
    this._analyticJob = analyticJob;
    this._appResult = appResult;
    this.data = data;
    this.executionType = executionType;
  }

  @Override
  public void run() {
    long startTime = System.nanoTime();
    try {
      logger.info(" Exception Fingerprinting thread started for app " + _analyticJob.getAppId());
      ExceptionFingerprinting exceptionFingerprinting =
          ExceptionFingerprintingFactory.getExceptionFingerprinting(executionType, data);
      List<ExceptionInfo> exceptionInfos = exceptionFingerprinting.processRawData(_analyticJob);
      saveDriverExceptionLogForExceptionFingerPrinting(exceptionInfos,
          exceptionFingerprinting.getExceptionLogSourceInformation());
      LogClass logClass = exceptionFingerprinting.classifyException(exceptionInfos);
      boolean isAutoTuningFault = false;
      if (logClass != null && logClass.equals(LogClass.AUTOTUNING_ENABLED)) {
        isAutoTuningFault = true;
      }
      if (isAutoTuningFault) {
        logger.info(" Since auto tuning fault , saving information into db for execution id " + _appResult.jobExecId);
        exceptionFingerprinting.saveData(_appResult.jobExecId);
      }
    } catch (Exception e) {
      logger.error(" Error while processing exception fingerprinting for app " + _analyticJob.getAppId(), e);
    }
    long endTime = System.nanoTime();
    logger.info("Total time spent in exception fingerprinting in  " + _analyticJob.getAppId() + " "
        + (endTime - startTime) * 1.0 / (1000000000.0) + "s");
  }

  /**
   *
   * @param exceptionInfoList : List contains all the exceptions
   * @param logSourceInformation : Map containing sources of exception
   */
  private void saveDriverExceptionLogForExceptionFingerPrinting(List<ExceptionInfo> exceptionInfoList,
      Map<String, String> logSourceInformation) {
    if (exceptionInfoList != null) {
      final String NOT_APPLICABLE = "NA";

      String exceptionsTrace = parseExceptions(exceptionInfoList,true);

      JobsExceptionFingerPrinting jobsExceptionFingerPrinting = new JobsExceptionFingerPrinting();
      jobsExceptionFingerPrinting.appId = NOT_APPLICABLE;
      jobsExceptionFingerPrinting.taskId = NOT_APPLICABLE;
      jobsExceptionFingerPrinting.flowExecUrl = _appResult.flowExecUrl;
      jobsExceptionFingerPrinting.jobName = getJobName(_appResult.jobExecUrl);
      jobsExceptionFingerPrinting.exceptionLog = "";
      jobsExceptionFingerPrinting.exceptionType = HadoopException.HadoopExceptionType.SPARK.toString();

      JobsExceptionFingerPrinting sparkJobException = new JobsExceptionFingerPrinting();
      sparkJobException.flowExecUrl = _appResult.flowExecUrl;
      sparkJobException.appId = _appResult.id;
      sparkJobException.taskId = NOT_APPLICABLE;
      sparkJobException.jobName = getJobName(_appResult.jobExecUrl);
      sparkJobException.logSourceInformation = processLogSourceInformation(logSourceInformation);
      //sparkJobException.exceptionLog = exceptionsTrace;
      if (exceptionsTrace.trim().length() > 2) {
        sparkJobException.exceptionLog = exceptionsTrace;
      } else {
        sparkJobException.exceptionLog = "Couldn't gather driver logs for the job";
      }
      sparkJobException.exceptionType = ExceptionInfo.ExceptionSource.DRIVER.toString();
      saveSparkJobException(sparkJobException, exceptionInfoList);
      jobsExceptionFingerPrinting.save();
    }
  }

  private void saveSparkJobException(JobsExceptionFingerPrinting sparkJobException,
      List<ExceptionInfo> exceptionInfoList) {
    try {
      sparkJobException.save();
      logger.debug(" Final String to be saved in dB " + sparkJobException.exceptionLog);
    } catch (PersistenceException pe) {
      logger.error(" Error while storing spark job exception ", pe);
      List<ExceptionInfo> halfExceptionList = exceptionInfoList.subList(0, exceptionInfoList.size() / 2);
      sparkJobException.exceptionLog = parseExceptions(halfExceptionList, false);
      try {
        sparkJobException.save();
        logger.debug(" Final String to be saved in dB " + sparkJobException.exceptionLog);
      } catch (PersistenceException pe1) {
        logger.error(" Error while storing half of the spark job exception ", pe1);
        List<ExceptionInfo> firstException = exceptionInfoList.subList(0, 0);
        sparkJobException.exceptionLog = parseExceptions(firstException, false);
        sparkJobException.save();
        logger.debug(" Final String to be saved in dB " + sparkJobException.exceptionLog);
      }
    }
  }

  /**
   *  User provided configuration is being used to set the number of
   *  exceptions in DB
   * @param exceptionInfoList : Exception List
   * @param isFirstTime : Is the list parsed first time
   * @return JSON Array as String containing exceptions
   */
  private String parseExceptions(List<ExceptionInfo> exceptionInfoList, boolean isFirstTime) {
    ObjectMapper Obj = new ObjectMapper();
    if (isFirstTime) {
      Collections.sort(exceptionInfoList);
    }
    String exceptionInJson = null;
    try {
      exceptionInJson = Obj.writeValueAsString(
          exceptionInfoList.subList(0, Math.min(exceptionInfoList.size(), NUMBER_OF_EXCEPTION_TO_PUT_IN_DB.getValue())));
    } catch (IOException e) {
      logger.error(" Exception while writing stack trace to DB ", e);
    } return exceptionInJson;
  }

  private String processLogSourceInformation(Map<String, String> logSourceInformation) {
    StringBuilder logSource = new StringBuilder();
    for (Map.Entry<String, String> entry : logSourceInformation.entrySet()) {
      logSource.append("SOURCE " + entry.getKey() + "\tURL " + entry.getValue()).append("\n");
    }
    return logSource.toString();
  }

  private String getJobName(String jobExecUrl) {
    Matcher matcher = jobNamePattern.matcher(jobExecUrl);
    if (matcher.find()) {
      return matcher.group(1);
    }
    return matcher.group(1);
  }
}
