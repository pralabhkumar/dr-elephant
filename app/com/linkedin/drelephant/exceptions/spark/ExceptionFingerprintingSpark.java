package com.linkedin.drelephant.exceptions.spark;

import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.analysis.AnalyticJob;
import com.linkedin.drelephant.spark.fetchers.statusapiv1.StageData;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import models.JobExecution;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;

import static com.linkedin.drelephant.exceptions.spark.ExceptionInfo.*;
import static com.linkedin.drelephant.exceptions.spark.Constant.*;
import static com.linkedin.drelephant.exceptions.spark.ExceptionUtils.*;


public class ExceptionFingerprintingSpark implements ExceptionFingerprinting {

  private static final Logger logger = Logger.getLogger(ExceptionFingerprintingSpark.class);
  boolean debugEnabled = logger.isDebugEnabled();
  private static final String JOBHISTORY_WEBAPP_ADDRESS = "mapreduce.jobhistory.webapp.address";
  private static final String NODEMANAGER_ADDRESS = "yarn.nodemanager.address";
  private static final int STARTING_INDEX_FOR_URL = 2;
  private static final String CONTAINER_PATTERN = "container";
  private static final String HOSTNAME_PATTERN = ":";
  private static final String URL_PATTERN = "/";
  private static final String JOBHISTORY_ADDRESS_FOR_LOG = "http://{0}/jobhistory/nmlogs/{1}";
  private static final int NUMBER_OF_STACKTRACE_LINE = 3;
  private static final int TIME_OUT = 150000;

  private enum LogLengthSchema {LOG_LENGTH, LENGTH}

  private AnalyticJob analyticJob;
  private List<StageData> failedStageData;
  private boolean useRestAPI = true;

  public ExceptionFingerprintingSpark(List<StageData> failedStageData) {
    this.failedStageData = failedStageData;
    if (failedStageData == null) {
      logger.debug(" No data fetched for stages ");
    }
  }

  /*
   *
   * @return If use rest API to get
   * TODO : Code for reading the logs from HDFS and then provide option to user for either reading from HDFS or rest API
   */
  public boolean isUseRestAPI() {
    return useRestAPI;
  }

  public void setUseRestAPI(boolean useRestAPI) {
    this.useRestAPI = useRestAPI;
  }

  @Override
  public List<ExceptionInfo> processRawData(AnalyticJob analyticJob) {
    this.analyticJob = analyticJob;
    List<ExceptionInfo> exceptions = new ArrayList<ExceptionInfo>();

    processStageLogs(exceptions);
    //ToDo : If there are enough information from stage log then we should not call
    //ToDo : driver logs ,to optimize the process .But it can lead to false positivies  in the system,
    //ToDo : since failure of stage may or may not be the reason for application failure
    processDriverLogs(exceptions);
    return exceptions;
  }

  /**
   * process stage logs
   * @param exceptions
   */
  private void processStageLogs(List<ExceptionInfo> exceptions) {
    long startTime = System.nanoTime();
    try {
      if (failedStageData != null && failedStageData.size() > 0) {
        for (StageData stageData : failedStageData) {
          /**
           * Currently there is no use of exception unique ID . But in future it can be used to
           * find out simillar exceptions . We might need to change from hashcode to some other id
           * which will give same id , if they similar exceptions .
           */
          addExceptions((stageData.failureReason().get() + "" + stageData.details()).hashCode(),
              stageData.failureReason().get(), stageData.details(), ExceptionSource.EXECUTOR, exceptions);
        }
      } else {
        logger.info(" There is not failed stage data ");
      }
    } catch (Exception e) {
      logger.error("Error process stages logs ", e);
    }
    long endTime = System.nanoTime();
    logger.info(" Total exception/error parsed so far - stage " + exceptions.size());
    logger.info(" Time taken for processing stage logs " + (endTime - startTime) * 1.0 / (1000000000.0) + "s");
  }

  private void addExceptions(int uniqueID, String exceptionName, String exceptionStackTrace,
      ExceptionSource exceptionSource, List<ExceptionInfo> exceptions) {
    ExceptionInfo exceptionInfo = new ExceptionInfo(uniqueID, exceptionName, exceptionStackTrace, exceptionSource);
    if (debugEnabled) {
      logger.info(" Exception Information " + exceptionInfo);
    }
    exceptions.add(exceptionInfo);
  }

  /**
   * process driver / Application master logs for exceptions
   * //ToDo: In case of unable to get log length there are
   * //ToDo: multiple calls to same URL (we can optimize this)
   * //Note : Above case should not happen unless there are some changes
   * // in JHS APIs .
   * @param exceptions
   */
  private void processDriverLogs(List<ExceptionInfo> exceptions) {
    long startTime = System.nanoTime();
    HttpURLConnection connection = null;
    BufferedReader in = null;
    try {
      String urlToQuery = buildURLtoQuery();
      long startTimeForFirstQuery = System.nanoTime();
      String completeURLToQuery = completeURLToQuery(urlToQuery);
      long endTimeForFirstQuery = System.nanoTime();
      logger.info(
          " Time taken for first query " + (endTimeForFirstQuery - startTimeForFirstQuery) * 1.0 / (1000000000.0)
              + "s");
      logger.error(" URL to query for driver logs  " + completeURLToQuery);
      URL amAddress = new URL(completeURLToQuery);
      connection = (HttpURLConnection) amAddress.openConnection();
      connection.setConnectTimeout(TIME_OUT);
      connection.setReadTimeout(TIME_OUT);
      connection.connect();
      in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
      String inputLine;
      String logLengthTrigger = LogLengthSchema.LOG_LENGTH.name().replace("_", " ");
      try {
        while ((inputLine = in.readLine()) != null) {
          if (inputLine.toUpperCase().contains(logLengthTrigger)) {
            logger.debug(" Processing of logs ");
            driverLogProcessingForException(in, exceptions);
            break;
          }
        }
      } catch (IOException e) {
        logger.error(" IO Exception while processing driver logs for ", e);
      }
      gracefullyCloseConnection(in, connection);
    } catch (Exception e) {
      logger.info(" Exception processing  driver logs ", e);
      gracefullyCloseConnection(in, connection);
    }
    long endTime = System.nanoTime();
    logger.info(" Total exception/error parsed so far - driver " + exceptions.size());
    logger.info(" Time taken for driver logs " + (endTime - startTime) * 1.0 / (1000000000.0) + "s");
  }

  /**
   * If unable to get log length , then read by default last 4096 bytes
   * If log length is greater than threshold then read last 20 percentage of logs
   * If log length is less than threshold then real complete logs .
   * @param url
   * @return
   */
  private String completeURLToQuery(String url) {
    String completeURLToQuery = null;
    BufferedReader in = null;
    HttpURLConnection connection = null;
    try {
      URL amAddress = new URL(url);
      connection = (HttpURLConnection) amAddress.openConnection();
      connection.setConnectTimeout(TIME_OUT);
      connection.setReadTimeout(TIME_OUT);
      connection.connect();
      in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
      String inputLine;
      long logLength = 0l;
      String logLengthTrigger = LogLengthSchema.LOG_LENGTH.name().replace("_", " ");
      while ((inputLine = in.readLine()) != null) {
        if (inputLine.toUpperCase().contains(logLengthTrigger)) {
          logLength = getLogLength(inputLine.toUpperCase());
          if (logLength == 0) {
            completeURLToQuery = url;
          } else if (logLength <= FIRST_THRESHOLD_LOG_LENGTH_IN_BYTES) {
            completeURLToQuery = url + "/stderr/?start="+MINIMUM_LOG_LENGTH_IN_BYTES_TO_SKIP;
          } else if (logLength<=LAST_THRESHOLD_LOG_LENGTH_IN_BYTES) {
            long startIndex = (long) Math.floor(logLength * THRESHOLD_PERCENTAGE_OF_LOG_TO_READ_FROM_LAST);
            completeURLToQuery = url+ "/stderr/?start=" + startIndex;
          }
          else {
            completeURLToQuery = url+ "/stderr/?start=" + (logLength-THRESHOLD_LOG_INDEX_FROM_END_IN_BYTES);
          }
          break;
        }
      } gracefullyCloseConnection(in, connection);
    } catch (Exception e) {
      logger.error(" Exception while creating complete URL to query ", e);
      gracefullyCloseConnection(in, connection);
      return url;
    } return completeURLToQuery;
  }

  private long getLogLength(String logLenghtLine) {
    long logLength = 0;
    try {
      String split[] = logLenghtLine.split(HOSTNAME_PATTERN);
      if (split.length >= LogLengthSchema.LENGTH.ordinal()) {
        logLength = Long.parseLong(split[LogLengthSchema.LENGTH.ordinal()].trim());
      }
    } catch (Exception e) {
      logger.error(" Exception parsing log length ", e);
    }
    logger.info(" log length in bytes " + logLength);
    return logLength;
  }

  private void gracefullyCloseConnection(BufferedReader in, HttpURLConnection connection) {
    try {
      if (in != null) {
        in.close();
      }
      if (connection != null) {
        connection.disconnect();
      }
    } catch (Exception e1) {
      logger.error(" Exception while closing the connections ", e1);
    }
  }

  /**
   *
   * @return Build the JHS URL to query it and get driver/AM logs
   * @throws Exception
   */
  public String buildURLtoQuery() throws Exception {
    Configuration configuration = ElephantContext.instance().getGeneralConf();
    String jobHistoryAddress = configuration.get(JOBHISTORY_WEBAPP_ADDRESS);
    String nodeManagerPort = configuration.get(NODEMANAGER_ADDRESS).split(HOSTNAME_PATTERN)[1];
    String amHostHTTPAddress = parseURL(analyticJob.getAmContainerLogsURL(), nodeManagerPort);
    String completeURLToQuery = MessageFormat.format(JOBHISTORY_ADDRESS_FOR_LOG, jobHistoryAddress, amHostHTTPAddress);
    logger.info(" Query this url for error details " + completeURLToQuery);
    return completeURLToQuery;
  }

  private String parseURL(String url, String nodeManagerPort) throws Exception {
    String amAddress = null, containerID = null, userName = null;
    String data[] = url.split(URL_PATTERN);
    for (int idx = STARTING_INDEX_FOR_URL; idx < data.length; idx++) {
      if (data[idx].contains(HOSTNAME_PATTERN)) {
        amAddress = data[idx].split(HOSTNAME_PATTERN)[0] + HOSTNAME_PATTERN + nodeManagerPort;
      } else if (data[idx].toLowerCase().contains(CONTAINER_PATTERN)) {
        containerID = data[idx];
      } else if (idx == data.length - 1) {
        userName = data[idx];
      }
    }
    return amAddress + URL_PATTERN + containerID + URL_PATTERN + containerID + URL_PATTERN + userName;
  }

  private void driverLogProcessingForException(BufferedReader in, List<ExceptionInfo> exceptions) throws IOException {
    String inputLine;
    while ((inputLine = in.readLine()) != null) {
      if (isExceptionContains(inputLine)) {
        if (debugEnabled) {
          logger.debug(" ExceptionFingerprinting " + inputLine);
        }
        String exceptionName = inputLine;
        int stackTraceLine = NUMBER_OF_STACKTRACE_LINE;
        StringBuffer stackTrace = new StringBuffer();
        while (stackTraceLine >= 0 && inputLine != null) {
          stackTrace.append(inputLine);
          stackTraceLine--;
          inputLine = in.readLine();
        }
        addExceptions((exceptionName + "" + stackTrace).hashCode(), exceptionName, stackTrace.toString(),
            ExceptionInfo.ExceptionSource.DRIVER, exceptions);
        if (inputLine == null) {
          break;
        }
      }
    }
  }

  @Override
  public LogClass classifyException(List<ExceptionInfo> exceptionInformation) {
    if (exceptionInformation != null && exceptionInformation.size() > 0) {
      Classifier classifier = ClassifierFactory.getClassifier(ClassifierTypes.RULE_BASE_CLASSIFIER);
      classifier.preProcessingData(exceptionInformation);
      LogClass logClass = classifier.classify(exceptionInformation);
      return logClass;
    }
    return null;
  }

  @Override
  public boolean saveData(String jobExecId) throws Exception {
    JobExecution jobExecution = JobExecution.find.where().eq(JobExecution.TABLE.jobExecId, jobExecId).findUnique();
    if (jobExecution == null) {
      logger.error(" Job Execution with following id doesn't exist " + jobExecId);
      throw new Exception("Job execution with " + jobExecId + " doesn't exist");
    } else {
      logger.error(" Job Execution is not null " + jobExecution);
      jobExecution.autoTuningFault = true;
      jobExecution.update();
    }
    return true;
  }
}
