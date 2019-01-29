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


public class ExceptionFingerprintingSpark implements ExceptionFingerprinting {

  private static final Logger logger = Logger.getLogger(ExceptionFingerprintingSpark.class);
  boolean debugEnabled = logger.isDebugEnabled();
  private static final String JOBHISTORY_WEBAPP_ADDRESS = "mapreduce.jobhistory.webapp.address";
  private static final String NODEMANAGER_ADDRESS = "yarn.nodemanager.address";
  private static final int STARTING_INDEX_FOR_URL = 2;
  private static final String CONTAINER_PATTERN = "container";
  private static final String HOSTNAME_PATTERN = ":";
  private static final String URL_PATTERN = "/";
  private static final String JOBHISTORY_ADDRESS_FOR_LOG = "http://{0}/jobhistory/nmlogs/{1}/stderr/?start=0";
  private static final int NUMBER_OF_STACKTRACE_LINE = 3;
  private static final int TIME_OUT = 150000;
  private static final String REGEX_FOR_EXCEPTION = "^.+[Exception|Error][^\\n]+";
  private static final Pattern PATTERN_FOR_EXCEPTION = Pattern.compile(REGEX_FOR_EXCEPTION, Pattern.CASE_INSENSITIVE);
  private static final String PATTERN_FOR_DRIVER_LOG_PROCESSING = "<td class=\"content\">";
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
      if (failedStageData != null) {
        for (StageData stageData : failedStageData) {
          /**
           * Currently there is no use of exception unique ID . But in future it can be used to
           * find out simillar exceptions . We might need to change from hashcode to some other id
           * which will give same id , if they similar exceptions .
           */
          addExceptions((stageData.failureReason().get() + "" + stageData.details()).hashCode(),
              stageData.failureReason().get(), stageData.details(), ExceptionSource.EXECUTOR, exceptions);
        }
      }
    } catch (Exception e) {
      logger.error("Error process stages logs ", e);
    }
    long endTime = System.nanoTime();
    logger.info(" Time taken for processing stage logs " + (startTime - endTime));
  }

  private void addExceptions(int uniqueID, String exceptionName, String exceptionStackTrace,
      ExceptionSource exceptionSource, List<ExceptionInfo> exceptions) {
    ExceptionInfo exceptionInfo = new ExceptionInfo(uniqueID, exceptionName, exceptionStackTrace, exceptionSource);
    logger.info(" Exception Information " + exceptionInfo);
    exceptions.add(exceptionInfo);
  }

  /**
   * process driver / Application master logs for exceptions
   * @param exceptions
   */
  private void processDriverLogs(List<ExceptionInfo> exceptions) {
    long startTime = System.nanoTime();
    try {
      URL amAddress = new URL(buildURLtoQuery());
      HttpURLConnection connection = (HttpURLConnection) amAddress.openConnection();
      connection.setConnectTimeout(TIME_OUT);
      connection.setReadTimeout(TIME_OUT);
      connection.connect();
      BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
      String inputLine;
      try {
        while ((inputLine = in.readLine()) != null) {
          if (inputLine.contains(PATTERN_FOR_DRIVER_LOG_PROCESSING)) {
            logger.debug(" Processing of logs ");
            driverLogProcessingForException(in, exceptions);
            break;
          }
        }
      } catch (IOException e) {
        logger.error(" IO Exception while processing driver logs for ", e);
      }
      in.close();
    } catch (Exception e) {
      logger.info(" Exception processing  driver logs ", e);
    }
    long endTime = System.nanoTime();
    logger.info(" Time taken for driver logs " + (startTime - endTime));
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

  private boolean isExceptionContains(String data) {
    Matcher m = PATTERN_FOR_EXCEPTION.matcher(data);
    return m.matches();
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
