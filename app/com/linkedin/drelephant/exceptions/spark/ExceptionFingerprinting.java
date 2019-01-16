package com.linkedin.drelephant.exceptions.spark;

import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.analysis.AnalyticJob;
import com.linkedin.drelephant.spark.data.SparkApplicationData;
import com.linkedin.drelephant.spark.fetchers.statusapiv1.StageData;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;


public class ExceptionFingerprinting implements Runnable {

  private static final Logger logger = Logger.getLogger(ExceptionFingerprinting.class);
  private static final String JOBHISTORY_WEBAPP_ADDRESS = "mapreduce.jobhistory.webapp.address";
  private static final String NODEMANAGER_ADDRESS = "yarn.nodemanager.address";
  private static final int STARTING_INDEX_FOR_URL = 2;
  private static final String CONTAINER_PATTERN = "container";
  private static final String HOSTNAME_PATTERN = ":";
  private static final String URL_PATTERN = "/";
  private static final String JOBHISTORY_ADDRESS_FOR_LOG = "http://{0}/jobhistory/nmlogs/{1}/stderr/?start=0";
  private static final int NUMBER_OF_STACKTRACE_LINE = 3;
  private AnalyticJob analyticJob;
  private List<StageData> failedStageData;
  private boolean useRestAPI = true;
  private List<ExceptionInfo> exceptions;

  public ExceptionFingerprinting(AnalyticJob analyticJob, List<StageData> failedStageData) {
    this.analyticJob = analyticJob;
    this.failedStageData = failedStageData;
    if(failedStageData == null) {
      logger.debug(" No data fetched ");
    }
    exceptions = new ArrayList<ExceptionInfo>();
  }

  /*
   *
   * @return If use rest API to get
   */
  public boolean isUseRestAPI() {
    return useRestAPI;
  }

  public void setUseRestAPI(boolean useRestAPI) {
    this.useRestAPI = useRestAPI;
  }

  @Override
  public void run() {
    try {
      if (failedStageData != null) {
        processStageLogsForFailure();
      }
      processDriverLogsForFailure();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void processStageLogsForFailure() {
    for (StageData stageData : failedStageData) {
      if (stageData.failureReason().get().contains("java.lang.OutOfMemoryError") || stageData.details()
          .contains("java.lang.OutOfMemoryError")) {
        logger.info(" Auto Tuning Induced error ");
      }
    }
  }

  private void processDriverLogsForFailure() throws Exception {
    URL amAddress = new URL(buildURLtoQuery());
    HttpURLConnection connection = (HttpURLConnection) amAddress.openConnection();
    connection.setConnectTimeout(150000);
    connection.setInstanceFollowRedirects(true);
    connection.setReadTimeout(150000);
    connection.connect();
    BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
    String inputLine;
    while ((inputLine = in.readLine()) != null) {
      if (inputLine.contains("<td class=\"content\">")) {
        logger.info( " Processing of logs ");
        processingOfLogs(in);
        break;
      }
    }
    in.close();
  }

  /*if (inputLine.contains("java.lang.OutOfMemoryError")) {
      logger.info(" Auto Tuning Induced error ");
    }*/
  private void processingOfLogs(BufferedReader in) throws IOException {
    String inputLine;
    while ((inputLine = in.readLine()) != null) {
      if (isExceptionContains(inputLine)) {
        logger.info(" ExceptionFingerprinting "+inputLine);
        String exceptionName = inputLine;
        int stackTraceLine = NUMBER_OF_STACKTRACE_LINE;
        StringBuffer stackTrace = new StringBuffer();
        while (stackTraceLine >= 0 && inputLine != null) {
          stackTrace.append(inputLine);
          stackTraceLine--;
          inputLine = in.readLine();
        }
        ExceptionInfo exceptionInfo =
            new ExceptionInfo((exceptionName + "" + stackTrace).hashCode(), exceptionName, stackTrace.toString());
        logger.info(" Exception Information "+exceptionInfo);
        exceptions.add(exceptionInfo);
        if (inputLine == null) {
          break;
        }
      }
    }
  }

  private boolean isExceptionContains(String data) {
    Pattern pattern = Pattern.compile("^.+Exception[^\\n]+", Pattern.CASE_INSENSITIVE);
    Matcher m = pattern.matcher(data);
    return m.matches();
  }

  private String buildURLtoQuery() throws Exception {
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
}
