package com.linkedin.drelephant.analysis.code.impl;

import com.linkedin.drelephant.analysis.code.CodeAnalyzerException;
import com.linkedin.drelephant.analysis.code.CodeExtractor;
import com.linkedin.drelephant.analysis.code.JobCodeInfoDataSet;
import com.linkedin.drelephant.clients.azkaban.AzkabanJobStatusUtil;
import com.linkedin.drelephant.clients.azkaban.AzkabanWorkflowClient;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import models.AppResult;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;


public class AzkabanJarvisCodeExtractor implements CodeExtractor {
  private static final Logger logger = Logger.getLogger(AzkabanJarvisCodeExtractor.class);
  private static AzkabanJobStatusUtil azkabanJobStatusUtil = null;
  private static final String JARVIS_BASE_URL = "http://jarvis.corp.linkedin.com/codesearch/api/v1/";
  private JobCodeInfoDataSet _jobCodeInfoDataSet = null;

  private enum ValidQueueName {UMP}

  static {
    azkabanJobStatusUtil = new AzkabanJobStatusUtil();
    logger.info(" Intialized Azkaban Job status Util to query azkaban for Code analysis ");
  }

  @Override
  public JobCodeInfoDataSet execute(AppResult appResult) throws CodeAnalyzerException {
    try {
      if (isCodeNameExtractible(appResult)) {
        logger.info(" Extracting source code file name of the  following job " + appResult.jobDefId);
        String codeFileName = codeFileNameExtractor(appResult);
        if (codeFileName != null) {
          logger.info(" Code file name is " + codeFileName);
          if (CodeOptimizerFactory.getCodeOptimizer(codeFileName) != null) {
            logger.info("Optimizier is avaiable for following code " + codeFileName + " hence getting complete code ");
            _jobCodeInfoDataSet = codeInfoExtractor(codeFileName);
            logger.info(" Successfully extracted code for following job " + appResult.jobDefId);
          }
        } else {
          logger.info("Unable to extract code name " + appResult.jobDefId);
        }
      } else {
        logger.info(" Job code is not extractible as it have insufficient information " + appResult.jobDefId);
      }
    } catch (IOException | JSONException e) {
      throw new CodeAnalyzerException(e);
    } catch (Exception e) {
      throw new CodeAnalyzerException(e);
    }
    return _jobCodeInfoDataSet;
  }

  @Override
  public boolean isCodeNameExtractible(AppResult appResult) {
    if (appResult.queueName.toUpperCase().contains(ValidQueueName.UMP.name())) {
      logger.debug(" Job is from the valid project/queue and script name can be extracted ");
      return true;
    } else {
      return false;
    }
  }

  @Override
  public String codeFileNameExtractor(AppResult appResult) throws MalformedURLException {
    String modifiedURL = appResult.jobDefId.replaceAll("&job=", "&jobName=").replaceAll("&flow=", "&flowName=");
    logger.info("Query following url to azkaban " + modifiedURL);
    AzkabanWorkflowClient azkabanWorkflowClient = azkabanJobStatusUtil.getWorkflowClient(appResult.flowExecId);
    return azkabanWorkflowClient.getCodePathfromJob(modifiedURL);
  }

  @Override
  public JobCodeInfoDataSet codeInfoExtractor(String codeFileName) throws IOException, JSONException {
    JobCodeInfoDataSet jobLocationInfo = null;
    jobLocationInfo = getCodeLocationInformation(codeFileName);
    String sourceCode = getSourceCode(jobLocationInfo);
    logger.debug(" Following source code " + sourceCode);
    jobLocationInfo.setSourceCode(sourceCode);
    return jobLocationInfo;
  }

  private JobCodeInfoDataSet getCodeLocationInformation(String codeFileName) throws IOException, JSONException {
    JobCodeInfoDataSet jobCodeInfo = new JobCodeInfoDataSet();
    String urlToGetJobLocation = JARVIS_BASE_URL + "filepaths?query=" + codeFileName.replaceAll("src/", "");
    logger.info(" Final URL to get Job Location " + urlToGetJobLocation);
    JSONObject jsonJobInfo = parseURL(urlToGetJobLocation);
    JSONArray paths = jsonJobInfo.getJSONArray("paths");
    JSONObject information = paths.getJSONObject(0);
    jobCodeInfo.setFileName(URLEncoder.encode(information.getString("filePath"), "UTF-8"));
    jobCodeInfo.setScmType(URLEncoder.encode(information.getString("scm"), "UTF-8"));
    jobCodeInfo.setRepoName(URLEncoder.encode(information.getString("repoName"), "UTF-8"));
    return jobCodeInfo;
  }

  private String getSourceCode(JobCodeInfoDataSet jobCodeInfo) throws IOException, JSONException {
    String urlToGetSourceCode =
        JARVIS_BASE_URL + "file/" + jobCodeInfo.getScmType() + "/" + jobCodeInfo.getRepoName() + "/"
            + jobCodeInfo.getFileName();
    logger.info(" Final URL to get source code " + urlToGetSourceCode);
    JSONObject jsonJobInfo = parseURL(urlToGetSourceCode);
    return jsonJobInfo.getJSONObject("fileInfo").getString("sourceCode");
  }

  private JSONObject parseURL(String urlName) throws IOException, JSONException {
    URL url = new URL(urlName);
    BufferedReader br = new BufferedReader(new InputStreamReader(url.openStream()));
    String data = "";
    StringBuilder sb = new StringBuilder();
    while ((data = br.readLine()) != null) {
      sb.append(data);
    }
    return new JSONObject(sb.toString());
  }
}
