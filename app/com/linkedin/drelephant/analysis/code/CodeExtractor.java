package com.linkedin.drelephant.analysis.code;

import com.linkedin.drelephant.analysis.code.dataset.JobCodeInfoDataSet;
import com.linkedin.drelephant.analysis.code.util.CodeAnalyzerException;
import java.io.IOException;
import java.net.MalformedURLException;
import models.AppResult;
import org.codehaus.jettison.json.JSONException;


public interface CodeExtractor {

  /**
   * Check if prerequistes are matched to extract the codee
   * @param appResult : Information about the application executed on cluster
   * @return true or false
   */
  boolean arePrerequisiteMatched(AppResult appResult);

  /**
   *
   * @param appResult : Information about the application executed on cluster
   * @return : Code File Name
   * @throws CodeAnalyzerException : Throws custom exception
   */
  String getCodeFileName(AppResult appResult) throws CodeAnalyzerException;

  /**
   *
   * @param appResult: Information about the application executed on cluster
   * @return : DataSet which contains source code and all the relevant information about code
   * @throws CodeAnalyzerException : Throws custom exception
   */
  JobCodeInfoDataSet execute(AppResult appResult) throws CodeAnalyzerException;

  /**
   *
   * @param codeInformation
   * @throws IOException
   * @throws JSONException
   */
  void processCodeLocationInformation(String codeInformation) throws IOException, JSONException;
}