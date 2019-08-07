package com.linkedin.drelephant.analysis.code;

import java.io.IOException;
import java.net.MalformedURLException;
import models.AppResult;
import org.codehaus.jettison.json.JSONException;


public interface CodeExtractor {
  boolean isCodeNameExtractible(AppResult appResult);

  String codeFileNameExtractor(AppResult appResult) throws MalformedURLException;

  JobCodeInfoDataSet codeInfoExtractor(String codeFileName) throws IOException, JSONException;

  JobCodeInfoDataSet execute(AppResult appResult) throws CodeAnalyzerException;
}