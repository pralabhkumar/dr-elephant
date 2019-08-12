package com.linkedin.drelephant.analysis.code.dataset;

import com.linkedin.drelephant.analysis.code.CodeOptimizer;
import java.util.HashMap;
import java.util.Map;


public class JobCodeInfoDataSet {
  private String jobExecutionID = null;
  private String jobName = null;
  private String flowName = null;
  private String projectName = null;
  private String fileName = null;
  private String sourceCode = null;
  private String scmType = null;
  private String repoName = null;
  private CodeOptimizer _codeOptimizer = null;
  private Map<String,String> metaData = null;

  public JobCodeInfoDataSet(){
    this.metaData = new HashMap<String,String>();
  }

  public Map<String,String> getMetaData(){
    return  this.metaData;
  }
  public String getRepoName() {
    return repoName;
  }

  public void setRepoName(String repoName) {
    this.repoName = repoName;
  }

  public String getScmType() {
    return scmType;
  }

  public void setScmType(String scmType) {
    this.scmType = scmType;
  }

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public String getFlowName() {
    return flowName;
  }

  public void setFlowName(String flowName) {
    this.flowName = flowName;
  }

  public String getProjectName() {
    return projectName;
  }

  public void setProjectName(String projectName) {
    this.projectName = projectName;
  }

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName.replaceAll("src/","");
  }

  public String getSourceCode() {
    return sourceCode;
  }

  public void setSourceCode(String sourceCode) {
    this.sourceCode = sourceCode;
  }

  public String getJobExecutionID() {
    return jobExecutionID;
  }

  public void setJobExecutionID(String jobExecutionID) {
    this.jobExecutionID = jobExecutionID;
  }

  public CodeOptimizer getCodeOptimizer() {
    return _codeOptimizer;
  }

  public void setCodeOptimizer(CodeOptimizer codeOptimizer) {
    _codeOptimizer = codeOptimizer;
  }

  @Override
  public String toString() {
    return "JobCodeInfoDataSet{" + "jobExecutionID='" + jobExecutionID + '\'' + ", jobName='" + jobName + '\''
        + ", flowName='" + flowName + '\'' + ", projectName='" + projectName + '\'' + ", fileName='" + fileName + '\''
        + ", scmType='" + scmType + '\'' + ", repoName='" + repoName + '\'' + ", _codeOptimizer=" + _codeOptimizer
        + ", metaData=" + metaData + '}';
  }
}
