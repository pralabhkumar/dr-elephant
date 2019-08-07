package com.linkedin.drelephant.analysis.code;

public class JobCodeInfoDataSet {
  private String jobExecutionID = null;
  private String jobName = null;
  private String flowName = null;
  private String projectName = null;
  private String fileName = null;
  private String sourceCode = null;
  private String scmType = null;
  private String repoName = null;

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

  @Override
  public String toString() {
    return "JobCodeInfoDataSet{" + "jobExecutionID='" + jobExecutionID + '\'' + ", jobName='" + jobName + '\''
        + ", flowName='" + flowName + '\'' + ", projectName='" + projectName + '\'' + ", fileName='" + fileName + '\''
        + ", scmType='" + scmType + '\'' + '}';
  }
}
