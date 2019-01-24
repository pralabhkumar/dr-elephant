package com.linkedin.drelephant.exceptions.spark;

public class ExceptionInfo {

  private int exceptionID;
  private String exceptionName;
  private String exceptionStackTrace;
  private String exceptionSource;
  enum ExceptionSource {DRIVER,EXECUTOR,SCHEDULER}

  public ExceptionInfo(int exceptionID, String exceptionName, String exceptionStackTrace, ExceptionSource exceptionSource) {
    this.exceptionID = exceptionID;
    this.exceptionName = exceptionName;
    this.exceptionStackTrace = exceptionStackTrace;
    this.exceptionSource = exceptionSource.name();
  }

  public int getExceptionID() {
    return exceptionID;
  }

  public void setExceptionID(int exceptionID) {
    this.exceptionID = exceptionID;
  }

  public String getExceptionName() {
    return exceptionName;
  }

  public String getExcptionStackTrace() {
    return exceptionStackTrace;
  }

  public String getExceptionSource() {
    return exceptionSource;
  }

  @Override
  public String toString() {
    return "ExceptionInfo{" + "exceptionID=" + exceptionID + ", exceptionName='" + exceptionName + '\''
        + ", excptionStackTrace='" + exceptionStackTrace + '\'' + ", exceptionSource='" + exceptionSource + '\'' + '}';
  }
}
