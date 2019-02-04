package com.linkedin.drelephant.exceptions.util;

/**
 * Class to store exception information
 */
public class ExceptionInfo {

  private int exceptionID;
  private String exceptionName;
  private String exceptionStackTrace;
  private String exceptionSource;
  public enum ExceptionSource {DRIVER,EXECUTOR,SCHEDULER}

  public ExceptionInfo(int exceptionID, String exceptionName, String exceptionStackTrace, ExceptionSource exceptionSource) {
    this.exceptionID = exceptionID;
    this.exceptionName = exceptionName;
    this.exceptionStackTrace = exceptionStackTrace;
    this.exceptionSource = exceptionSource.name();
  }

  /**
   * ToDo:
   * Currently this has not been used . But the idea to have ID of two excpetion same
   * if they are simillar , so that we can remove simillar exception from the exception fingerprinting
   * system
   * @return
   */
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

  //ToDo : Use exception source to prioritize  the exception
  //
  public String getExceptionSource() {
    return exceptionSource;
  }

  @Override
  public String toString() {
    return "ExceptionInfo{" + "exceptionID=" + exceptionID + ", exceptionName='" + exceptionName + '\''
        + ", excptionStackTrace='" + exceptionStackTrace + '\'' + ", exceptionSource='" + exceptionSource + '\'' + '}';
  }
}
