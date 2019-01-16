package com.linkedin.drelephant.exceptions.spark;

public class ExceptionInfo {

  private int exceptionID;
  private String exceptionName;
  private String excptionStackTrace;

  public ExceptionInfo (int exceptionID, String exceptionName, String excptionStackTrace){
    this.exceptionID=exceptionID;
    this.exceptionName=exceptionName;
    this.excptionStackTrace=excptionStackTrace;
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
    return excptionStackTrace;
  }

  @Override
  public String toString() {
    return "ExceptionInfo{" + "exceptionID=" + exceptionID + ", exceptionName='" + exceptionName + '\''
        + ", excptionStackTrace='" + excptionStackTrace + '\'' + '}';
  }
}
