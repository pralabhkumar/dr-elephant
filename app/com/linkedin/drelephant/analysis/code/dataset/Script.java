package com.linkedin.drelephant.analysis.code.dataset;

/**
 * This is the representation of the actual script . Script consist of two things
 * 1) Code (which is executable part)
 * 2) comment  (non executable part , user level comment , optimization comment)
 */
public class Script {
  private StringBuilder optimizationComment = new StringBuilder();
  private Code code = null;
  private String inputFileName = null;
  private String inputData = null;

  public Script(String inputFileName){
    this.inputFileName = inputFileName;
  }

  public StringBuilder getOptimizationComment() {
    return optimizationComment;
  }


  public Code getCode() {
    return code;
  }

  public void setCode(Code code) {
    this.code = code;
  }

  public String getInputFileName() {
    return inputFileName;
  }

  public void setInputFileName(String inputFileName) {
    this.inputFileName = inputFileName;
  }

  public String getInputData() {
    return inputData;
  }

  public void setInputData(String inputData) {
    this.inputData = inputData;
  }



}
