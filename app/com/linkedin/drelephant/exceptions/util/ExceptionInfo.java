/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.drelephant.exceptions.util;

/**
 * Class to store exception information
 */
public class ExceptionInfo implements Comparable<ExceptionInfo> {

  private int exceptionID;
  private Integer weightOfException;
  private String exceptionName;
  private String exceptionStackTrace;
  private ExceptionSource exceptionSource;

  /**
   * Added for serialize and deserialize into JSON
   */
  public ExceptionInfo(){

  }

  @Override
  public int compareTo(ExceptionInfo o) {
    return o.weightOfException.compareTo(this.weightOfException);
  }

  public enum ExceptionSource {DRIVER, EXECUTOR, SCHEDULER}

  public ExceptionInfo(int exceptionID, String exceptionName, String exceptionStackTrace,
      ExceptionSource exceptionSource, int weightOfException) {
    this.exceptionID = exceptionID;
    this.exceptionName = exceptionName;
    this.exceptionStackTrace = exceptionStackTrace;
    this.exceptionSource = exceptionSource;
    this.weightOfException = weightOfException;
  }

  //TODO: Currently this has not been used . But the idea to have ID of two excpetion same
  //if they are simillar , so that we can remove simillar exception from the exception fingerprinting
  //system
  // Getter and setters are used by GSON/Jackson for serialize and deserialize into JSON

  public int getExceptionID() {
    return exceptionID;
  }

  public void setExceptionID(int exceptionID) {
    this.exceptionID = exceptionID;
  }

  public Integer getWeightOfException() {
    return weightOfException;
  }

  public void setWeightOfException(Integer weightOfException) {
    this.weightOfException = weightOfException;
  }

  public String getExceptionName() {
    return exceptionName;
  }

  public void setExceptionName(String exceptionName) {
    this.exceptionName = exceptionName;
  }

  public String getExceptionStackTrace() {
    return exceptionStackTrace;
  }

  public void setExceptionStackTrace(String exceptionStackTrace) {
    this.exceptionStackTrace = exceptionStackTrace;
  }

  public ExceptionSource getExceptionSource() {
    return exceptionSource;
  }

  public void setExceptionSource(ExceptionSource exceptionSource) {
    this.exceptionSource = exceptionSource;
  }

  //TODO : Use exception source to prioritize  the exception

  @Override
  public String toString() {
    return "ExceptionInfo{" + "exceptionID=" + exceptionID + ", weightOfException=" + weightOfException
        + ", exceptionName='" + exceptionName + '\'' + ", exceptionStackTrace='" + exceptionStackTrace + '\''
        + ", exceptionSource=" + exceptionSource + '}';
  }

  /**
   * todo: Remove duplicate exceptions
   * @param
   * @return
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ExceptionInfo that = (ExceptionInfo) o;

    if (!exceptionName.equals(that.exceptionName)) {
      return false;
    }
    return exceptionStackTrace.equals(that.exceptionStackTrace);
  }

  @Override
  public int hashCode() {
    int result = exceptionName.hashCode();
    result = 31 * result + exceptionStackTrace.hashCode();
    return result;
  }
}
