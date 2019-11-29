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
import lombok.Data;
/**
 * Class to store exception information
 */
@Data
public class ExceptionInfo implements Comparable<ExceptionInfo> {

  private int exceptionID;
  private Integer weightOfException;
  private String exceptionName;
  private String exceptionStackTrace;
  private ExceptionSource exceptionSource;

  private String exceptionTrackingURL;

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
      ExceptionSource exceptionSource, int weightOfException, String exceptionTrackingURL) {
    this.exceptionID = exceptionID;
    this.exceptionName = exceptionName;
    this.exceptionStackTrace = exceptionStackTrace;
    this.exceptionSource = exceptionSource;
    this.weightOfException = weightOfException;
    this.exceptionTrackingURL = exceptionTrackingURL;
  }

  //TODO: Currently this has not been used . But the idea to have ID of two excpetion same
  //if they are simillar , so that we can remove simillar exception from the exception fingerprinting
  //system
  // Getter and setters are used by GSON/Jackson for serialize and deserialize into JSON


  //TODO : Use exception source to prioritize  the exception

  @Override
  public String toString() {
    return "ExceptionInfo{" + "exceptionID=" + exceptionID + ", weightOfException=" + weightOfException
        + ", exceptionName='" + exceptionName + '\'' + ", exceptionStackTrace='" + exceptionStackTrace + '\''
        + ", exceptionSource=" + exceptionSource + ", exceptionTrackingURL='" + exceptionTrackingURL + '\'' + '}';
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

  public String getExceptionTrackingURL() {
    return exceptionTrackingURL;
  }

  public void setExceptionTrackingURL(String exceptionTrackingURL) {
    this.exceptionTrackingURL = exceptionTrackingURL;
  }

  @Override
  public int hashCode() {
    int result = exceptionName.hashCode();
    result = 31 * result + exceptionStackTrace.hashCode();
    return result;
  }
}
