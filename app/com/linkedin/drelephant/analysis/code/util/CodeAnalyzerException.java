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


package com.linkedin.drelephant.analysis.code.util;

import java.io.IOException;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;


/**
 * This is wrapper for exceptions
 */
public class CodeAnalyzerException extends Exception {
  private static final Logger logger = Logger.getLogger(CodeAnalyzerException.class);

  public CodeAnalyzerException(IOException exception) {
    super(exception);
    logger.error("Unable to get code ", exception);
  }

  public CodeAnalyzerException(JSONException exception) {
    super(exception);
    logger.error("Unable to  parse JSON ", exception);
  }

  /**
   * Blanket exception is used , so as to avoid any side effects in the
   * Dr Elephant framework , because of unknown exception in Code level analysis.
   * @param exception
   */
  public CodeAnalyzerException(Exception exception) {
    super(exception);
    logger.error("Unknown exception have come , catching this , to not halt the system because of unknown exception ", exception);
  }
}
