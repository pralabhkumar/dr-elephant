package com.linkedin.drelephant.analysis.code;

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

  public CodeAnalyzerException(Exception exception) {
    super(exception);
    logger.error("Unknown exception have come , catching this , to not halt the system because of unknown exception ", exception);
  }
}
