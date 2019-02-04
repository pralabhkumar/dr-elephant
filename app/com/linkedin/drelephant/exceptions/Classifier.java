package com.linkedin.drelephant.exceptions;

import com.linkedin.drelephant.exceptions.util.ExceptionInfo;
import java.util.List;

import static com.linkedin.drelephant.exceptions.util.Constant.*;


/**
 * Every classifier should implement this interface to
 * create own classifier . Current implementation is Rule
 * Base classifier , another one could be ML based classified
 */
public interface Classifier {

  void preProcessingData(List<com.linkedin.drelephant.exceptions.util.ExceptionInfo> exceptions);

  /**
   *
   * @param exceptions : List of exceptions provided as input to this method
   * @return : It will return the class in which , all exceptions are classified.
   */
  LogClass classify(List<ExceptionInfo> exceptions);
}
