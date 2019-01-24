package com.linkedin.drelephant.exceptions.spark;

import java.util.List;


public interface Classifier {
  enum LogClass {USER_ENABLED, AUTOTUINING_ENABLED}
  LogClass classify(List<ExceptionInfo> exceptions);
}
