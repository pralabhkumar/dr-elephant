package com.linkedin.drelephant.analysis.code.util;

/**
 * This class will contains all the constants require by Code Analysis
 * which are shared across classes.
 */

public final class Constant {
  static final String BASE_URL_FOR_EXTRACTING_CODE_NAME = "ca.url.extract.code";
  static final String QUEUE_NAMES_VALID_FOR_CODE_NAME_EXTRACTION_NAME = "ca.queue.name.valid.code.extraction";

  public enum CodeLocationJSONKey {
    PATH("paths"),
    SCM("scm"),
    FILE_PATH("filePath"),
    REPONAME("repoName"),
    FILE_INFO("fileInfo"),
    SOURCE_CODE("sourceCode");
    private final String keyName;

    CodeLocationJSONKey(String keyName) {
      this.keyName = keyName;
    }

    public String getJSONKey() {
      return keyName;
    }
  }
}
