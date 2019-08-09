package com.linkedin.drelephant.analysis.code.util;

import org.apache.hadoop.conf.Configuration;

import static com.linkedin.drelephant.analysis.code.util.Constant.*;
import org.apache.log4j.Logger;

/**
 * This class is Helper class , contains Helper methods or
 * classes which are required to Build Configuration
 */

public class Helper {
  private static final Logger logger = Logger.getLogger(Helper.class);
  private static boolean debugEnabled = logger.isDebugEnabled();

  /**
   * This class used to create configuration required for code level analysis.
   */
  public static class ConfigurationBuilder {
    public static CodeAnalysisConfiguration<String> BASE_URL_FOR_EXTRACTING_CODE = null;
    public static CodeAnalysisConfiguration<String[]> QUEUE_NAMES_VALID_FOR_CODE_NAME_EXTRACTION = null;

    public static void buildConfigurations(Configuration configuration) {
      BASE_URL_FOR_EXTRACTING_CODE =
          new CodeAnalysisConfiguration<String>().setConfigurationName(BASE_URL_FOR_EXTRACTING_CODE_NAME)
              .setValue(configuration.get(BASE_URL_FOR_EXTRACTING_CODE_NAME,"http://abcd/efgh/api/v1/"))
              .setDoc(" Base URL to extract the source code");

      QUEUE_NAMES_VALID_FOR_CODE_NAME_EXTRACTION =
          new CodeAnalysisConfiguration<String[]>().setConfigurationName(QUEUE_NAMES_VALID_FOR_CODE_NAME_EXTRACTION_NAME)
                .setValue(configuration.getStrings(QUEUE_NAMES_VALID_FOR_CODE_NAME_EXTRACTION_NAME))
              .setDoc(" Projects/queues which are valid for code name extraction");

      if (debugEnabled) {
        logger.debug(" Code Level Analysis configurations ");
        logger.debug(BASE_URL_FOR_EXTRACTING_CODE);
        logger.debug(QUEUE_NAMES_VALID_FOR_CODE_NAME_EXTRACTION);
      }
    }
  }
}
