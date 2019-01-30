package com.linkedin.drelephant.exceptions.spark;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.linkedin.drelephant.exceptions.spark.Constant.*;


/**
 * It will be util class which will container helper method and will be static.
 */
public class ExceptionUtils {
  private static final List<Pattern> patterns = new ArrayList<Pattern>();

  static {
    for (String regex : REGEX_FOR_EXCEPTION_IN_LOGS) {
      patterns.add(Pattern.compile(regex, Pattern.CASE_INSENSITIVE));
    }
  }

  public static boolean isExceptionContains(String data) {
    for (Pattern pattern : patterns) {
      Matcher matcher = pattern.matcher(data);
      if (matcher.find()) {
        return true;
      }
    }
    return false;
  }
}
