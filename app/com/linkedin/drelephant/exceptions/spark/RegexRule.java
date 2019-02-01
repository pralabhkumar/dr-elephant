package com.linkedin.drelephant.exceptions.spark;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;

import static com.linkedin.drelephant.exceptions.spark.Constant.*;


/**
 * Rule identifies exception class based on regex .
 */
public class RegexRule implements Rule{
  private static final Logger logger = Logger.getLogger(RegexRule.class);
  boolean debugEnabled = logger.isDebugEnabled();
  private List<ExceptionInfo> exceptions ;
  private static final List<Pattern> patterns = new ArrayList<Pattern>();
  RulePriority rulePriority;
  static {
    for (String regex : REGEX_AUTO_TUNING_FAULT) {
      patterns.add(Pattern.compile(regex, Pattern.CASE_INSENSITIVE));
    }
  }

  /**
   * This rule checks for Out of Memory and even if one log have Out of Memory. It will classify the
   * exceptions to Auto Tune Enabled exception . Another extension to this logic is to check how many
   * logs have out of memory exception/error (percentage out of total)
   * and based on that decide whether its autotuning error or not.
   * @return : If its out of memory error
   */

  public LogClass logic (List<ExceptionInfo> exceptions){
    this.exceptions = exceptions;
    for (ExceptionInfo exceptionInfo : this.exceptions) {
      if(checkForPattern((exceptionInfo.getExceptionName() + " " + exceptionInfo.getExcptionStackTrace()))) {
        if(debugEnabled) {
          logger.debug("Exception Analysis " + exceptionInfo.getExceptionName() + " " + exceptionInfo.getExcptionStackTrace());
        }
        logger.info(" AutoTuning Fault ");
        return LogClass.AUTOTUNING_ENABLED;
      }
    }
    logger.info(" User Fault ");
    return LogClass.USER_ENABLED;
  }

  @Override
  public Rule setPriority(RulePriority priority) {
    rulePriority = priority;
    return this;
  }

  @Override
  public RulePriority getPriority() {
    return rulePriority;
  }

  private boolean checkForPattern(String data){
    for (Pattern pattern : patterns) {
      Matcher matcher = pattern.matcher(data);
      if (matcher.find()) {
        return true;
      }
    }
    return false;
  }
}
