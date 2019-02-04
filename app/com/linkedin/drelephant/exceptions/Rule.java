package com.linkedin.drelephant.exceptions;

import com.linkedin.drelephant.exceptions.util.ExceptionInfo;
import java.util.List;
import static com.linkedin.drelephant.exceptions.util.Constant.*;

public interface Rule {
  /**
   * This will contain the actual logic of the rule . For e.g for RegexRule
   * it will contain regex which are used to classify exceptions
   * @param exceptions
   * @return
   */
  LogClass logic(List<ExceptionInfo> exceptions);

  /**
   * Every rule set the priorty , this priority can be used by rule based classifier to finally decide the class of the exception.
   * @param priority
   */
  Rule setPriority(RulePriority priority);

  RulePriority getPriority();


}
