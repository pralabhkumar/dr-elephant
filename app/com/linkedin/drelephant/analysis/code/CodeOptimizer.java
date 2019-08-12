package com.linkedin.drelephant.analysis.code;

import com.linkedin.drelephant.analysis.code.dataset.Code;
import com.linkedin.drelephant.analysis.code.dataset.Script;
import java.util.List;


/**
 * Code Optimizier Interface , one of the concrete implementation
 * is HiveCodeOptimizer
 */
public interface CodeOptimizer {
  /**
   *
   * @param sourceCode : Give the sourceCode and convert it into Code (which is List<Statement></Statement>)
   * @return Return Code : Format on which optimizations can be applied.
   */
  Code generateCode (String sourceCode);

  /**
   *  Return the Severity of the code, High severity means , it should be fix
   *  immediately
   * @return
   */
  String getSeverity();

  /**
   * This is the brain of the optimization . It will do the optimization of the code.
   * @param sourceScript : Source Code
   * @param rules : Rules to apply to optimize the code.
   * @return
   */

  void optimizationEngine(Script sourceScript , List<CodeOptimizationRule> rules);

  /**
   *
   * @param sourceCode Souce code of the script
   * @return : Script , which contains optimization comments and also Code.
   */

  Script execute(String sourceCode);
}

