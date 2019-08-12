package com.linkedin.drelephant.analysis.code;

import com.linkedin.drelephant.analysis.code.dataset.Script;
import com.linkedin.drelephant.analysis.code.util.CodeAnalyzerException;


public interface CodeOptimizationRule {
  void processRule(Script script) throws CodeAnalyzerException;
  String getRuleName();
  String getSeverity();
}
