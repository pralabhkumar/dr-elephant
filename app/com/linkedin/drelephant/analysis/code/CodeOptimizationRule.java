package com.linkedin.drelephant.analysis.code;

import com.linkedin.drelephant.analysis.code.dataset.Script;


public interface CodeOptimizationRule {
  void processRule(Script script);
  String getRuleName();
  String getSeverity();
}
