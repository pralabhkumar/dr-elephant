package com.linkedin.drelephant.analysis.code;

import com.linkedin.drelephant.analysis.Severity;


public interface CodeOptimizer {
  Code generateCode (String inputFileName);
  Script execute(String inputFileName);
  String getSeverity();
}
