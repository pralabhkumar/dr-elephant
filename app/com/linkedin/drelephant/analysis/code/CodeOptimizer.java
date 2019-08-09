package com.linkedin.drelephant.analysis.code;

import com.linkedin.drelephant.analysis.code.dataset.Code;
import com.linkedin.drelephant.analysis.code.dataset.Script;


public interface CodeOptimizer {
  Code generateCode (String inputFileName);
  Script execute(String inputFileName);
  String getSeverity();
}
