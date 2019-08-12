package com.linkedin.drelephant.analysis.code.extractors;

import com.linkedin.drelephant.analysis.code.CodeExtractor;
import com.linkedin.drelephant.analysis.code.CodeOptimizer;
import models.AppResult;
import org.apache.log4j.Logger;


/**
 * Factory class for code extractor . By default extractor is AzkabanJarvis
 * ,which will take extract file Name from Azkaban and Code from Jarvis
 */
public class CodeExtractionFactory {
  private static final Logger logger = Logger.getLogger(CodeExtractionFactory.class);
  public static CodeExtractor getCodeExtractor(AppResult appResult){
    logger.info(" Default code extractor is AzkabanJarvis");
    return new AzkabanJarvisCodeExtractor();
  }
}
