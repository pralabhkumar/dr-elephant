package com.linkedin.drelephant.analysis.code.optimizers;

import com.linkedin.drelephant.analysis.code.CodeOptimizer;
import com.linkedin.drelephant.analysis.code.optimizers.hive.HiveCodeOptimizer;
import org.apache.log4j.Logger;


public class CodeOptimizerFactory {
  private static final Logger logger = Logger.getLogger(CodeOptimizerFactory.class);
  public static CodeOptimizer getCodeOptimizer(String fileName){
    if(fileName.endsWith(".sql")||fileName.endsWith(".hql")){
      logger.info(" Source code is of Hive type , therefore hive code optimizer is called ");
      return new HiveCodeOptimizer();
    }
    else {
      return null;
    }
  }

}
