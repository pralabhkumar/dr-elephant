package com.linkedin.drelephant.tuning.obt;

import com.avaje.ebean.Expr;
import com.linkedin.drelephant.tuning.ExecutionEngine;
import java.util.List;
import models.AppResult;
import models.JobDefinition;
import models.JobExecution;
import models.JobSuggestedParamSet;
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import models.TuningParameter;
import org.apache.log4j.Logger;

public abstract class ParameterGenerateManagerOBTAlgoPSOImpl<T extends ExecutionEngine> extends ParameterGenerateManagerOBTAlgoPSO<T> {
  private final Logger logger = Logger.getLogger(getClass());

  @Override
  protected void updateBoundryConstraint(List<TuningParameter> tuningParameterList, JobDefinition job) {

  }



  @Override
  public void initializePrerequisite(TuningAlgorithm tuningAlgorithm, JobDefinition job) {

  }

  @Override
  public void parameterOptimizer(List<AppResult> appResults, JobExecution jobExecution) {

  }

  @Override
  public int getSwarmSize() {
    return 3;
  }

  public String getManagerName() {
    return "ParameterGenerateManagerOBTAlgoPSOImpl" + this.getClass().getSimpleName();
  }
}
