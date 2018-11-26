package com.linkedin.drelephant.tuning.obt;

import com.linkedin.drelephant.tuning.engine.SparkExecutionEngine;
import java.util.List;
import models.AppResult;
import models.JobExecution;
import models.JobSuggestedParamSet;
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import models.TuningParameter;


public class ParameterGenerateManagerOBTAlgoPSOIPSOSparkImpl<T extends SparkExecutionEngine> extends ParameterGenerateManagerOBTAlgoPSOIPSOImpl<T> {
  T sparkExecutionEngine;
  public ParameterGenerateManagerOBTAlgoPSOIPSOSparkImpl(T sparkExecutionEngine) {
    this.sparkExecutionEngine=sparkExecutionEngine;
  }

  @Override
  protected List<JobSuggestedParamSet> getPendingParamSets() {
    List<JobSuggestedParamSet> pendingParamSetList = sparkExecutionEngine.getPendingJobs()
        .eq(JobSuggestedParamSet.TABLE.tuningAlgorithm + "." + TuningAlgorithm.TABLE.optimizationAlgo,
            TuningAlgorithm.OptimizationAlgo.PSO_IPSO.name())
        .eq(JobSuggestedParamSet.TABLE.isParamSetDefault, 0)
        .findList();
    return pendingParamSetList;
  }

  @Override
  protected List<TuningJobDefinition> getTuningJobDefinitions() {
    return sparkExecutionEngine.getTuningJobDefinitionsForParameterSuggestion()
        .eq(TuningJobDefinition.TABLE.tuningAlgorithm + "." + TuningAlgorithm.TABLE.optimizationAlgo,
            TuningAlgorithm.OptimizationAlgo.PSO_IPSO.name())
        .findList();
  }

  @Override
  public void parameterOptimizer(List<AppResult> appResults, JobExecution jobExecution) {

  }

  @Override
  public boolean isParamConstraintViolated(List<JobSuggestedParamValue> jobSuggestedParamValues) {
    return false;
  }

  @Override
  public void computeValuesOfDerivedConfigurationParameters(List<TuningParameter> derivedParameterList,
      List<JobSuggestedParamValue> jobSuggestedParamValueList) {
    sparkExecutionEngine.computeValuesOfDerivedConfigurationParameters(derivedParameterList,jobSuggestedParamValueList);
  }
}
