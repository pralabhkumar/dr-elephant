package com.linkedin.drelephant.tuning.obt;

import com.linkedin.drelephant.tuning.engine.SparkExecutionEngine;
import java.util.List;
import models.JobSuggestedParamSet;
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import models.TuningParameter;
import org.apache.log4j.Logger;

public class ParameterGenerateManagerOBTAlgoPSOSparkImpl<T extends SparkExecutionEngine>
    extends ParameterGenerateManagerOBTAlgoPSOImpl<T> {
  T sparkExecutionEngine;
  private final Logger logger = Logger.getLogger(getClass());
  public ParameterGenerateManagerOBTAlgoPSOSparkImpl(T sparkExecutionEngine) {
    this.sparkExecutionEngine = sparkExecutionEngine;
  }

  @Override
  protected List<JobSuggestedParamSet> getPendingParamSets() {
    List<JobSuggestedParamSet> pendingParamSetList = sparkExecutionEngine.getPendingJobs()
        .eq(JobSuggestedParamSet.TABLE.tuningAlgorithm
            + "." + TuningAlgorithm.TABLE.optimizationAlgo, TuningAlgorithm.OptimizationAlgo.PSO.name())
        .eq(JobSuggestedParamSet.TABLE.isParamSetDefault, 0)
        .findList();
    return pendingParamSetList;
  }

  @Override
  protected List<TuningJobDefinition> getTuningJobDefinitions() {
    return sparkExecutionEngine.getTuningJobDefinitionsForParameterSuggestion()
        .eq(TuningJobDefinition.TABLE.tuningAlgorithm
            + "." + TuningAlgorithm.TABLE.optimizationAlgo, TuningAlgorithm.OptimizationAlgo.PSO.name())
        .findList();
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
