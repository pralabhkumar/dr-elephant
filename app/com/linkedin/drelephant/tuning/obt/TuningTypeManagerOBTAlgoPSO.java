package com.linkedin.drelephant.tuning.obt;

import com.avaje.ebean.Expr;
import com.linkedin.drelephant.tuning.ExecutionEngine;
import java.util.List;
import java.util.Map;
import models.AppResult;
import models.JobDefinition;
import models.JobSuggestedParamSet;
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import models.TuningParameter;


public class TuningTypeManagerOBTAlgoPSO extends TuningTypeManagerOBT{
  public TuningTypeManagerOBTAlgoPSO(ExecutionEngine executionEngine) {
    tuningAlgorithm = TuningAlgorithm.OptimizationAlgo.PSO.name();
    this._executionEngine=executionEngine;
  }

  @Override
  protected void updateBoundryConstraint(List<TuningParameter> tuningParameterList,
      TuningJobDefinition tuningJobDefinition, JobDefinition job) {

  }

  @Override
  public boolean isParamConstraintViolated(List<JobSuggestedParamValue> jobSuggestedParamValues) {
    return _executionEngine.isParamConstraintViolatedPSO(jobSuggestedParamValues);
  }

  @Override
  public Map<String, Map<String, Double>> extractParameterInformation(List<AppResult> appResults) {
    return null;
  }

  @Override
  protected List<JobSuggestedParamSet> getPendingParamSets() {
    List<JobSuggestedParamSet> pendingParamSetList = _executionEngine.getPendingJobs()
        .eq(TuningJobDefinition.TABLE.tuningAlgorithm, TuningAlgorithm.OptimizationAlgo.PSO.name())
        .findList();
    return pendingParamSetList;
  }

  @Override
  protected List<TuningJobDefinition> getTuningJobDefinitions() {
    return _executionEngine.getTuningJobDefinitionsForParameterSuggestion()
        .eq(TuningJobDefinition.TABLE.tuningAlgorithm, TuningAlgorithm.OptimizationAlgo.PSO.name())
        .findList();
  }

  @Override
  public void intializePrerequisite(TuningAlgorithm tuningAlgorithm, JobSuggestedParamSet jobSuggestedParamSet) {

  }

  @Override
  public void parameterOptimizer(Integer jobID) {

  }

  @Override
  public void applyIntelligenceOnParameter(List<TuningParameter> tuningParameterList, JobDefinition job) {

  }

  @Override
  public int getSwarmSize() {
    return 3;
  }
}
