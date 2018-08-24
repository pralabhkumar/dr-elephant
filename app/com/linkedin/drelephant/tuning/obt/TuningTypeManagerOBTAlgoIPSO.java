package com.linkedin.drelephant.tuning.obt;

import com.avaje.ebean.Expr;
import com.linkedin.drelephant.mapreduce.heuristics.CommonConstantsHeuristic;
import com.linkedin.drelephant.tuning.ExecutionEngine;
import com.linkedin.drelephant.tuning.engine.MRExecutionEngine;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import models.AppResult;
import models.JobDefinition;
import models.JobSuggestedParamSet;
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import models.TuningParameter;
import models.TuningParameterConstraint;
import org.apache.log4j.Logger;

import static java.lang.Math.*;


public class TuningTypeManagerOBTAlgoIPSO extends TuningTypeManagerOBT {

  private static final Logger logger = Logger.getLogger(TuningTypeManagerOBTAlgoIPSO.class);
  private Map<String, Map<String, Double>> usageDataGlobal = null;


  enum UsageCounterSchema {USED_PHYSICAL_MEMORY, USED_VIRTUAL_MEMORY, USED_HEAP_MEMORY}

  public TuningTypeManagerOBTAlgoIPSO(ExecutionEngine executionEngine) {
    tuningAlgorithm = TuningAlgorithm.OptimizationAlgo.PSO_IPSO.name();
    this._executionEngine = executionEngine;
  }

  @Override
  protected void updateBoundryConstraint(List<TuningParameter> tuningParameterList,
      TuningJobDefinition tuningJobDefinition, JobDefinition job) {
    applyIntelligenceOnParameter(tuningParameterList, job);
  }

  @Override
  public boolean isParamConstraintViolated(List<JobSuggestedParamValue> jobSuggestedParamValues) {
    return _executionEngine.isParamConstraintViolatedIPSO(jobSuggestedParamValues);
  }



  @Override
  protected List<JobSuggestedParamSet> getPendingParamSets() {
    List<JobSuggestedParamSet> pendingParamSetList = _executionEngine.getPendingJobs()
        .eq(JobSuggestedParamSet.TABLE.tuningAlgorithm
            + "." + TuningAlgorithm.TABLE.optimizationAlgo, TuningAlgorithm.OptimizationAlgo.PSO_IPSO.name())
        .findList();
    return pendingParamSetList;
  }

  @Override
  protected List<TuningJobDefinition> getTuningJobDefinitions() {
    return _executionEngine.getTuningJobDefinitionsForParameterSuggestion()
        .eq(TuningJobDefinition.TABLE.tuningAlgorithm
            + "." + TuningAlgorithm.TABLE.optimizationAlgo, TuningAlgorithm.OptimizationAlgo.PSO_IPSO.name())
        .findList();
  }

  @Override
  public void intializePrerequisite(TuningAlgorithm tuningAlgorithm, JobSuggestedParamSet jobSuggestedParamSet) {
    logger.info(" Intialize Prerequisite ");
    setDefaultParameterValues(tuningAlgorithm, jobSuggestedParamSet);
  }

  private void setDefaultParameterValues(TuningAlgorithm tuningAlgorithm, JobSuggestedParamSet jobSuggestedParamSet) {
    List<TuningParameter> tuningParameters =
        TuningParameter.find.where().eq(TuningParameter.TABLE.tuningAlgorithm, tuningAlgorithm).findList();
    for (TuningParameter tuningParameter : tuningParameters) {
      TuningParameterConstraint tuningParameterConstraint = new TuningParameterConstraint();
      tuningParameterConstraint.jobDefinition = jobSuggestedParamSet.jobDefinition;
      tuningParameterConstraint.tuningParameter = tuningParameter;
      tuningParameterConstraint.lowerBound = tuningParameter.minValue;
      tuningParameterConstraint.upperBound = tuningParameter.maxValue;
      tuningParameterConstraint.constraintType = TuningParameterConstraint.ConstraintType.BOUNDARY;
      tuningParameterConstraint.save();
    }
  }

  private void intialize() {
    usageDataGlobal = _executionEngine.intializeUsageCounterValuesIPSO();
  }

  @Override
  public Map<String, Map<String, Double>> extractParameterInformation(List<AppResult> appResults) {
    logger.info(" Extract Parameter Information");
    intialize();
    for (AppResult appResult : appResults) {
      Map<String, Map<String, Double>> usageDataApplicationlocal = _executionEngine.collectUsageDataPerApplicationIPSO(appResult);
      for (String functionType : usageDataApplicationlocal.keySet()) {
        Map<String, Double> usageDataForFunctionGlobal = usageDataGlobal.get(functionType);
        Map<String, Double> usageDataForFunctionlocal = usageDataApplicationlocal.get(functionType);
        for (String usageName : usageDataForFunctionlocal.keySet()) {
          usageDataForFunctionGlobal.put(usageName,
              max(usageDataForFunctionGlobal.get(usageName), usageDataForFunctionlocal.get(usageName)));
        }
      }
    }
    logger.debug("Usage Values Global ");
    printInformation(usageDataGlobal);
    return usageDataGlobal;
  }

  private void printInformation(Map<String, Map<String, Double>> information) {
    /*for (String functionType : information.keySet()) {
      logger.debug("function Type    " + functionType);
      Map<String, Double> usage = information.get(functionType);
      for (String data : usage.keySet()) {
        logger.debug(data + " " + usage.get(data));
      }
    }*/
  }




  @Override
  public void parameterOptimizer(Integer jobID) {
    logger.info(" IPSO Optimizer");
    List<TuningParameterConstraint> parameterConstraints = TuningParameterConstraint.find.where().
        eq("job_definition_id", jobID).findList();
    this._executionEngine.parameterOptimizerIPSO(jobID,usageDataGlobal,parameterConstraints);
  }

  @Override
  public void applyIntelligenceOnParameter(List<TuningParameter> tuningParameterList, JobDefinition job) {
    logger.info(" Apply Intelligence");
    List<TuningParameterConstraint> tuningParameterConstraintList = new ArrayList<TuningParameterConstraint>();
    try {
      tuningParameterConstraintList = TuningParameterConstraint.find.where()
          .eq("job_definition_id", job.id)
          .eq(TuningParameterConstraint.TABLE.constraintType, TuningParameterConstraint.ConstraintType.BOUNDARY)
          .findList();
    } catch (NullPointerException e) {
      logger.info("No boundary constraints found for job: " + job.jobName);
    }

    Map<Integer, Integer> paramConstrainIndexMap = new HashMap<Integer, Integer>();
    int i = 0;
    for (TuningParameterConstraint tuningParameterConstraint : tuningParameterConstraintList) {
      paramConstrainIndexMap.put(tuningParameterConstraint.tuningParameter.id, i);
      i += 1;
    }

    for (TuningParameter tuningParameter : tuningParameterList) {
      if (paramConstrainIndexMap.containsKey(tuningParameter.id)) {
        int index = paramConstrainIndexMap.get(tuningParameter.id);
        tuningParameter.minValue = tuningParameterConstraintList.get(index).lowerBound;
        tuningParameter.maxValue = tuningParameterConstraintList.get(index).upperBound;
      }
    }
  }

  @Override
  public int getSwarmSize() {
    return 2;
  }

  public String getManagerName() {
    return "TuningTypeManagerOBTAlgoIPSO" + this._executionEngine.getClass().getSimpleName();
  }
}
