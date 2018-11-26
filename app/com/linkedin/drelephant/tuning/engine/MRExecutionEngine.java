package com.linkedin.drelephant.tuning.engine;

import com.avaje.ebean.Expr;
import com.avaje.ebean.ExpressionList;
import com.linkedin.drelephant.mapreduce.heuristics.CommonConstantsHeuristic;
import com.linkedin.drelephant.tuning.ExecutionEngine;
import com.linkedin.drelephant.util.MemoryFormatUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import models.AppHeuristicResult;
import models.AppHeuristicResultDetails;
import models.AppResult;
import models.JobDefinition;
import models.JobExecution;
import models.JobSuggestedParamSet;
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import models.TuningParameter;
import models.TuningParameterConstraint;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import static java.lang.Math.*;


/**
 * This class represents Map Reduce Execution Engine. It handles all the cases related to Map Reduce Engine
 */

public class MRExecutionEngine implements ExecutionEngine {
  private final Logger logger = Logger.getLogger(getClass());
  boolean debugEnabled = logger.isDebugEnabled();
  public enum UsageCounterSchema {USED_PHYSICAL_MEMORY, USED_VIRTUAL_MEMORY, USED_HEAP_MEMORY}
  public final String functionTypes[] = {"map", "reduce"};

  @Override
  public void computeValuesOfDerivedConfigurationParameters(List<TuningParameter> derivedParameterList,
      List<JobSuggestedParamValue> jobSuggestedParamValues) {
    Map<String, Double> jobSuggestedParamValueMap = new HashMap<String, Double>();
    for (JobSuggestedParamValue jobSuggestedParamValue : jobSuggestedParamValues) {
      jobSuggestedParamValueMap.put(jobSuggestedParamValue.tuningParameter.paramName,
          jobSuggestedParamValue.paramValue);
    }

    for (TuningParameter derivedParameter : derivedParameterList) {
      logger.info("Computing value of derived param: " + derivedParameter.paramName);
      Double paramValue = null;
      if (derivedParameter.paramName.equals("mapreduce.reduce.java.opts")) {
        String parentParamName = "mapreduce.reduce.memory.mb";
        if (jobSuggestedParamValueMap.containsKey(parentParamName)) {
          paramValue = 0.75 * jobSuggestedParamValueMap.get(parentParamName);
        }
      } else if (derivedParameter.paramName.equals("mapreduce.map.java.opts")) {
        String parentParamName = "mapreduce.map.memory.mb";
        if (jobSuggestedParamValueMap.containsKey(parentParamName)) {
          paramValue = 0.75 * jobSuggestedParamValueMap.get(parentParamName);
        }
      } else if (derivedParameter.paramName.equals("mapreduce.input.fileinputformat.split.maxsize")) {
        String parentParamName = "pig.maxCombinedSplitSize";
        if (jobSuggestedParamValueMap.containsKey(parentParamName)) {
          paramValue = jobSuggestedParamValueMap.get(parentParamName);
        }
      }

      if (paramValue != null) {
        JobSuggestedParamValue jobSuggestedParamValue = new JobSuggestedParamValue();
        jobSuggestedParamValue.paramValue = paramValue;
        jobSuggestedParamValue.tuningParameter = derivedParameter;
        jobSuggestedParamValues.add(jobSuggestedParamValue);
      }
    }
  }



  @Override
  public ExpressionList<JobSuggestedParamSet> getPendingJobs() {
    return JobSuggestedParamSet.find.select("*")
        .fetch(JobSuggestedParamSet.TABLE.jobDefinition, "*")
        .where()
        .or(Expr.or(Expr.eq(JobSuggestedParamSet.TABLE.paramSetState, JobSuggestedParamSet.ParamSetStatus.CREATED),
            Expr.eq(JobSuggestedParamSet.TABLE.paramSetState, JobSuggestedParamSet.ParamSetStatus.SENT)),
            Expr.eq(JobSuggestedParamSet.TABLE.paramSetState, JobSuggestedParamSet.ParamSetStatus.EXECUTED))
        .eq(JobSuggestedParamSet.TABLE.tuningAlgorithm + "." + TuningAlgorithm.TABLE.jobType,
            TuningAlgorithm.JobType.PIG.name())
        .eq(JobSuggestedParamSet.TABLE.isParamSetBest, 0);
  }

  @Override
  public ExpressionList<TuningJobDefinition> getTuningJobDefinitionsForParameterSuggestion() {
    return TuningJobDefinition.find.select("*")
        .fetch(TuningJobDefinition.TABLE.job, "*")
        .where()
        .eq(TuningJobDefinition.TABLE.tuningEnabled, 1)
        .eq(TuningJobDefinition.TABLE.tuningAlgorithm + "." + TuningAlgorithm.TABLE.jobType,
            TuningAlgorithm.JobType.PIG.name());
  }

  @Override
  public List<Double> extractUsageParameter(String functionType, Map<String, Map<String, Double>> usageDataGlobal) {
    Double usedPhysicalMemoryMB = 0.0, usedVirtualMemoryMB = 0.0, usedHeapMemoryMB = 0.0;
    usedPhysicalMemoryMB = usageDataGlobal.get(functionType)
        .get(CommonConstantsHeuristic.UtilizedParameterKeys.MAX_PHYSICAL_MEMORY.getValue());
    usedVirtualMemoryMB = usageDataGlobal.get(functionType)
        .get(CommonConstantsHeuristic.UtilizedParameterKeys.MAX_VIRTUAL_MEMORY.getValue());
    usedHeapMemoryMB = usageDataGlobal.get(functionType)
        .get(CommonConstantsHeuristic.UtilizedParameterKeys.MAX_TOTAL_COMMITTED_HEAP_USAGE_MEMORY.getValue());
    if (debugEnabled) {
      logger.debug(" Usage Stats " + functionType);
      logger.debug(" Physical Memory Usage MB " + usedPhysicalMemoryMB);
      logger.debug(" Virtual Memory Usage MB " + usedVirtualMemoryMB / 2.1);
      logger.debug(" Heap Usage MB " + usedHeapMemoryMB);
    }
    List<Double> usageStats = new ArrayList<Double>();
    usageStats.add(usedPhysicalMemoryMB);
    usageStats.add(usedVirtualMemoryMB);
    usageStats.add(usedHeapMemoryMB);
    return usageStats;
  }

  @Override
  public Map<String, Map<String, Double>> extractParameterInformation(List<AppResult> appResults) {
    logger.info(" Extract Parameter Information for MR");
    Map<String, Map<String, Double>> usageDataGlobal = new HashMap<String, Map<String, Double>>();
    intialize(usageDataGlobal);
    for (AppResult appResult : appResults) {
      Map<String, Map<String, Double>> usageDataApplicationlocal = collectUsageDataPerApplication(appResult);
      for (String functionType : usageDataApplicationlocal.keySet()) {
        Map<String, Double> usageDataForFunctionGlobal = usageDataGlobal.get(functionType);
        Map<String, Double> usageDataForFunctionlocal = usageDataApplicationlocal.get(functionType);
        for (String usageName : usageDataForFunctionlocal.keySet()) {
          usageDataForFunctionGlobal.put(usageName,
              max(usageDataForFunctionGlobal.get(usageName), usageDataForFunctionlocal.get(usageName)));
        }
      }
    }
    logger.info("Usage Values Global ");
    printInformation(usageDataGlobal);
    return usageDataGlobal;
  }

  private void printInformation(Map<String, Map<String, Double>> information) {
    for (String functionType : information.keySet()) {
      if (debugEnabled) {
        logger.debug("function Type    " + functionType);
      }
      Map<String, Double> usage = information.get(functionType);
      for (String data : usage.keySet()) {
        if (debugEnabled) {
          logger.debug(data + " " + usage.get(data));
        }
      }
    }
  }

  private void collectUsageDataPerApplicationForFunction(AppHeuristicResult appHeuristicResult,
      Map<String, Double> counterData) {
    if (appHeuristicResult.yarnAppHeuristicResultDetails != null) {
      for (AppHeuristicResultDetails appHeuristicResultDetails : appHeuristicResult.yarnAppHeuristicResultDetails) {
        for (CommonConstantsHeuristic.UtilizedParameterKeys value : CommonConstantsHeuristic.UtilizedParameterKeys.values()) {
          if (appHeuristicResultDetails.name.equals(value.getValue())) {
            counterData.put(value.getValue(), appHeuristicResultDetails.value == null ? 0
                : ((double) MemoryFormatUtils.stringToBytes(appHeuristicResultDetails.value)));
          }
        }
      }
    }
  }



  private void intialize(Map<String, Map<String, Double>> usageDataGlobal) {
    for (String function : functionTypes) {
      Map<String, Double> usageData = new HashMap<String, Double>();
      for (CommonConstantsHeuristic.UtilizedParameterKeys value : CommonConstantsHeuristic.UtilizedParameterKeys.values()) {
        usageData.put(value.getValue(), 0.0);
      }
      usageDataGlobal.put(function, usageData);
    }
  }

  private Map<String, Map<String, Double>> collectUsageDataPerApplication(AppResult appResult) {
    Map<String, Map<String, Double>> usageData = null;
    usageData = new HashMap<String, Map<String, Double>>();
    if (appResult.yarnAppHeuristicResults != null) {
      for (AppHeuristicResult appHeuristicResult : appResult.yarnAppHeuristicResults) {

        if (appHeuristicResult.heuristicName.equals("Mapper Memory")) {
          Map<String, Double> counterData = new HashMap<String, Double>();
          collectUsageDataPerApplicationForFunction(appHeuristicResult, counterData);
          usageData.put("map", counterData);
        }
        if (appHeuristicResult.heuristicName.equals("Reducer Memory")) {
          Map<String, Double> counterData = new HashMap<String, Double>();
          collectUsageDataPerApplicationForFunction(appHeuristicResult, counterData);
          usageData.put("reduce", counterData);
        }
      }
    }
    logger.info("Usage Values local   " + appResult.jobExecUrl);
    printInformation(usageData);

    return usageData;
  }



}


