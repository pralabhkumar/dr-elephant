package com.linkedin.drelephant.tuning;

import com.linkedin.drelephant.util.MemoryFormatUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import models.JobDefinition;
import models.JobSuggestedParamSet;
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningParameter;
import org.apache.log4j.Logger;
import models.AppHeuristicResult;
import models.AppHeuristicResultDetails;
import models.AppResult;
import com.linkedin.drelephant.mapreduce.heuristics.*;
import models.TuningParameterConstraint;
import org.apache.commons.io.FileUtils;

import static com.linkedin.drelephant.mapreduce.heuristics.CommnConstantsMRAutoTuningIPSOHeuristics.UTILIZED_PARAMETER_KEYS;
import static java.lang.Double.*;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static com.linkedin.drelephant.tuning.IPSOConstants.*;


/*
This Implements IPSO in AutoTuning Framework

 */
public class IPSOManager implements AutoTuningOptimizeManager {
  private static final Logger logger = Logger.getLogger(IPSOManager.class);
  private Map<String, Map<String, Double>> usageDataGlobal = null;
  private String functionTypes[] = {"map", "reduce"};

  enum USAGE_COUNTER_SCHEMA {USED_PHYSICAL_MEMORY, USED_VIRTUAL_MEMORY, USED_HEAP_MEMORY}

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
      tuningParameterConstraint.constraintId = tuningParameter.id;
      tuningParameterConstraint.lowerBound = tuningParameter.minValue;
      tuningParameterConstraint.upperBound = tuningParameter.maxValue;
      tuningParameterConstraint.constraintType = TuningParameterConstraint.ConstraintType.BOUNDARY;
      tuningParameterConstraint.paramName = tuningParameter.paramName;
      tuningParameterConstraint.save();
    }
  }

  @Override
  public void extractParameterInformation(List<AppResult> appResults) {
    logger.info(" Extract Parameter Information");
    usageDataGlobal = new HashMap<String, Map<String, Double>>();
    intialize();
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
  }

  private void intialize() {
    for (String function : functionTypes) {
      Map<String, Double> usageData = new HashMap<String, Double>();
      for (UTILIZED_PARAMETER_KEYS value : UTILIZED_PARAMETER_KEYS.values()) {
        usageData.put(value.getValue(), 0.0);
      }
      usageDataGlobal.put(function, usageData);
    }
  }

  private Map<String, Map<String, Double>> collectUsageDataPerApplication(AppResult appResult) {
    Map<String, Map<String, Double>> usageData = new HashMap<String, Map<String, Double>>();
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

  private void printInformation(Map<String, Map<String, Double>> information) {
    for (String functionType : information.keySet()) {
      logger.info("function Type    " + functionType);
      Map<String, Double> usage = information.get(functionType);
      for (String data : usage.keySet()) {
        logger.info(data + " " + usage.get(data));
      }
    }
  }

  private void collectUsageDataPerApplicationForFunction(AppHeuristicResult appHeuristicResult,
      Map<String, Double> counterData) {
    if (appHeuristicResult.yarnAppHeuristicResultDetails != null) {
      for (AppHeuristicResultDetails appHeuristicResultDetails : appHeuristicResult.yarnAppHeuristicResultDetails) {
        for (UTILIZED_PARAMETER_KEYS value : UTILIZED_PARAMETER_KEYS.values()) {
          if (appHeuristicResultDetails.name.equals(value.getValue())) {
            counterData.put(value.getValue(), appHeuristicResultDetails.value == null ? 0
                : ((double) MemoryFormatUtils.stringToBytes(appHeuristicResultDetails.value)));
          }
        }
      }
    }
  }

  @Override
  public void parameterOptimizer(Integer jobID) {
    logger.info(" IPSO Optimizer");
    List<TuningParameterConstraint> parameterConstraints = TuningParameterConstraint.find.where().
        eq("job_definition_id", jobID).findList();
    for (String function : functionTypes) {
      logger.info(" Optimizing Parameter Space  " + function);
      if (usageDataGlobal.get(function).get(UTILIZED_PARAMETER_KEYS.MAX_PHYSICAL_MEMORY.getValue()) > 0.0) {
        List<Double> usageStats = extractUsageParameter(function);
        Map<String, TuningParameterConstraint> memoryConstraints =
            filterMemoryConstraint(parameterConstraints, function);
        memoryParameterIPSO(function, memoryConstraints, usageStats);
      }
    }
  }

  private Map<String, TuningParameterConstraint> filterMemoryConstraint(
      List<TuningParameterConstraint> parameterConstraints, String functionType) {
    Map<String, TuningParameterConstraint> memoryConstraints = new HashMap<String, TuningParameterConstraint>();
    for (TuningParameterConstraint parameterConstraint : parameterConstraints) {
      if (functionType.equals("map")) {
        if (parameterConstraint.paramName.equals(PARAMTER_CONSTRAINT.MAPPER_MEMORY.getValue())) {
          memoryConstraints.put("CONTAINER_MEMORY", parameterConstraint);
        }
        if (parameterConstraint.paramName.equals(PARAMTER_CONSTRAINT.MAPPER_HEAP_MEMORY.getValue())) {
          memoryConstraints.put("CONTAINER_HEAP", parameterConstraint);
        }
      }
      if (functionType.equals("reduce")) {
        if (parameterConstraint.paramName.equals(PARAMTER_CONSTRAINT.REDUCER_MEMORY.getValue())) {
          memoryConstraints.put("CONTAINER_MEMORY", parameterConstraint);
        }
        if (parameterConstraint.paramName.equals(PARAMTER_CONSTRAINT.REDUCER_HEAP_MEMORY.getValue())) {
          memoryConstraints.put("CONTAINER_HEAP", parameterConstraint);
        }
      }
    }
    return memoryConstraints;
  }

  private List<Double> extractUsageParameter(String functionType) {
    Double usedPhysicalMemoryMB = 0.0, usedVirtualMemoryMB = 0.0, usedHeapMemoryMB = 0.0;
    usedPhysicalMemoryMB =
        usageDataGlobal.get(functionType).get(UTILIZED_PARAMETER_KEYS.MAX_PHYSICAL_MEMORY.getValue());
    usedVirtualMemoryMB = usageDataGlobal.get(functionType).get(UTILIZED_PARAMETER_KEYS.MAX_VIRTUAL_MEMORY.getValue());
    usedHeapMemoryMB =
        usageDataGlobal.get(functionType).get(UTILIZED_PARAMETER_KEYS.MAX_TOTAL_COMMITTED_HEAP_USAGE_MEMORY.getValue());
    logger.info(" Usage Stats " + functionType);
    logger.info(" Physical Memory Usage MB " + usedPhysicalMemoryMB);
    logger.info(" Virtual Memory Usage MB " + usedVirtualMemoryMB / 2.1);
    logger.info(" Heap Usage MB " + usedHeapMemoryMB);
    List<Double> usageStats = new ArrayList<Double>();
    usageStats.add(usedPhysicalMemoryMB);
    usageStats.add(usedVirtualMemoryMB);
    usageStats.add(usedHeapMemoryMB);
    return usageStats;
  }

  private void memoryParameterIPSO(String trigger, Map<String, TuningParameterConstraint> constraints,
      List<Double> usageStats) {
    logger.info(" IPSO for " + trigger);
    Double usagePhysicalMemory = usageStats.get(USAGE_COUNTER_SCHEMA.USED_PHYSICAL_MEMORY.ordinal());
    Double usageVirtualMemory = usageStats.get(USAGE_COUNTER_SCHEMA.USED_VIRTUAL_MEMORY.ordinal());
    Double usageHeapMemory = usageStats.get(USAGE_COUNTER_SCHEMA.USED_HEAP_MEMORY.ordinal());
    Double memoryMB =
        applyContainerSizeFormula(constraints.get("CONTAINER_MEMORY"), usagePhysicalMemory, usageVirtualMemory);
    applyHeapSizeFormula(constraints.get("CONTAINER_HEAP"), usageHeapMemory, memoryMB);
  }

  private Double applyContainerSizeFormula(TuningParameterConstraint containerConstraint, Double usagePhysicalMemory,
      Double usageVirtualMemory) {
    Double memoryMB = max(usagePhysicalMemory, usageVirtualMemory / (2.1));
    Double containerSizeLower = getContainerSize(memoryMB);
    Double containerSizeUpper = getContainerSize(1.2 * memoryMB);
    logger.info(" Previous Lower Bound  Memory  " + containerConstraint.lowerBound);
    logger.info(" Previous Upper Bound  Memory " + containerConstraint.upperBound);
    logger.info(" Current Lower Bound  Memory  " + containerSizeLower);
    logger.info(" Current Upper Bound  Memory " + containerSizeUpper);
    containerConstraint.lowerBound = containerSizeLower;
    containerConstraint.upperBound = containerSizeUpper;
    containerConstraint.save();
    return memoryMB;
  }

  private void applyHeapSizeFormula(TuningParameterConstraint containerHeapSizeConstraint, Double usageHeapMemory,
      Double memoryMB) {
    Double heapSizeLowerBound = min(0.75 * memoryMB, usageHeapMemory);
    Double heapSizeUpperBound = heapSizeLowerBound * 1.2;
    logger.info(" Previous Lower Bound  XMX  " + containerHeapSizeConstraint.lowerBound);
    logger.info(" Previous Upper Bound  XMX " + containerHeapSizeConstraint.upperBound);
    logger.info(" Current Lower Bound  XMX  " + heapSizeLowerBound);
    logger.info(" Current Upper Bound  XMX " + heapSizeUpperBound);
    containerHeapSizeConstraint.lowerBound = heapSizeLowerBound;
    containerHeapSizeConstraint.upperBound = heapSizeUpperBound;
    containerHeapSizeConstraint.save();
  }

  private Double getContainerSize(Double memory) {
    return Math.ceil(memory / 1024.0) * 1024;
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
  public int numberOfConstraintsViolated(List<JobSuggestedParamValue> jobSuggestedParamValueList) {
    logger.info(" Constraint Violeted ");
    Double mrSortMemory = null;
    Double mrMapMemory = null;
    Double mrReduceMemory = null;
    Double mrMapXMX = null;
    Double mrReduceXMX = null;
    Double pigMaxCombinedSplitSize = null;
    Integer violations = 0;

    for (JobSuggestedParamValue jobSuggestedParamValue : jobSuggestedParamValueList) {

      if (jobSuggestedParamValue.tuningParameter.paramName.equals("mapreduce.task.io.sort.mb")) {
        mrSortMemory = jobSuggestedParamValue.paramValue;
      } else if (jobSuggestedParamValue.tuningParameter.paramName.equals("mapreduce.map.memory.mb")) {
        mrMapMemory = jobSuggestedParamValue.paramValue;
      } else if (jobSuggestedParamValue.tuningParameter.paramName.equals("mapreduce.reduce.memory.mb")) {
        mrReduceMemory = jobSuggestedParamValue.paramValue;
      } else if (jobSuggestedParamValue.tuningParameter.paramName.equals("mapreduce.map.java.opts")) {
        mrMapXMX = jobSuggestedParamValue.paramValue;
      } else if (jobSuggestedParamValue.tuningParameter.paramName.equals("mapreduce.reduce.java.opts")) {
        mrReduceXMX = jobSuggestedParamValue.paramValue;
      } else if (jobSuggestedParamValue.tuningParameter.paramName.equals("pig.maxCombinedSplitSize")) {
        pigMaxCombinedSplitSize = jobSuggestedParamValue.paramValue / FileUtils.ONE_MB;
      }
    }

    if (mrSortMemory != null && mrMapMemory != null) {
      if (mrSortMemory > 0.6 * mrMapMemory) {
        logger.info("Constraint violated: Sort memory > 60% of map memory");
        violations++;
      }
      if (mrMapMemory - mrSortMemory < 768) {
        logger.info("Constraint violated: Map memory - sort memory < 768 mb");
        violations++;
      }
      if (mrMapXMX > 0.80 * mrMapMemory) {
        logger.info("Constraint violated:  Mapper  XMX > 0.8*mrMapMemory");
        violations++;
      }
      if (mrReduceXMX > 0.80 * mrReduceMemory) {
        logger.info("Constraint violated:  Reducer  XMX > 0.8*mrReducerMemory");
        violations++;
      }
    }

    if (pigMaxCombinedSplitSize != null && mrMapMemory != null && (pigMaxCombinedSplitSize > 1.8 * mrMapMemory)) {
      logger.info("Constraint violated: Pig max combined split size > 1.8 * map memory");
      violations++;
    }
    return violations;
  }

  @Override
  public int getSwarmSize() {
    return 2;
  }
}

