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
  Map<String, Double> usageDataGlobal = null;

  enum USAGE_COUNTER_SCHEMA {USED_PHYSICAL_MEMORY, USED_VIRTUAL_MEMORY, USED_HEAP_MEMORY}

  @Override
  public void intializePrerequisite(JobSuggestedParamSet jobSuggestedParamSet) {
    logger.info(" Intialize Prerequisite");
    setDefaultParameterValues(jobSuggestedParamSet);
    changeDerivedConstraint();
  }

  private void setDefaultParameterValues(JobSuggestedParamSet jobSuggestedParamSet) {
    for (PARAMTER_CONSTRAINT value : PARAMTER_CONSTRAINT.values()) {
      TuningParameter tuningParameter =
          TuningParameter.find.where().eq(TuningParameter.TABLE.paramName, value.getValue()).findUnique();
      TuningParameterConstraint tuningParameterConstraint = new TuningParameterConstraint();
      tuningParameterConstraint.jobDefinition = jobSuggestedParamSet.jobDefinition;
      tuningParameterConstraint.constraintId = value.getConstraintID();
      tuningParameterConstraint.lowerBound = value.getLowerBound();
      tuningParameterConstraint.upperBound = value.getUpperBound();
      tuningParameterConstraint.constraintType = TuningParameterConstraint.ConstraintType.BOUNDARY;
      tuningParameterConstraint.tuningParameter = tuningParameter;
      tuningParameterConstraint.save();
    }
  }

  private void changeDerivedConstraint() {
    for (NON_DERIVED_PARAMETER value : NON_DERIVED_PARAMETER.values()) {
      TuningParameter tuningParameter =
          TuningParameter.find.where().eq(TuningParameter.TABLE.paramName, value.getValue()).findUnique();
      tuningParameter.isDerived = 0;
      tuningParameter.update();
    }
  }

  @Override
  public void extractParameterInformation(List<AppResult> appResults) {
    logger.info(" Extract Parameter Information");
    usageDataGlobal = new HashMap<String, Double>();
    intialize(usageDataGlobal);
    for (AppResult appResult : appResults) {
      Map<String, Double> usageDataApplication = collectUsageDataPerApplication(appResult);
      for (String keys : usageDataApplication.keySet()) {
        usageDataGlobal.put(keys, max(usageDataGlobal.get(keys), usageDataApplication.get(keys)));
      }
    }
    logger.info("Usage Values maximum   ");
    for (String keys : usageDataGlobal.keySet()) {
      logger.info(keys + " " + usageDataGlobal.get(keys));
    }
  }

  private void intialize(Map<String, Double> usageDataGlobal) {
    for (UTILIZED_PARAMETER_KEYS value : UTILIZED_PARAMETER_KEYS.values()) {
      usageDataGlobal.put(value.getValue(), 0.0);
    }
  }

  private Map<String, Double> collectUsageDataPerApplication(AppResult appResult) {
    Map<String, Double> counterData = new HashMap<String, Double>();
    if (appResult.yarnAppHeuristicResults != null) {
      for (AppHeuristicResult appHeuristicResult : appResult.yarnAppHeuristicResults) {
        if (appHeuristicResult.heuristicName.equals(
            CommnConstantsMRAutoTuningIPSOHeuristics.AUTO_TUNING_IPSO_HEURISTICS)) {
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
      }
    }
    logger.info("Usage Values   " + appResult.jobExecUrl);
    for (String counterNames : counterData.keySet()) {
      logger.info(counterNames + " " + counterData.get(counterNames));
    }
    return counterData;
  }

  @Override
  public void parameterOptimizer(Integer id) {
    logger.info(" IPSO Optimizer");
    if (usageDataGlobal.get(UTILIZED_PARAMETER_KEYS.MAX_MAP_PHYSICAL_MEMORY_BYTES.getValue()) > 0.0) {
      logger.info(" Optimizing Mapper Parameter Space ");
      List<Double> usageStats = extractUsageParameter("map");
      List<Integer> mapContraintIDs = new ArrayList<Integer>();
      mapContraintIDs.add(PARAMTER_CONSTRAINT.MAPPER_MEMORY.getConstraintID());
      mapContraintIDs.add(PARAMTER_CONSTRAINT.MAPPER_HEAP_MEMORY.getConstraintID());
      memoryParameterIPSO("map", mapContraintIDs, usageStats, id);
    }
    if (usageDataGlobal.get(UTILIZED_PARAMETER_KEYS.MAX_REDUCE_PHYSICAL_MEMORY_BYTES.getValue()) > 0.0) {
      logger.info(" Optimizing Reducer Parameter Space ");
      List<Double> usageStats = extractUsageParameter("reduce");
      List<Integer> reduceContraintIDs = new ArrayList<Integer>();
      reduceContraintIDs.add(PARAMTER_CONSTRAINT.REDUCER_MEMORY.getConstraintID());
      reduceContraintIDs.add(PARAMTER_CONSTRAINT.REDUCER_HEAP_MEMORY.getConstraintID());
      memoryParameterIPSO("reduce", reduceContraintIDs, usageStats, id);
    }
  }

  private List<Double> extractUsageParameter(String trigger) {
    Double usedPhysicalMemoryMB = 0.0, usedVirtualMemoryMB = 0.0, usedHeapMemoryMB = 0.0;
    if (trigger.equals("map")) {
      usedPhysicalMemoryMB =
          usageDataGlobal.get(UTILIZED_PARAMETER_KEYS.MAX_MAP_PHYSICAL_MEMORY_BYTES.getValue()) / FileUtils.ONE_MB;
      usedVirtualMemoryMB =
          usageDataGlobal.get(UTILIZED_PARAMETER_KEYS.MAX_MAP_VIRTUAL_MEMORY_BYTES.getValue()) / FileUtils.ONE_MB;
      usedHeapMemoryMB = usageDataGlobal.get(UTILIZED_PARAMETER_KEYS.MAX_MAP_TOTAL_COMMITTED_MEMORY_BYTES.getValue())
          / FileUtils.ONE_MB;
    } else if (trigger.equals("reduce")) {
      usedPhysicalMemoryMB =
          usageDataGlobal.get(UTILIZED_PARAMETER_KEYS.MAX_REDUCE_PHYSICAL_MEMORY_BYTES.getValue()) / FileUtils.ONE_MB;
      usedVirtualMemoryMB =
          usageDataGlobal.get(UTILIZED_PARAMETER_KEYS.MAX_REDUCE_VIRTUAL_MEMORY_BYTES.getValue()) / FileUtils.ONE_MB;
      usedHeapMemoryMB = usageDataGlobal.get(UTILIZED_PARAMETER_KEYS.MAX_REDUCE_TOTAL_COMMITTED_MEMORY_BYTES.getValue())
          / FileUtils.ONE_MB;
    }
    logger.info(" Usage Stats " + trigger);
    logger.info(" Physical Memory Usage MB " + usedPhysicalMemoryMB);
    logger.info(" Virtual Memory Usage MB " + usedVirtualMemoryMB / 2.1);
    logger.info(" Heap Usage MB " + usedHeapMemoryMB);
    List<Double> usageStats = new ArrayList<Double>();
    usageStats.add(usedPhysicalMemoryMB);
    usageStats.add(usedVirtualMemoryMB);
    usageStats.add(usedHeapMemoryMB);
    return usageStats;
  }

  private void memoryParameterIPSO(String trigger, List<Integer> constraintIDs, List<Double> usageStats,
      Integer jobID) {
    logger.info(" IPSO for " + trigger);
    TuningParameterConstraint containerConstraint = TuningParameterConstraint.find.where().
        eq(TuningParameterConstraint.TABLE.constraintId, constraintIDs.get(0)).
        eq("job_definition_id", jobID).
        findUnique();
    TuningParameterConstraint containerHeapSizeConstraint = TuningParameterConstraint.find.where().
        eq(TuningParameterConstraint.TABLE.constraintId, constraintIDs.get(1)).
        eq("job_definition_id", jobID).
        findUnique();
    Double usagePhysicalMemory = usageStats.get(USAGE_COUNTER_SCHEMA.USED_PHYSICAL_MEMORY.ordinal());
    Double usageVirtualMemory = usageStats.get(USAGE_COUNTER_SCHEMA.USED_VIRTUAL_MEMORY.ordinal());
    Double usageHeapMemory = usageStats.get(USAGE_COUNTER_SCHEMA.USED_HEAP_MEMORY.ordinal());
    Double memoryMB = applyContainerSizeFormula(containerConstraint, usagePhysicalMemory, usageVirtualMemory);
    applyHeapSizeFormula(containerHeapSizeConstraint, usageHeapMemory, memoryMB);
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
}

