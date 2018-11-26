package com.linkedin.drelephant.tuning.obt;

import com.linkedin.drelephant.mapreduce.heuristics.CommonConstantsHeuristic;
import com.linkedin.drelephant.tuning.ExecutionEngine;
import com.linkedin.drelephant.tuning.TuningHelper;
import com.linkedin.drelephant.tuning.engine.MRExecutionEngine;
import com.linkedin.drelephant.util.MemoryFormatUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import models.AppHeuristicResult;
import models.AppHeuristicResultDetails;
import models.AppResult;
import models.JobExecution;
import models.JobSuggestedParamSet;
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import models.TuningParameter;
import models.TuningParameterConstraint;
import org.apache.log4j.Logger;
import org.apache.commons.io.FileUtils;

import static com.linkedin.drelephant.tuning.TuningHelper.*;
import static java.lang.Math.*;


public class ParameterGenerateManagerOBTAlgoPSOIPSOMRImpl<T extends MRExecutionEngine> extends ParameterGenerateManagerOBTAlgoPSOIPSOImpl<T> {
  private final Logger logger = Logger.getLogger(getClass());
  final static int MAX_MAP_MEM_SORT_MEM_DIFF = 768;
  T mrExecutionEngine;

  public ParameterGenerateManagerOBTAlgoPSOIPSOMRImpl(T mrExecutionEngine) {
    this.mrExecutionEngine=mrExecutionEngine;
  }

  @Override
  protected List<JobSuggestedParamSet> getPendingParamSets() {
    List<JobSuggestedParamSet> pendingParamSetList = mrExecutionEngine.getPendingJobs()
        .eq(JobSuggestedParamSet.TABLE.tuningAlgorithm + "." + TuningAlgorithm.TABLE.optimizationAlgo,
            TuningAlgorithm.OptimizationAlgo.PSO_IPSO.name())
        .eq(JobSuggestedParamSet.TABLE.isParamSetDefault, 0)
        .findList();
    return pendingParamSetList;
  }

  @Override
  protected List<TuningJobDefinition> getTuningJobDefinitions() {
    return mrExecutionEngine.getTuningJobDefinitionsForParameterSuggestion()
        .eq(TuningJobDefinition.TABLE.tuningAlgorithm + "." + TuningAlgorithm.TABLE.optimizationAlgo,
            TuningAlgorithm.OptimizationAlgo.PSO_IPSO.name())
        .findList();
  }

  @Override
  public boolean isParamConstraintViolated(List<JobSuggestedParamValue> jobSuggestedParamValueList) {
    logger.info(" Constraint Violeted ");
    Double mrSortMemory = null;
    Double mrMapMemory = null;
    Double mrReduceMemory = null;
    Double mrMapXMX = null;
    Double mrReduceXMX = null;
    Double pigMaxCombinedSplitSize = null;
    Integer violations = 0;

    for (JobSuggestedParamValue jobSuggestedParamValue : jobSuggestedParamValueList) {
      if (jobSuggestedParamValue.tuningParameter.paramName.equals(
          CommonConstantsHeuristic.ParameterKeys.SORT_BUFFER_HADOOP_CONF.getValue())) {
        mrSortMemory = jobSuggestedParamValue.paramValue;
      } else if (jobSuggestedParamValue.tuningParameter.paramName.equals(
          CommonConstantsHeuristic.ParameterKeys.MAPPER_MEMORY_HADOOP_CONF.getValue())) {
        mrMapMemory = jobSuggestedParamValue.paramValue;
      } else if (jobSuggestedParamValue.tuningParameter.paramName.equals(
          CommonConstantsHeuristic.ParameterKeys.REDUCER_MEMORY_HADOOP_CONF.getValue())) {
        mrReduceMemory = jobSuggestedParamValue.paramValue;
      } else if (jobSuggestedParamValue.tuningParameter.paramName.equals(
          CommonConstantsHeuristic.ParameterKeys.MAPPER_HEAP_HADOOP_CONF.getValue())) {
        mrMapXMX = jobSuggestedParamValue.paramValue;
      } else if (jobSuggestedParamValue.tuningParameter.paramName.equals(
          CommonConstantsHeuristic.ParameterKeys.REDUCER_HEAP_HADOOP_CONF.getValue())) {
        mrReduceXMX = jobSuggestedParamValue.paramValue;
      } else if (jobSuggestedParamValue.tuningParameter.paramName.equals(
          CommonConstantsHeuristic.ParameterKeys.PIG_SPLIT_SIZE_HADOOP_CONF.getValue())) {
        pigMaxCombinedSplitSize = jobSuggestedParamValue.paramValue / FileUtils.ONE_MB;
      }
    }

    if (mrSortMemory != null && mrMapMemory != null) {
      if (mrSortMemory > 0.6 * mrMapMemory) {
        if (debugEnabled) {
          logger.debug("Sort Memory " + mrSortMemory);
          logger.debug("Mapper Memory " + mrMapMemory);
          logger.debug("Constraint violated: Sort memory > 60% of map memory");
        }
        violations++;
      }
      if (mrMapMemory - mrSortMemory < MAX_MAP_MEM_SORT_MEM_DIFF) {
        if (debugEnabled) {
          logger.debug("Sort Memory " + mrSortMemory);
          logger.debug("Mapper Memory " + mrMapMemory);
          logger.debug("Constraint violated: Map memory - sort memory < 768 mb");
        }
        violations++;
      }
    }
    if (mrMapXMX != null && mrMapMemory != null && mrMapXMX > 0.80 * mrMapMemory) {
      if (debugEnabled) {
        logger.debug("Mapper Heap Max " + mrMapXMX);
        logger.debug("Mapper Memory " + mrMapMemory);
        logger.debug("Constraint violated:  Mapper  XMX > 0.8*mrMapMemory");
      }
      violations++;
    }
    if (mrReduceMemory != null && mrReduceXMX != null && mrReduceXMX > 0.80 * mrReduceMemory) {
      if (debugEnabled) {
        logger.debug("Reducer Heap Max " + mrMapXMX);
        logger.debug("Reducer Memory " + mrMapMemory);
        logger.debug("Constraint violated:  Reducer  XMX > 0.8*mrReducerMemory");
      }
      violations++;
    }

    if (pigMaxCombinedSplitSize != null && mrMapMemory != null && (pigMaxCombinedSplitSize > 1.8 * mrMapMemory)) {
      if (debugEnabled) {
        logger.debug("Constraint violated: Pig max combined split size > 1.8 * map memory");
      }
      violations++;
    }
    if (violations == 0) {
      return false;
    } else {
      logger.info("Number of constraint(s) violated: " + violations);
      return true;
    }
  }

  @Override
  public void parameterOptimizer(List<AppResult> results, JobExecution jobExecution) {
    Map<String, Map<String, Double>> previousUsedMetrics = extractParameterInformation(results);
    List<TuningParameterConstraint> parameterConstraints = TuningParameterConstraint.find.where().
        eq("job_definition_id", jobExecution.job.id).findList();
    for (String function : mrExecutionEngine.functionTypes) {
      logger.info(" Optimizing Parameter Space  " + function);
      if (previousUsedMetrics.get(function)
          .get(CommonConstantsHeuristic.UtilizedParameterKeys.MAX_PHYSICAL_MEMORY.getValue()) > 0.0) {
        List<Double> usageStats = mrExecutionEngine.extractUsageParameter(function, previousUsedMetrics);
        Map<String, TuningParameterConstraint> memoryConstraints =
            filterMemoryConstraint(parameterConstraints, function);
        memoryParameter(function, memoryConstraints, usageStats);
      }
    }
  }

  private Map<String, TuningParameterConstraint> filterMemoryConstraint(
      List<TuningParameterConstraint> parameterConstraints, String functionType) {
    Map<String, TuningParameterConstraint> memoryConstraints = new HashMap<String, TuningParameterConstraint>();
    for (TuningParameterConstraint parameterConstraint : parameterConstraints) {
      if (functionType.equals("map")) {
        if (parameterConstraint.tuningParameter.paramName.equals(
            CommonConstantsHeuristic.ParameterKeys.MAPPER_MEMORY_HADOOP_CONF.getValue())) {
          memoryConstraints.put("CONTAINER_MEMORY", parameterConstraint);
        }
        if (parameterConstraint.tuningParameter.paramName.equals(
            CommonConstantsHeuristic.ParameterKeys.MAPPER_HEAP_HADOOP_CONF.getValue())) {
          memoryConstraints.put("CONTAINER_HEAP", parameterConstraint);
        }
      }
      if (functionType.equals("reduce")) {
        if (parameterConstraint.tuningParameter.paramName.equals(
            CommonConstantsHeuristic.ParameterKeys.REDUCER_MEMORY_HADOOP_CONF.getValue())) {
          memoryConstraints.put("CONTAINER_MEMORY", parameterConstraint);
        }
        if (parameterConstraint.tuningParameter.paramName.equals(
            CommonConstantsHeuristic.ParameterKeys.REDUCER_HEAP_HADOOP_CONF.getValue())) {
          memoryConstraints.put("CONTAINER_HEAP", parameterConstraint);
        }
      }
    }
    return memoryConstraints;
  }

  private Map<String, Map<String, Double>> extractParameterInformation(List<AppResult> appResults) {
    logger.info(" Extract Parameter Information for MR IPSO");
    return mrExecutionEngine.extractParameterInformation(appResults);
  }

  private void memoryParameter(String trigger, Map<String, TuningParameterConstraint> constraints,
      List<Double> usageStats) {
    logger.info(" IPSO for " + trigger);
    Double usagePhysicalMemory = usageStats.get(MRExecutionEngine.UsageCounterSchema.USED_PHYSICAL_MEMORY.ordinal());
    Double usageVirtualMemory = usageStats.get(MRExecutionEngine.UsageCounterSchema.USED_VIRTUAL_MEMORY.ordinal());
    Double usageHeapMemory = usageStats.get(MRExecutionEngine.UsageCounterSchema.USED_HEAP_MEMORY.ordinal());
    Double memoryMB =
        applyContainerSizeFormula(constraints.get("CONTAINER_MEMORY"), usagePhysicalMemory, usageVirtualMemory);
    applyHeapSizeFormula(constraints.get("CONTAINER_HEAP"), usageHeapMemory, memoryMB);
  }

  private Double applyContainerSizeFormula(TuningParameterConstraint containerConstraint, Double usagePhysicalMemory,
      Double usageVirtualMemory) {
    Double memoryMB = max(usagePhysicalMemory, usageVirtualMemory / (2.1));
    Double containerSizeLower = TuningHelper.getContainerSize(memoryMB);
    Double containerSizeUpper = TuningHelper.getContainerSize(1.2 * memoryMB);
    if (debugEnabled) {
      logger.debug(" Previous Lower Bound  Memory  " + containerConstraint.lowerBound);
      logger.debug(" Previous Upper Bound  Memory " + containerConstraint.upperBound);
      logger.debug(" Current Lower Bound  Memory  " + containerSizeLower);
      logger.debug(" Current Upper Bound  Memory " + containerSizeUpper);
    }
    containerConstraint.lowerBound = containerSizeLower;
    containerConstraint.upperBound = containerSizeUpper;
    containerConstraint.save();
    return memoryMB;
  }

  private void applyHeapSizeFormula(TuningParameterConstraint containerHeapSizeConstraint, Double usageHeapMemory,
      Double memoryMB) {
    Double heapSizeLowerBound = min(0.75 * memoryMB, usageHeapMemory);
    Double heapSizeUpperBound = heapSizeLowerBound * 1.2;
    if (debugEnabled) {
      logger.debug(" Previous Lower Bound  XMX  " + containerHeapSizeConstraint.lowerBound);
      logger.debug(" Previous Upper Bound  XMX " + containerHeapSizeConstraint.upperBound);
      logger.debug(" Current Lower Bound  XMX  " + heapSizeLowerBound);
      logger.debug(" Current Upper Bound  XMX " + heapSizeUpperBound);
    }
    containerHeapSizeConstraint.lowerBound = heapSizeLowerBound;
    containerHeapSizeConstraint.upperBound = heapSizeUpperBound;
    containerHeapSizeConstraint.save();
  }

  @Override
  public void computeValuesOfDerivedConfigurationParameters(List<TuningParameter> derivedParameterList,
      List<JobSuggestedParamValue> jobSuggestedParamValueList) {
    mrExecutionEngine.computeValuesOfDerivedConfigurationParameters(derivedParameterList,jobSuggestedParamValueList);
  }
}
