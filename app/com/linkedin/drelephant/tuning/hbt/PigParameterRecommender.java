package com.linkedin.drelephant.tuning.hbt;

import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.tuning.TuningHelper;
import com.linkedin.drelephant.util.MemoryFormatUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import models.AppHeuristicResult;
import models.AppHeuristicResultDetails;
import models.AppResult;
import org.apache.log4j.Logger;

import static com.linkedin.drelephant.mapreduce.heuristics.CommonConstantsHeuristic.*;
import static com.linkedin.drelephant.mapreduce.heuristics.CommonConstantsHeuristic.ParameterKeys.*;
import static com.linkedin.drelephant.mapreduce.heuristics.CommonConstantsHeuristic.UtilizedParameterKeys.MAX_PHYSICAL_MEMORY;
import static com.linkedin.drelephant.mapreduce.heuristics.CommonConstantsHeuristic.UtilizedParameterKeys.MAX_VIRTUAL_MEMORY;
import static com.linkedin.drelephant.mapreduce.heuristics.CommonConstantsHeuristic.UtilizedParameterKeys.*;
import static java.lang.Math.*;


public class PigParameterRecommender {
  private final List<AppResult> appResultList;
  private final Logger logger = Logger.getLogger(getClass());
  private static HashSet<String> heuristicsToTune;
  private Map<String, Double> jobSuggestedParameters = new HashMap();
  private Map<String, String> latestAppliedParams = new HashMap();

  private final double YARN_VMEM_TO_PMEM_RATIO = 2.1;


  public PigParameterRecommender(List<AppResult> appResultList) {
    this.appResultList = appResultList;
  }

  static {
    heuristicsToTune = new HashSet();
    heuristicsToTune.addAll(Arrays.asList(
        MAPPER_MEMORY_HEURISTICS_CONF.getValue(),
        MAPPER_TIME,
        MAPPER_SPILL,
        MAPPER_SPEED,
        REDUCER_MEMORY,
        REDUCER_TIME
    ));
  }

  private void loadLatestAppliedParameters() {
    if (appResultList != null && appResultList.size() > 0) {
      AppResult appResult = appResultList.get(0);
      for (AppHeuristicResult appHeuristicResult : appResult.yarnAppHeuristicResults) {
        if (appHeuristicResult != null && appHeuristicResult.heuristicName.equals(MAPREDUCE_CONFIGURATION)) {
          for (AppHeuristicResultDetails appHeuristicResultDetails : appHeuristicResult.yarnAppHeuristicResultDetails) {
            latestAppliedParams.put(appHeuristicResultDetails.name, appHeuristicResultDetails.value);
          }
        }
      }
    }
  }

  public Map<String, Double> suggestParameters() {
    loadLatestAppliedParameters();
    suggestMemoryParam(MRJobTaskType.MAP);
    suggestMemoryParam(MRJobTaskType.REDUCE);
    List<String> failedHeuristicNameList = getFailedHeuristics();
    for (String failedHeuristicName : failedHeuristicNameList) {
      if (failedHeuristicName.equals(MAPPER_TIME) || failedHeuristicName.equals(MAPPER_SPEED)) {
        suggestSplitSize();
      }
    }
    logger.info("PIG_HBT suggested parameters");
    for (String key : jobSuggestedParameters.keySet()) {
      logger.info("Param suggestion : "  + key + " " + jobSuggestedParameters.get(key));
    }
    return jobSuggestedParameters;
  }
  private void suggestMemoryParam(MRJobTaskType taskType) {
    List<AppHeuristicResult> appHeuristicResultList = new ArrayList();
    String memoryHeuristicName;
    if (taskType.equals(MRJobTaskType.MAP)) {
      memoryHeuristicName = MAPPER_MEMORY_HEURISTICS_CONF.getValue();
    } else {
      memoryHeuristicName = REDUCER_MEMORY_HEURISTICS_CONF.getValue();
    }
    double maxUsedPhysicalMapperMemoryByJob = getHeuristicDetailsMaxValueForJob(memoryHeuristicName,
        MAX_PHYSICAL_MEMORY.getValue());
    double maxUsedHeapMemory = getHeuristicDetailsMaxValueForJob(memoryHeuristicName,
        MAX_TOTAL_COMMITTED_HEAP_USAGE_MEMORY.getValue());
    double maxUsedVirtualMemory = getHeuristicDetailsMaxValueForJob(memoryHeuristicName, MAX_VIRTUAL_MEMORY.getValue());
    double recommendedMemory = max(maxUsedPhysicalMapperMemoryByJob,
        (maxUsedVirtualMemory / YARN_VMEM_TO_PMEM_RATIO));
    double containerSize =  TuningHelper.getContainerSize(recommendedMemory);
    double recommendedHeapMemory = TuningHelper.getHeapSize(min(0.75 * recommendedMemory, maxUsedHeapMemory));
    logger.info("Max Used Physical Heap Virtual Memory for " + taskType + " "  + maxUsedPhysicalMapperMemoryByJob + " "
        + maxUsedHeapMemory+  " " + maxUsedVirtualMemory + " " + (maxUsedVirtualMemory/YARN_VMEM_TO_PMEM_RATIO));
    setSuggestedParameters(taskType, containerSize, recommendedHeapMemory);
  }

  private void setSuggestedParameters(MRJobTaskType taskType, double suggestedContainerMemory, double suggestedHeapMemory) {
    if (taskType.equals(MRJobTaskType.MAP)) {
      setSuggestedMemoryParametersForMapper(suggestedContainerMemory, suggestedHeapMemory);
    } else if (taskType.equals(MRJobTaskType.REDUCE)) {
      setSuggestedMemoryParametersForReducer(suggestedContainerMemory, suggestedHeapMemory);
    }
  }

  private void setSuggestedMemoryParametersForMapper(double suggestedContainerMemory, double suggestedHeapMemory) {
    jobSuggestedParameters.put(MAPPER_MEMORY_HADOOP_CONF.getValue(), suggestedContainerMemory);
    jobSuggestedParameters.put(MAPPER_HEAP_HADOOP_CONF.getValue(), suggestedHeapMemory);
  }

  private void setSuggestedMemoryParametersForReducer(double suggestedContainerMemory, double suggestedHeapMemory) {
    jobSuggestedParameters.put(REDUCER_MEMORY_HADOOP_CONF.getValue(), suggestedContainerMemory);
    jobSuggestedParameters.put(REDUCER_HEAP_HADOOP_CONF.getValue(), suggestedHeapMemory);
  }

  private Double getHeuristicDetailsMaxValueForJob(String heuristicName, String heuristicDetailName) {
    List<AppHeuristicResult> appHeuristicResultList = new ArrayList();
    for (AppResult appResult : appResultList) {
        for (AppHeuristicResult appHeuristicResult : appResult.yarnAppHeuristicResults) {
          if (appHeuristicResult.heuristicName.equals(heuristicName)) {
            appHeuristicResultList.add(appHeuristicResult);
          }
        }
      }
    return getHeuristicDetailsMaxValue(appHeuristicResultList, heuristicDetailName);
  }

  private Double getHeuristicDetailsMaxValue(List<AppHeuristicResult> appHeuristicResultList, String heuristicDetailName) {
    double maxHeuristicDetailValueForJob = 0D;
    if (appHeuristicResultList != null) {
      for (AppHeuristicResult appHeuristicResult : appHeuristicResultList) {
        for (AppHeuristicResultDetails appHeuristicResultDetail : appHeuristicResult.yarnAppHeuristicResultDetails) {
          if (appHeuristicResultDetail.name.equals(heuristicDetailName)) {
            double maxHeuristicDetailValueForApplication = (double) MemoryFormatUtils.stringToBytes(appHeuristicResultDetail.value);
            maxHeuristicDetailValueForJob = Math.max(maxHeuristicDetailValueForJob, maxHeuristicDetailValueForApplication);
          }
        }
      }
      return maxHeuristicDetailValueForJob;

    } else {
      throw new IllegalArgumentException("No heuristic analysis result found for any application");
    }
  }

  private void suggestSplitSize() {
    long maxAvgInputSizeInBytes = 0L;
    int maxAvgRuntimeInSeconds = 0;
    for (AppResult appResult : appResultList) {
      for (AppHeuristicResult appHeuristicResult : appResult.yarnAppHeuristicResults) {
        if (appHeuristicResult.heuristicName.equals(MAPPER_TIME)) {
          for (AppHeuristicResultDetails appHeuristicResultDetail : appHeuristicResult.yarnAppHeuristicResultDetails) {
            if (appHeuristicResultDetail.name.equals(AVG_INPUT_SIZE_IN_BYTES)) {
              logger.info("Average input size in bytes " + appHeuristicResultDetail.value);
              maxAvgInputSizeInBytes = Math.max(maxAvgInputSizeInBytes, Long.parseLong(appHeuristicResultDetail.value));
            } else if (appHeuristicResultDetail.name.equals("Average task runtime")) {
              logger.info("Average runtime " + appHeuristicResultDetail.value);
              maxAvgRuntimeInSeconds = Math.max(maxAvgRuntimeInSeconds , getTimeInSeconds(appHeuristicResultDetail.value));
            }
          }
        }
      }
    }
    logger.info("maxIS maxRT " + maxAvgInputSizeInBytes + " " + maxAvgRuntimeInSeconds);
    long newSplitSizeInBytes = (maxAvgInputSizeInBytes * 10 * 60 / maxAvgRuntimeInSeconds);
    logger.info("Split size suggested " + newSplitSizeInBytes);
    jobSuggestedParameters.put(PIG_SPLIT_SIZE_HADOOP_CONF.getValue(), newSplitSizeInBytes * 1.0);
    jobSuggestedParameters.put(SPLIT_SIZE_HADOOP_CONF.getValue(), newSplitSizeInBytes * 1.0);
  }

  private void suggestParametersForMemorySpill() {
    int suggestedBufferSize = 0;
    double suggestedSpillPercentage = 0.0f;
    double mapperSpillRatio = getHeuristicDetailsMaxValueForJob(MAPPER_SPILL,
        MAPPER_OUTPUT_RECORD_SPILL_RATIO);
    int latestExecutionSortBufferConfig = Integer.parseInt(latestAppliedParams.get(SORT_BUFFER_HADOOP_CONF.getValue()));
    double latestExecutionSortThresholdConfig = Double.parseDouble(latestAppliedParams.get(SORT_SPILL_HADOOP_CONF.getValue()));
    if (mapperSpillRatio >= 2.7) {
      if (latestExecutionSortBufferConfig <= 0.85) {
        suggestedSpillPercentage = latestExecutionSortThresholdConfig + 0.05f;
        suggestedBufferSize = (int) (latestExecutionSortBufferConfig * 1.2);
      } else {
        suggestedBufferSize = (int) (latestExecutionSortBufferConfig * 1.3);
      }
    } else if (mapperSpillRatio >= 2.2) {
      if (latestExecutionSortThresholdConfig <= 0.85) {
        suggestedSpillPercentage = latestExecutionSortThresholdConfig + 0.05f;
        suggestedBufferSize = (int) (latestExecutionSortBufferConfig * 1.1);
      } else {
        suggestedBufferSize = (int) (latestExecutionSortBufferConfig * 1.2);
      }
    }
    jobSuggestedParameters.put(SORT_BUFFER_HADOOP_CONF.getValue(), suggestedBufferSize * 1.0);
    jobSuggestedParameters.put(SORT_SPILL_HADOOP_CONF.getValue(), suggestedSpillPercentage * 1.0);
    modifyMapperMemory();
  }

  private void modifyMapperMemory() {
    double currentMapperMemory = jobSuggestedParameters.get(MAPPER_MEMORY_HADOOP_CONF.getValue()) == null ? Double.parseDouble(
        latestAppliedParams.get(MAPPER_MEMORY_HEURISTICS_CONF.getValue())) : jobSuggestedParameters.get(MAPPER_MEMORY_HADOOP_CONF.getValue());
    Double sortBuffer = jobSuggestedParameters.get(SORT_BUFFER_HADOOP_CONF.getValue());
    Double minPhysicalMemoryRequired = max(sortBuffer + 769, sortBuffer * (10 / 6));
    if (minPhysicalMemoryRequired > currentMapperMemory) {
      currentMapperMemory = minPhysicalMemoryRequired;
      jobSuggestedParameters.put(MAPPER_MEMORY_HADOOP_CONF.getValue(), TuningHelper.getContainerSize(currentMapperMemory));
      Double heapMemory = jobSuggestedParameters.get(MAPPER_HEAP_HADOOP_CONF.getValue());
      heapMemory = (heapMemory == null) ? (0.75 * currentMapperMemory) : min(0.75 * currentMapperMemory, heapMemory);
      jobSuggestedParameters.put(MAPPER_HEAP_HADOOP_CONF.getValue(), heapMemory);
    }
  }

  private int getTimeInSeconds(String value) {
    value = value.replaceAll(" ", "");
    String[] timeSplit = value.split("hr|min|sec");
    int timeInSeconds = 0;
    if (timeSplit.length == 3) {
      timeInSeconds = timeInSeconds + Integer.parseInt(timeSplit[0]) * 60 * 60;
      timeInSeconds = timeInSeconds + Integer.parseInt(timeSplit[1]) * 60;
      timeInSeconds = timeInSeconds + Integer.parseInt(timeSplit[2]);
    } else if (timeSplit.length == 2) {
      timeInSeconds = timeInSeconds + Integer.parseInt(timeSplit[0]) * 60;
      timeInSeconds = timeInSeconds + Integer.parseInt(timeSplit[1]);
    } else if (timeSplit.length == 1) {
      timeInSeconds = timeInSeconds + Integer.parseInt(timeSplit[0]);
    }
    logger.info("Time in seconds " + timeInSeconds);
    return timeInSeconds;
  }

  private List<String> getFailedHeuristics() {
    List<String> failedHeuristicNameList = new ArrayList();
    for (AppResult appResult : appResultList) {
      for (AppHeuristicResult appHeuristicResult :  appResult.yarnAppHeuristicResults) {
        logger.info("Heuristics name : " + appHeuristicResult.heuristicName);
        if (heuristicsToTune.contains(appHeuristicResult.heuristicName) &&
            !failedHeuristicNameList.contains(appHeuristicResult.heuristicName) &&
            appHeuristicResult.severity.getValue() >= Severity.MODERATE.getValue()) {
          logger.info("Failed heuristic " + appHeuristicResult.heuristicName);
          failedHeuristicNameList.add(appHeuristicResult.heuristicName);
        }
      }
    }
    return failedHeuristicNameList;
  }
}
