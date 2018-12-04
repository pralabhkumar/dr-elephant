package com.linkedin.drelephant.tuning.hbt;

import com.linkedin.drelephant.tuning.TuningHelper;
import com.linkedin.drelephant.util.MemoryFormatUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import models.AppHeuristicResult;
import models.AppHeuristicResultDetails;
import models.AppResult;
import org.apache.log4j.Logger;

import static java.lang.Math.*;


public class MRApplicationData {
  private final Logger logger = Logger.getLogger(getClass());
  boolean debugEnabled = logger.isDebugEnabled();
  private String applicationID;
  private Map<String, Double> suggestedParameter;
  private AppResult _result;
  Map<String, AppHeuristicResult> failedHeuristics = null;
  private static Set<String> validHeuristic = null;
  private Map<String, String> appliedParameter = null;
  private boolean processForMapperMemory = false;
  private Map<String, Double> counterValues = null;

  static {
    validHeuristic = new HashSet<String>();
    //validHeuristic.add("Mapper GC");
    validHeuristic.add("Mapper Time");
    validHeuristic.add("Mapper Speed");
    validHeuristic.add("Mapper Memory");
    // validHeuristic.add("Reducer GC");
    validHeuristic.add("Reducer Time");
    validHeuristic.add("Reducer Memory");
  }

  MRApplicationData(AppResult result, Map<String, String> appliedParameter) {
    this.applicationID = result.id;
    this._result = result;
    this.suggestedParameter = new HashMap<String, Double>();
    this.failedHeuristics = new HashMap<String, AppHeuristicResult>();
    this.appliedParameter = appliedParameter;
    this.counterValues = new HashMap<String, Double>();
    processForSuggestedParameter();
  }

  public Map<String, Double> getCounterValues() {
    return this.counterValues;
  }

  public String getApplicationID() {
    return this.applicationID;
  }

  private void processForSuggestedParameter() {
    if (_result.yarnAppHeuristicResults != null) {
      for (AppHeuristicResult yarnAppHeuristicResult : _result.yarnAppHeuristicResults) {
        if (isValidHeuristic(yarnAppHeuristicResult)) {
          processHeuristics(yarnAppHeuristicResult);
          failedHeuristics.put(yarnAppHeuristicResult.heuristicName, yarnAppHeuristicResult);
        }
      }
    }
    if (failedHeuristics.size() == 0) {
      optimizeForResourceUsage();
    }
  }

  public Map<String, Double> getSuggestedParameter() {
    return this.suggestedParameter;
  }

  private boolean isValidHeuristic(AppHeuristicResult yarnAppHeuristicResult) {
    if (validHeuristic.contains(yarnAppHeuristicResult.heuristicName)
        && yarnAppHeuristicResult.severity.getValue() > 2) {
      return true;
    }
    return false;
  }

  private void processHeuristics(AppHeuristicResult yarnAppHeuristicResult) {
    if (yarnAppHeuristicResult.heuristicName.equals("Mapper Memory")) {
      processForMemory(yarnAppHeuristicResult, "Mapper");
      processForMapperMemory = true;
    } else if (yarnAppHeuristicResult.heuristicName.equals("Reducer Memory")) {
      processForMemory(yarnAppHeuristicResult, "Reducer");
    } else if (yarnAppHeuristicResult.heuristicName.equals("Mapper Time")) {
      processForNumberOfTask(yarnAppHeuristicResult, "Mapper");
    } else if (yarnAppHeuristicResult.heuristicName.equals("Reducer Time")) {
      processForNumberOfTask(yarnAppHeuristicResult, "Reducer");
    } else if (yarnAppHeuristicResult.heuristicName.equals("Mapper Spill")) {
      processForMemoryBuffer(yarnAppHeuristicResult);
    }
  }

  private void processForMemory(AppHeuristicResult yarnAppHeuristicResult, String functionType) {
    Double usedPhysicalMemoryMB = 0.0, usedVirtualMemoryMB = 0.0, usedHeapMemoryMB = 0.0;
    for (AppHeuristicResultDetails appHeuristicResultDetails : yarnAppHeuristicResult.yarnAppHeuristicResultDetails) {
      if (appHeuristicResultDetails.name.equals("Max Virtual Memory (MB)")) {
        usedVirtualMemoryMB = (double) MemoryFormatUtils.stringToBytes(appHeuristicResultDetails.value);
      }
      if (appHeuristicResultDetails.name.equals("Max Physical Memory (MB)")) {
        usedPhysicalMemoryMB = (double) MemoryFormatUtils.stringToBytes(appHeuristicResultDetails.value);
      }
      if (appHeuristicResultDetails.name.equals("Max Total Committed Heap Usage Memory (MB)")) {
        usedHeapMemoryMB = (double) MemoryFormatUtils.stringToBytes(appHeuristicResultDetails.value);
      }
    }
    counterValues.put(functionType + " Max Virtual Memory (MB)", usedVirtualMemoryMB);
    counterValues.put(functionType + " Max Physical Memory (MB)", usedPhysicalMemoryMB);
    counterValues.put(functionType + " Max Total Committed Heap Usage Memory (MB)", usedHeapMemoryMB);

    if (debugEnabled) {
      logger.debug(
          " Used Physical Memory " + yarnAppHeuristicResult.id + "_" + functionType + " " + usedPhysicalMemoryMB);
      logger.debug(
          " Used Virtual Memory " + yarnAppHeuristicResult.id + "_" + functionType + " " + usedVirtualMemoryMB);
      logger.debug(" Used heap Memory " + yarnAppHeuristicResult.id + "_" + functionType + " " + usedHeapMemoryMB);
    }
    Double memoryMB = max(usedPhysicalMemoryMB, usedVirtualMemoryMB / (2.1));
    Double heapSizeMax = TuningHelper.getHeapSize(min(0.75 * memoryMB, usedHeapMemoryMB));
    Double containerSize = TuningHelper.getContainerSize(memoryMB);
    addParameterToSuggestedParameter(heapSizeMax, containerSize, yarnAppHeuristicResult.id, functionType);
  }

  private void addParameterToSuggestedParameter(Double heapSizeMax, Double containerSize, int id, String functionType) {
    if (functionType.equals("Mapper")) {
      addMapperMemoryAndHeapToSuggestedParameter(heapSizeMax, containerSize, id);
    } else {
      addReducerMemoryAndHeapToSuggestedParameter(heapSizeMax, containerSize, id);
    }
  }

  private void addMapperMemoryAndHeapToSuggestedParameter(Double heapSizeMax, Double containerSize,
      int heuristicsResultID) {
    suggestedParameter.put("mapreduce.map.memory.mb", containerSize);
    suggestedParameter.put("mapreduce.map.java.opts", heapSizeMax);
    if (debugEnabled) {
      logger.debug(
          " Memory Assigned " + heuristicsResultID + "_Mapper " + suggestedParameter.get("mapreduce.map.memory.mb"));
      logger.debug(
          " Heap Assigned " + heuristicsResultID + "_Mapper " + suggestedParameter.get("mapreduce.map.java.opts"));
    }
  }

  private void addReducerMemoryAndHeapToSuggestedParameter(Double heapSizeMax, Double containerSize,
      int heuristicsResultID) {
    suggestedParameter.put("mapreduce.reduce.memory.mb", containerSize);
    suggestedParameter.put("mapreduce.reduce.java.opts", heapSizeMax);
    if (debugEnabled) {
      logger.debug(
          " Memory Assigned " + heuristicsResultID + "_Reducer " + suggestedParameter.get("mapreduce.map.memory.mb"));
      logger.debug(
          " Heap Assigned " + heuristicsResultID + "_Reducer " + suggestedParameter.get("mapreduce.map.java.opts"));
    }
  }

  private void processForNumberOfTask(AppHeuristicResult yarnAppHeuristicResult, String functionType) {
    long splitSize = 0l;
    long numberOfReduceTask = 0l;
    if (functionType.equals("Mapper")) {
      splitSize = getNewSplitSize(yarnAppHeuristicResult);
      if (splitSize > 0) {
        suggestedParameter.put("pig.maxCombinedSplitSize", splitSize * 1.0);
      }
    }
    if (functionType.equals("Reducer")) {
      numberOfReduceTask = getNumberOfReducer(yarnAppHeuristicResult);
      if (numberOfReduceTask > 0) {
        suggestedParameter.put("mapreduce.job.reduces", numberOfReduceTask * 1.0);
      }
    }
  }

  private long getNewSplitSize(AppHeuristicResult yarnAppHeuristicResult) {
    double averageTaskInputSize = 0.0;
    double averageTaskTimeInMinute = 0.0;
    //long blockSize = 536870912l;
    long newSplitSize = 0l;
    for (AppHeuristicResultDetails appHeuristicResultDetails : yarnAppHeuristicResult.yarnAppHeuristicResultDetails) {
      if (appHeuristicResultDetails.name.equals("Average task input size")) {
        averageTaskInputSize = (double) MemoryFormatUtils.stringToBytes(appHeuristicResultDetails.value);
      }
      if (appHeuristicResultDetails.name.equals("Average task input size")) {
        averageTaskTimeInMinute = getTimeInMinute(appHeuristicResultDetails.value);
      }
    }
    if (averageTaskTimeInMinute <= 1.0) {
      newSplitSize = (long) averageTaskInputSize * 2;
    } else if (averageTaskTimeInMinute <= 2.0) {
      newSplitSize = (long) (averageTaskInputSize * 1.2);
    } else if (averageTaskTimeInMinute >= 120) {
      newSplitSize = (long) (averageTaskInputSize / 2);
    } else if (averageTaskTimeInMinute >= 60) {
      newSplitSize = (long) (averageTaskInputSize * 0.8);
    }
    return newSplitSize;
  }

  private long getNumberOfReducer(AppHeuristicResult yarnAppHeuristicResult) {
    int numberoOfTasks = 0;
    double averageTaskTimeInMinute = 0.0;
    int newNumberOfReducer = 0;
    for (AppHeuristicResultDetails appHeuristicResultDetails : yarnAppHeuristicResult.yarnAppHeuristicResultDetails) {
      if (appHeuristicResultDetails.name.equals("Average task time")) {
        averageTaskTimeInMinute = getTimeInMinute(appHeuristicResultDetails.value);
      }
      if (appHeuristicResultDetails.name.equals("Number of tasks")) {
        numberoOfTasks = Integer.parseInt(appHeuristicResultDetails.value);
      }
    }
    if (averageTaskTimeInMinute <= 1.0) {
      newNumberOfReducer = numberoOfTasks / 2;
    } else if (averageTaskTimeInMinute <= 2.0) {
      newNumberOfReducer = (int) (numberoOfTasks * 0.8);
    } else if (averageTaskTimeInMinute >= 120) {
      newNumberOfReducer = numberoOfTasks * 2;
    } else if (averageTaskTimeInMinute >= 60) {
      newNumberOfReducer = (int) (newNumberOfReducer * 1.2);
    }
    return newNumberOfReducer;
  }

  private double getTimeInMinute(String value) {
    String split[] = value.split(" ");
    double timeInMinutes = 0.0;
    for (String data : split) {
      if (data.contains("hr")) {
        data = data.replaceAll("hr", "");
        timeInMinutes = timeInMinutes + Integer.parseInt(data) * 60;
      }
      if (data.contains("min")) {
        data = data.replaceAll("min", "");
        timeInMinutes = timeInMinutes + Integer.parseInt(data);
      }
      if (data.contains("sec")) {
        data = data.replaceAll("sec", "");
        timeInMinutes = timeInMinutes + Integer.parseInt(data) * 1.0 / 60 * 1.0;
      }
    }
    return timeInMinutes;
  }

  private void processForMemoryBuffer(AppHeuristicResult yarnAppHeuristicResult) {
    Double ratioOfDiskSpillsToOutputRecords = 0.0;
    int newBufferSize = 0;
    Double newSpillPercentage = 0.0;
    for (AppHeuristicResultDetails appHeuristicResultDetails : yarnAppHeuristicResult.yarnAppHeuristicResultDetails) {
      if (appHeuristicResultDetails.name.equals("Ratio of disk spills to output records")) {
        ratioOfDiskSpillsToOutputRecords = Double.parseDouble(appHeuristicResultDetails.value);
      }
      int previousBufferSize = Integer.parseInt(appliedParameter.get("Sort Buffer"));
      float previousSortSpill = Float.parseFloat(appliedParameter.get("Sort Spill"));
      if (ratioOfDiskSpillsToOutputRecords >= 3.0) {
        if (previousSortSpill <= 0.8) {
          newSpillPercentage = previousSortSpill + 0.05;
          newBufferSize = (int) (previousBufferSize * 1.2);
        } else if (previousSortSpill >= 0.9) {
          newBufferSize = (int) (previousBufferSize * 1.3);
        }
      } else if (ratioOfDiskSpillsToOutputRecords >= 2.5) {
        if (previousSortSpill <= 0.8) {
          newSpillPercentage = previousSortSpill + 0.05;
          newBufferSize = (int) (previousBufferSize * 1.1);
        } else if (previousSortSpill >= 0.9) {
          newBufferSize = (int) (previousBufferSize * 1.2);
        }
      }
      suggestedParameter.put("mapreduce.task.io.sort.mb", newBufferSize * 1.0);
      suggestedParameter.put("mapreduce.map.sort.spill.percent", newSpillPercentage);
      if (processForMapperMemory) {
        modifyMapperMemory();
      }
    }
  }

  private void modifyMapperMemory() {
    Double mapperMemory = suggestedParameter.get("mapreduce.map.memory.mb");
    Double heapMemory = suggestedParameter.get("mapreduce.map.java.opts");
    Double sortBuffer = suggestedParameter.get("mapreduce.task.io.sort.mb");
    Double minimumMemoryBasedonSortBuffer = max(sortBuffer + 769, sortBuffer * (10 / 6));
    if (minimumMemoryBasedonSortBuffer > mapperMemory) {
      mapperMemory = minimumMemoryBasedonSortBuffer;
      heapMemory = 0.75 * mapperMemory;
    }
    suggestedParameter.put("mapreduce.map.memory.mb", TuningHelper.getContainerSize(mapperMemory));
    suggestedParameter.put("mapreduce.map.java.opts", heapMemory);
  }

  private void optimizeForResourceUsage() {
    for (AppHeuristicResult yarnAppHeuristicResult : _result.yarnAppHeuristicResults) {
      if (yarnAppHeuristicResult.heuristicName != null && yarnAppHeuristicResult.heuristicName.equals(
          "Mapper Memory")) {
        processForMemory(yarnAppHeuristicResult, "Mapper");
      }
      if (yarnAppHeuristicResult.heuristicName != null && yarnAppHeuristicResult.heuristicName.equals(
          "Reducer Memory")) {
        processForMemory(yarnAppHeuristicResult, "Reducer");
      }
    }
  }
}
