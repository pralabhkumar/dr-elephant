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
  }

  public void processForSuggestedParameter() {
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
    Double memoryMB = max(usedPhysicalMemoryMB, usedVirtualMemoryMB / (2.1));
    Double heapSizeLowerBound = min(0.75 * memoryMB, usedHeapMemoryMB);
    suggestedParameter.put(functionType + " Memory", TuningHelper.getContainerSize(memoryMB));
    suggestedParameter.put(functionType + "Heap", heapSizeLowerBound);
  }

  private void processForNumberOfTask(AppHeuristicResult yarnAppHeuristicResult, String functionType) {
    long splitSize = 0l;
    long numberOfReduceTask = 0l;
    if (functionType.equals("Mapper")) {
      splitSize = getNewSplitSize(yarnAppHeuristicResult);
      if (splitSize > 0) {
        suggestedParameter.put("Split Size", splitSize * 1.0);
      }
    }
    if (functionType.equals("Reducer")) {
      numberOfReduceTask = getNumberOfReducer(yarnAppHeuristicResult);
      if (numberOfReduceTask > 0) {
        suggestedParameter.put("Number of Reducers", numberOfReduceTask * 1.0);
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
      suggestedParameter.put("Sort Buffer", newBufferSize * 1.0);
      suggestedParameter.put("Sort Spill", newSpillPercentage);
      if (processForMapperMemory) {
        modifyMapperMemory();
      }
    }
  }

  private void modifyMapperMemory() {
    Double mapperMemory = suggestedParameter.get("Mapper Memory");
    Double heapMemory = suggestedParameter.get("Mapper Heap");
    Double sortBuffer = suggestedParameter.get("Sort Buffer");
    Double minimumMemoryBasedonSortBuffer = max(sortBuffer + 769, sortBuffer * (10 / 6));
    if (minimumMemoryBasedonSortBuffer > mapperMemory) {
      mapperMemory = minimumMemoryBasedonSortBuffer;
      heapMemory = 0.75 * mapperMemory;
    }
    suggestedParameter.put("Mapper Memory", TuningHelper.getContainerSize(mapperMemory));
    suggestedParameter.put("Mapper Heap", heapMemory);
    suggestedParameter.put("Sort Buffer", sortBuffer);
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
