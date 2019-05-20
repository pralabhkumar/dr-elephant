/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */


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


/**
 * This class represent one Map Reduce Application . It contains
 * suggested parameters per application.
 */
public class MRApplicationData {
  private final Logger logger = Logger.getLogger(getClass());
  boolean debugEnabled = logger.isDebugEnabled();
  private String applicationID;
  private Map<String, Double> suggestedParameter;
  private AppResult _result;
  private Map<String, AppHeuristicResult> failedHeuristics = null;
  private static Set<String> validHeuristic = null;
  private Map<String, String> appliedParameter = null;
  private Map<String, Double> counterValues = null;

  private enum HeuristicName {
    MAPPER_TIME("Mapper Time"),
    MAPPER_SPEED("Mapper Speed"),
    MAPPER_MEMORY("Mapper Memory"),
    MAPPER_SPILL("Mapper Spill"),
    REDUCER_TIME("Reducer Time"),
    REDUCER_MEMORY("Reducer Memory");
    private final String HeuristicNameValue;

    HeuristicName(String HeuristicNameValue) {
      this.HeuristicNameValue = HeuristicNameValue;
    }

    public String getHeuristicNameValue() {
      return HeuristicNameValue;
    }
  }

  private enum MRApplicationParameter {
    MAPPER_MEMORY("mapreduce.map.memory.mb"),
    MAPPER_HEAP("mapreduce.map.java.opts"),
    REDUCER_MEMORY("mapreduce.reduce.memory.mb"),
    REDUCER_HEAP("mapreduce.reduce.java.opts"),
    MAP_SPLIT_SIZE("mapreduce.input.fileinputformat.split.maxsize"),
    PIG_SPLIT_SIZE("pig.maxCombinedSplitSize"),
    REDUCER_NUMBER("mapreduce.job.reduces"),
    MAP_BUFFER_SIZE("mapreduce.task.io.sort.mb"),
    MAP_SPILL_PERCENTAGE("mapreduce.map.sort.spill.percent");
    private final String propertyName;

    MRApplicationParameter(String propertyName) {
      this.propertyName = propertyName;
    }

    public String getPropertyName() {
      return propertyName;
    }
  }

  /**
   * Rest of the heuristics , may requires code changes
   * and cannot be fixed by just changing parameters and tuning
   * may not be able to tune those heuristics
   */
  static {
    validHeuristic = new HashSet<String>();
    validHeuristic.add(HeuristicName.MAPPER_TIME.getHeuristicNameValue());
    validHeuristic.add(HeuristicName.MAPPER_SPEED.getHeuristicNameValue());
    validHeuristic.add(HeuristicName.MAPPER_MEMORY.getHeuristicNameValue());
    validHeuristic.add(HeuristicName.MAPPER_SPILL.getHeuristicNameValue());
    validHeuristic.add(HeuristicName.REDUCER_TIME.getHeuristicNameValue());
    validHeuristic.add(HeuristicName.REDUCER_MEMORY.getHeuristicNameValue());
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

  /**
   * Check if the heuristics is failed for this application.
   * If more than one heuristics are failed ,then fix heuirstics .
   * Otherwise go for the memory optimization
   *
   */
  private void processForSuggestedParameter() {
    Map<String, AppHeuristicResult> memoryHeuristics = new HashMap<String, AppHeuristicResult>();
    if (_result.yarnAppHeuristicResults != null) {
      for (AppHeuristicResult yarnAppHeuristicResult : _result.yarnAppHeuristicResults) {
        if (yarnAppHeuristicResult.heuristicName.equals(HeuristicName.MAPPER_MEMORY.getHeuristicNameValue())) {
          memoryHeuristics.put("Mapper", yarnAppHeuristicResult);
        }
        if (yarnAppHeuristicResult.heuristicName.equals(HeuristicName.REDUCER_MEMORY.getHeuristicNameValue())) {
          memoryHeuristics.put("Reducer", yarnAppHeuristicResult);
        }
        if (isValidHeuristic(yarnAppHeuristicResult)) {
          logger.info(" Following Heuristic is valid for Optimization. As it have some failure "
              + yarnAppHeuristicResult.heuristicName);
          processHeuristics(yarnAppHeuristicResult);
          failedHeuristics.put(yarnAppHeuristicResult.heuristicName, yarnAppHeuristicResult);
        }
      }
    }
    if (failedHeuristics.size() == 0) {
      logger.info(" No Heuristics Failure . But Still trying to optimize for Memory ");
      processForMemory(memoryHeuristics.get("Mapper"), "Mapper");
      processForMemory(memoryHeuristics.get("Reducer"), "Reducer");
    }
  }

  public Map<String, Double> getSuggestedParameter() {
    return this.suggestedParameter;
  }

  /**
   * Only if the Heuristics severity is greater than 2 , then try to tune it.
   * @param yarnAppHeuristicResult
   * @return
   */
  private boolean isValidHeuristic(AppHeuristicResult yarnAppHeuristicResult) {
    if (validHeuristic.contains(yarnAppHeuristicResult.heuristicName)
        && yarnAppHeuristicResult.severity.getValue() > 2) {
      return true;
    }
    return false;
  }

  private void processHeuristics(AppHeuristicResult yarnAppHeuristicResult) {
    if (yarnAppHeuristicResult.heuristicName.equals(HeuristicName.MAPPER_MEMORY.getHeuristicNameValue())) {
      processForMemory(yarnAppHeuristicResult, "Mapper");
    } else if (yarnAppHeuristicResult.heuristicName.equals(HeuristicName.REDUCER_MEMORY.getHeuristicNameValue())) {
      processForMemory(yarnAppHeuristicResult, "Reducer");
    } else if (yarnAppHeuristicResult.heuristicName.equals(HeuristicName.MAPPER_TIME.getHeuristicNameValue())) {
      processForNumberOfTask(yarnAppHeuristicResult, "Mapper");
    } else if (yarnAppHeuristicResult.heuristicName.equals(HeuristicName.REDUCER_TIME.getHeuristicNameValue())) {
      processForNumberOfTask(yarnAppHeuristicResult, "Reducer");
    } else if (yarnAppHeuristicResult.heuristicName.equals(HeuristicName.MAPPER_SPILL.getHeuristicNameValue())) {
      processForMemoryBuffer(yarnAppHeuristicResult);
    }
  }

  /**
   * Process for Mapper Memory and Heap and reducer memory and heap.
   * @param yarnAppHeuristicResult
   * @param functionType
   */
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
    addCounterData(new String[]{functionType + " Max Virtual Memory (MB)", functionType + " Max Physical Memory (MB)",
            functionType + " Max Total Committed Heap Usage Memory (MB)"}, usedVirtualMemoryMB, usedPhysicalMemoryMB,
        usedHeapMemoryMB);

    logDebuggingStatement(" Used Physical Memory " + yarnAppHeuristicResult.yarnAppResult.id + "_" + functionType + " "
            + usedPhysicalMemoryMB,
        " Used Virtual Memory " + yarnAppHeuristicResult.yarnAppResult.id + "_" + functionType + " "
            + usedVirtualMemoryMB,
        " Used heap Memory " + yarnAppHeuristicResult.yarnAppResult.id + "_" + functionType + " " + usedHeapMemoryMB);

    Double memoryMB = max(usedPhysicalMemoryMB, usedVirtualMemoryMB / (2.1));
    Double heapSizeMax = TuningHelper.getHeapSize(min(0.75 * memoryMB, usedHeapMemoryMB));
    Double containerSize = TuningHelper.getContainerSize(memoryMB);
    addParameterToSuggestedParameter(heapSizeMax, containerSize, yarnAppHeuristicResult.yarnAppResult.id, functionType);
  }

  private void addParameterToSuggestedParameter(Double heapSizeMax, Double containerSize, String id,
      String functionType) {
    if (functionType.equals("Mapper")) {
      addMapperMemoryAndHeapToSuggestedParameter(heapSizeMax, containerSize, id);
    } else {
      addReducerMemoryAndHeapToSuggestedParameter(heapSizeMax, containerSize, id);
    }
  }

  private void addMapperMemoryAndHeapToSuggestedParameter(Double heapSizeMax, Double containerSize,
      String heuristicsResultID) {
    suggestedParameter.put(MRApplicationParameter.MAPPER_MEMORY.getPropertyName(), containerSize);
    suggestedParameter.put(MRApplicationParameter.MAPPER_HEAP.getPropertyName(), heapSizeMax);
    logDebuggingStatement(" Memory Assigned " + heuristicsResultID + "_Mapper " + suggestedParameter.get(
        MRApplicationParameter.MAPPER_MEMORY.getPropertyName()),
        " Heap Assigned " + heuristicsResultID + "_Mapper " + suggestedParameter.get(
            MRApplicationParameter.MAPPER_HEAP.getPropertyName()));
  }

  private void addReducerMemoryAndHeapToSuggestedParameter(Double heapSizeMax, Double containerSize,
      String heuristicsResultID) {
    suggestedParameter.put(MRApplicationParameter.REDUCER_MEMORY.getPropertyName(), containerSize);
    suggestedParameter.put(MRApplicationParameter.REDUCER_HEAP.getPropertyName(), heapSizeMax);
    logDebuggingStatement(" Memory Assigned " + heuristicsResultID + "_Reducer " + suggestedParameter.get(
        MRApplicationParameter.REDUCER_MEMORY.getPropertyName()),
        " Heap Assigned " + heuristicsResultID + "_Reducer " + suggestedParameter.get(
            MRApplicationParameter.REDUCER_HEAP.getPropertyName()));
  }

  private void processForNumberOfTask(AppHeuristicResult yarnAppHeuristicResult, String functionType) {
    long splitSize = 0l;
    long numberOfReduceTask = 0l;
    if (functionType.equals("Mapper")) {
      splitSize = getNewSplitSize(yarnAppHeuristicResult);
      if (splitSize > 0) {
        suggestedParameter.put(MRApplicationParameter.MAP_SPLIT_SIZE.getPropertyName(), splitSize * 1.0);
        suggestedParameter.put(MRApplicationParameter.PIG_SPLIT_SIZE.getPropertyName(), splitSize * 1.0);
      }
    }
    if (functionType.equals("Reducer")) {
      numberOfReduceTask = getNumberOfReducer(yarnAppHeuristicResult);
      if (numberOfReduceTask > 0) {
        suggestedParameter.put(MRApplicationParameter.REDUCER_NUMBER.getPropertyName(), numberOfReduceTask * 1.0);
      }
    }
  }

  private long getNewSplitSize(AppHeuristicResult yarnAppHeuristicResult) {
    logger.info("Calculating Split Size ");
    double averageTaskInputSize = 0.0;
    double averageTaskTimeInMinute = 0.0;
    //long blockSize = 536870912l;
    long newSplitSize = 0l;
    for (AppHeuristicResultDetails appHeuristicResultDetails : yarnAppHeuristicResult.yarnAppHeuristicResultDetails) {
      logger.info("Names " + appHeuristicResultDetails.name);
      if (appHeuristicResultDetails.name.equals("Average task input size")) {
        averageTaskInputSize = (double) MemoryFormatUtils.stringToBytes(appHeuristicResultDetails.value);
      }
      if (appHeuristicResultDetails.name.equals("Average task runtime")) {
        averageTaskTimeInMinute = getTimeInMinute(appHeuristicResultDetails.value);
      }
    }
    addCounterData(new String[]{"Mapper Average task input size", "Mapper Average task runtime"}, averageTaskInputSize,
        averageTaskTimeInMinute);
    if (averageTaskTimeInMinute <= 1.0) {
      newSplitSize = (long) averageTaskInputSize * 2;
    } else if (averageTaskTimeInMinute <= 2.0) {
      newSplitSize = (long) (averageTaskInputSize * 1.2);
    } else if (averageTaskTimeInMinute >= 120) {
      newSplitSize = (long) (averageTaskInputSize / 2);
    } else if (averageTaskTimeInMinute >= 60) {
      newSplitSize = (long) (averageTaskInputSize * 0.8);
    }
    logDebuggingStatement(" Average task input size " + averageTaskInputSize,
        " Average task runtime " + averageTaskTimeInMinute, " New Split Size " + newSplitSize);

    return newSplitSize;
  }

  private long getNumberOfReducer(AppHeuristicResult yarnAppHeuristicResult) {
    int numberoOfTasks = 0;
    double averageTaskTimeInMinute = 0.0;
    int newNumberOfReducer = 0;
    for (AppHeuristicResultDetails appHeuristicResultDetails : yarnAppHeuristicResult.yarnAppHeuristicResultDetails) {
      logger.info("Names " + appHeuristicResultDetails.name);
      if (appHeuristicResultDetails.name.equals("Average task runtime")) {
        averageTaskTimeInMinute = getTimeInMinute(appHeuristicResultDetails.value);
      }
      if (appHeuristicResultDetails.name.equals("Number of tasks")) {
        numberoOfTasks = Integer.parseInt(appHeuristicResultDetails.value);
      }
    }
    addCounterData(new String[]{"Reducer Average task runtime", "Reducer Number of tasks"}, averageTaskTimeInMinute,
        numberoOfTasks * 1.0);
    if (averageTaskTimeInMinute <= 1.0) {
      newNumberOfReducer = numberoOfTasks / 2;
    } else if (averageTaskTimeInMinute <= 2.0) {
      newNumberOfReducer = (int) (numberoOfTasks * 0.8);
    } else if (averageTaskTimeInMinute >= 120) {
      newNumberOfReducer = numberoOfTasks * 2;
    } else if (averageTaskTimeInMinute >= 60) {
      newNumberOfReducer = (int) (numberoOfTasks * 1.2);
    }
    logDebuggingStatement(" Reducer Average task time " + averageTaskTimeInMinute,
        " Reducer Number of tasks " + numberoOfTasks * 1.0, " New number of reducer " + newNumberOfReducer);

    return newNumberOfReducer;
  }

  private double getTimeInMinute(String value) {
    value = value.replaceAll(" ", "");
    String timeSplit[] = value.split("hr|min|sec");
    double timeInMinutes = 0.0;
    if (timeSplit.length == 3) {
      timeInMinutes = timeInMinutes + Integer.parseInt(timeSplit[0]) * 60;
      timeInMinutes = timeInMinutes + Integer.parseInt(timeSplit[1]);
      timeInMinutes = timeInMinutes + Integer.parseInt(timeSplit[2]) * 1.0 / 60 * 1.0;
    } else if (timeSplit.length == 2) {
      timeInMinutes = timeInMinutes + Integer.parseInt(timeSplit[0]);
      timeInMinutes = timeInMinutes + Integer.parseInt(timeSplit[1]) * 1.0 / 60 * 1.0;
    } else if (timeSplit.length == 1) {
      timeInMinutes = timeInMinutes + Integer.parseInt(timeSplit[0]) * 1.0 / 60 * 1.0;
    }
    return timeInMinutes;
  }

  private void processForMemoryBuffer(AppHeuristicResult yarnAppHeuristicResult) {
    float ratioOfDiskSpillsToOutputRecords = 0.0f;
    int newBufferSize = 0;
    float newSpillPercentage = 0.0f;
    for (AppHeuristicResultDetails appHeuristicResultDetails : yarnAppHeuristicResult.yarnAppHeuristicResultDetails) {
      if (appHeuristicResultDetails.name.equals("Ratio of spilled records to output records")) {
        ratioOfDiskSpillsToOutputRecords = Float.parseFloat(appHeuristicResultDetails.value);
      }
      int previousBufferSize = Integer.parseInt(appliedParameter.get("Sort Buffer"));
      float previousSortSpill = Float.parseFloat(appliedParameter.get("Sort Spill"));
      addCounterData(new String[]{"Ratio of spilled records to output records", "Sort Buffer", "Sort Spill"},
          ratioOfDiskSpillsToOutputRecords * 1.0, previousBufferSize * 1.0, previousSortSpill * 1.0);
      if (ratioOfDiskSpillsToOutputRecords >= 3.0) {
        if (previousSortSpill <= 0.85) {
          newSpillPercentage = previousSortSpill + 0.05f;
          newBufferSize = (int) (previousBufferSize * 1.2);
        } else {
          newBufferSize = (int) (previousBufferSize * 1.3);
        }
      } else if (ratioOfDiskSpillsToOutputRecords >= 2.5) {
        if (previousSortSpill <= 0.85) {
          newSpillPercentage = previousSortSpill + 0.05f;
          newBufferSize = (int) (previousBufferSize * 1.1);
        } else {
          newBufferSize = (int) (previousBufferSize * 1.2);
        }
      }
      suggestedParameter.put(MRApplicationParameter.MAP_BUFFER_SIZE.getPropertyName(), newBufferSize * 1.0);
      suggestedParameter.put(MRApplicationParameter.MAP_SPILL_PERCENTAGE.getPropertyName(), newSpillPercentage * 1.0);
      logDebuggingStatement(" Previous Buffer " + previousBufferSize, " Previous Split " + previousSortSpill,
          "Ratio of disk spills to output records " + ratioOfDiskSpillsToOutputRecords,
          "New Buffer Size " + newBufferSize * 1.0, " New Buffer Percentage " + newSpillPercentage);

      modifyMapperMemory();
    }
  }

  private void modifyMapperMemory() {
    Double mapperMemory =
        suggestedParameter.get(MRApplicationParameter.MAPPER_MEMORY.getPropertyName()) == null ? Double.parseDouble(
            appliedParameter.get("Mapper Memory"))
            : suggestedParameter.get(MRApplicationParameter.MAPPER_MEMORY.getPropertyName());
    Double sortBuffer = suggestedParameter.get(MRApplicationParameter.MAP_BUFFER_SIZE.getPropertyName());
    Double minimumMemoryBasedonSortBuffer = max(sortBuffer + 769, sortBuffer * (10 / 6));
    if (minimumMemoryBasedonSortBuffer > mapperMemory) {
      mapperMemory = minimumMemoryBasedonSortBuffer;
      suggestedParameter.put(MRApplicationParameter.MAPPER_MEMORY.getPropertyName(),
          TuningHelper.getContainerSize(mapperMemory));
      Double heapMemory = suggestedParameter.get(MRApplicationParameter.MAPPER_HEAP.getPropertyName());
      if (heapMemory != null) {
        heapMemory = TuningHelper.getHeapSize(min(0.75 * mapperMemory, heapMemory));
        suggestedParameter.put(MRApplicationParameter.MAPPER_HEAP.getPropertyName(), heapMemory);
      } else {
        suggestedParameter.put(MRApplicationParameter.MAPPER_HEAP.getPropertyName(),
            TuningHelper.getHeapSize(0.75 * mapperMemory));
      }
      logDebuggingStatement("Mapper Memory After Buffer Modify " + TuningHelper.getContainerSize(mapperMemory) * 1.0,
          " Mapper heap After Buffer Modify " + heapMemory);
    }
  }

  private void addCounterData(String[] counterNames, Double... counterValue) {
    for (int i = 0; i < counterNames.length; i++) {
      counterValues.put(counterNames[i], counterValue[i]);
    }
  }

  private void logDebuggingStatement(String... statements) {
    if (debugEnabled) {
      for (String log : statements) {
        logger.info(log);
      }
    }
  }
}