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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import models.AppHeuristicResult;
import models.AppHeuristicResultDetails;
import models.AppResult;
import org.apache.log4j.Logger;

import static com.linkedin.drelephant.tuning.hbt.MRConstant.*;
import static com.linkedin.drelephant.tuning.hbt.MRConstant.Function_Name.*;
import static java.lang.Math.*;


/**
 * This class represent one Map Reduce Application per Job . It contains
 * suggested parameters per application.
 */
public class MRApplicationData {
  private final Logger logger = Logger.getLogger(getClass());
  private static final int HEURISTIC_THRESHOLD = 2;
  private boolean debugEnabled = logger.isDebugEnabled();
  private String applicationID;
  private Map<String, Double> suggestedParameter;
  private AppResult _result;
  private Map<String, AppHeuristicResult> failedHeuristics = null;
  private static Set<String> validHeuristic = null;
  private Map<String, String> appliedParameter = null;
  private Map<String, Double> counterValues = null;

  static {
    validHeuristic = new HashSet<String>();
    validHeuristic.add(MAPPER_TIME_HEURISTIC);
    validHeuristic.add(MAPPER_SPEED_HEURISTIC);
    validHeuristic.add(MAPPER_MEMORY_HEURISTIC);
    validHeuristic.add(MAPPER_SPILL_HEURISTIC);
    validHeuristic.add(REDUCER_TIME_HEURISTIC);
    validHeuristic.add(REDUCER_MEMORY_HEURISTIC);
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
        if (yarnAppHeuristicResult.heuristicName.equals(MAPPER_MEMORY_HEURISTIC)) {
          memoryHeuristics.put(Mapper.name(), yarnAppHeuristicResult);
        }
        if (yarnAppHeuristicResult.heuristicName.equals(REDUCER_MEMORY_HEURISTIC)) {
          memoryHeuristics.put(Reducer.name(), yarnAppHeuristicResult);
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
      processForMemory(memoryHeuristics.get(Mapper.name()), Mapper.name());
      processForMemory(memoryHeuristics.get(Reducer.name()), Reducer.name());
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
        && yarnAppHeuristicResult.severity.getValue() > HEURISTIC_THRESHOLD) {
      return true;
    }
    return false;
  }

  /**
   * Since Dr elephant runs on Java 1.5 , switch case is not used.
   * @param yarnAppHeuristicResult
   */
  private void processHeuristics(AppHeuristicResult yarnAppHeuristicResult) {
    if (yarnAppHeuristicResult.heuristicName.equals(MAPPER_MEMORY_HEURISTIC)) {
      processForMemory(yarnAppHeuristicResult, Mapper.name());
    } else if (yarnAppHeuristicResult.heuristicName.equals(REDUCER_MEMORY_HEURISTIC)) {
      processForMemory(yarnAppHeuristicResult, Reducer.name());
    } else if (yarnAppHeuristicResult.heuristicName.equals(MAPPER_TIME_HEURISTIC)) {
      processForNumberOfTask(yarnAppHeuristicResult, Mapper.name());
    } else if (yarnAppHeuristicResult.heuristicName.equals(REDUCER_TIME_HEURISTIC)) {
      processForNumberOfTask(yarnAppHeuristicResult, Reducer.name());
    } else if (yarnAppHeuristicResult.heuristicName.equals(MAPPER_SPILL_HEURISTIC)) {
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
      if (appHeuristicResultDetails.name.equals(MAX_VIRTUAL_MEMORY)) {
        usedVirtualMemoryMB = (double) MemoryFormatUtils.stringToBytes(appHeuristicResultDetails.value);
      }
      if (appHeuristicResultDetails.name.equals(MAX_PHYSICAL_MEMORY)) {
        usedPhysicalMemoryMB = (double) MemoryFormatUtils.stringToBytes(appHeuristicResultDetails.value);
      }
      if (appHeuristicResultDetails.name.equals(MAX_TOTAL_COMMITTED_HEAP_USAGE)) {
        usedHeapMemoryMB = (double) MemoryFormatUtils.stringToBytes(appHeuristicResultDetails.value);
      }
    }
    addCounterData(new String[]{functionType + " " + MAX_VIRTUAL_MEMORY, functionType + " " + MAX_PHYSICAL_MEMORY,
            functionType + " " + MAX_TOTAL_COMMITTED_HEAP_USAGE}, usedVirtualMemoryMB, usedPhysicalMemoryMB,
        usedHeapMemoryMB);

    logDebuggingStatement(" Used Physical Memory " + yarnAppHeuristicResult.yarnAppResult.id + "_" + functionType + " "
            + usedPhysicalMemoryMB,
        " Used Virtual Memory " + yarnAppHeuristicResult.yarnAppResult.id + "_" + functionType + " "
            + usedVirtualMemoryMB,
        " Used heap Memory " + yarnAppHeuristicResult.yarnAppResult.id + "_" + functionType + " " + usedHeapMemoryMB);

    Double memoryMB = max(usedPhysicalMemoryMB, usedVirtualMemoryMB / (VIRTUALMEMORY_TO_PHYSICALMEMORY_RATIO));
    Double heapSizeMax =
        TuningHelper.getHeapSize(min(HEAPSIZE_TO_MAPPERMEMORY_SAFE_RATIO * memoryMB, usedHeapMemoryMB));
    Double containerSize = TuningHelper.getContainerSize(memoryMB);
    addParameterToSuggestedParameter(heapSizeMax, containerSize, yarnAppHeuristicResult.yarnAppResult.id, functionType);
  }

  private void addParameterToSuggestedParameter(Double heapSizeMax, Double containerSize, String id,
      String functionType) {
    if (functionType.equals(Mapper.name())) {
      addMapperMemoryAndHeapToSuggestedParameter(heapSizeMax, containerSize, id);
    } else {
      addReducerMemoryAndHeapToSuggestedParameter(heapSizeMax, containerSize, id);
    }
  }

  private void addMapperMemoryAndHeapToSuggestedParameter(Double heapSizeMax, Double containerSize,
      String heuristicsResultID) {
    suggestedParameter.put(MAPPER_MEMORY, containerSize);
    suggestedParameter.put(MAPPER_HEAP, heapSizeMax);
    logDebuggingStatement(" Memory Assigned " + heuristicsResultID + "_Mapper " + suggestedParameter.get(MAPPER_MEMORY),
        " Heap Assigned " + heuristicsResultID + "_Mapper " + suggestedParameter.get(MAPPER_HEAP));
  }

  private void addReducerMemoryAndHeapToSuggestedParameter(Double heapSizeMax, Double containerSize,
      String heuristicsResultID) {
    suggestedParameter.put(REDUCER_MEMORY, containerSize);
    suggestedParameter.put(REDUCER_HEAP, heapSizeMax);
    logDebuggingStatement(
        " Memory Assigned " + heuristicsResultID + "_Reducer " + suggestedParameter.get(REDUCER_MEMORY),
        " Heap Assigned " + heuristicsResultID + "_Reducer " + suggestedParameter.get(REDUCER_HEAP));
  }

  private void processForNumberOfTask(AppHeuristicResult yarnAppHeuristicResult, String functionType) {
    long splitSize = 0l;
    long numberOfReduceTask = 0l;
    if (functionType.equals(Mapper.name())) {
      splitSize = getNewSplitSize(yarnAppHeuristicResult);
      if (splitSize > 0) {
        suggestedParameter.put(MAP_SPLIT_SIZE, splitSize * 1.0);
        suggestedParameter.put(PIG_SPLIT_SIZE, splitSize * 1.0);
      }
    }
    if (functionType.equals(Reducer.name())) {
      numberOfReduceTask = getNumberOfReducer(yarnAppHeuristicResult);
      if (numberOfReduceTask > 0) {
        suggestedParameter.put(NUMBER_OF_REDUCER, numberOfReduceTask * 1.0);
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
      if (appHeuristicResultDetails.name.equals(AVERAGE_TASK_INPUT_SIZE)) {
        averageTaskInputSize = (double) MemoryFormatUtils.stringToBytes(appHeuristicResultDetails.value);
      }
      if (appHeuristicResultDetails.name.equals(AVERAGE_TASK_RUNTIME)) {
        averageTaskTimeInMinute = getTimeInMinute(appHeuristicResultDetails.value);
      }
    }
    addCounterData(new String[]{Mapper + " " + AVERAGE_TASK_INPUT_SIZE, Mapper + " " + AVERAGE_TASK_RUNTIME},
        averageTaskInputSize, averageTaskTimeInMinute);
    if (averageTaskTimeInMinute <= AVG_TASK_TIME_LOW_THRESHOLDS_FIRST) {
      newSplitSize = (long) averageTaskInputSize * SPLIT_SIZE_INCREASE_FIRST;
    } else if (averageTaskTimeInMinute <= AVG_TASK_TIME_LOW_THRESHOLDS_SECOND) {
      newSplitSize = (long) (averageTaskInputSize * SPLIT_SIZE_INCREASE_SECOND);
    } else if (averageTaskTimeInMinute >= AVG_TASK_TIME_HIGH_THRESHOLDS_FIRST) {
      newSplitSize = (long) (averageTaskInputSize / SPLIT_SIZE_INCREASE_FIRST);
    } else if (averageTaskTimeInMinute >= AVG_TASK_TIME_HIGH_THRESHOLDS_SECOND) {
      newSplitSize = (long) (averageTaskInputSize * SPLIT_SIZE_DECREASE);
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
      if (appHeuristicResultDetails.name.equals(AVERAGE_TASK_RUNTIME)) {
        averageTaskTimeInMinute = getTimeInMinute(appHeuristicResultDetails.value);
      }
      if (appHeuristicResultDetails.name.equals(NUMBER_OF_TASK)) {
        numberoOfTasks = Integer.parseInt(appHeuristicResultDetails.value);
      }
    }
    addCounterData(new String[]{Reducer + " " + AVERAGE_TASK_RUNTIME, Reducer + " " + NUMBER_OF_TASK},
        averageTaskTimeInMinute, numberoOfTasks * 1.0);
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
      if (appHeuristicResultDetails.name.equals(RATIO_OF_SPILLED_RECORDS_TO_OUTPUT_RECORDS)) {
        ratioOfDiskSpillsToOutputRecords = Float.parseFloat(appHeuristicResultDetails.value);
      }
      int previousBufferSize = Integer.parseInt(appliedParameter.get(SORT_BUFFER));
      float previousSortSpill = Float.parseFloat(appliedParameter.get(SORT_SPILL));
      addCounterData(new String[]{RATIO_OF_SPILLED_RECORDS_TO_OUTPUT_RECORDS, SORT_BUFFER, SORT_SPILL},
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
      suggestedParameter.put(BUFFER_SIZE, newBufferSize * 1.0);
      suggestedParameter.put(SPILL_PERCENTAGE, newSpillPercentage * 1.0);
      logDebuggingStatement(" Previous Buffer " + previousBufferSize, " Previous Split " + previousSortSpill,
          "Ratio of disk spills to output records " + ratioOfDiskSpillsToOutputRecords,
          "New Buffer Size " + newBufferSize * 1.0, " New Buffer Percentage " + newSpillPercentage);

      modifyMapperMemory();
    }
  }

  private void modifyMapperMemory() {
    Double mapperMemory = suggestedParameter.get(MAPPER_MEMORY) == null ? Double.parseDouble(
        appliedParameter.get(MAPPER_MEMORY_HEURISTIC)) : suggestedParameter.get(MAPPER_MEMORY);
    Double sortBuffer = suggestedParameter.get(BUFFER_SIZE);
    Double minimumMemoryBasedonSortBuffer = max(sortBuffer + 769, sortBuffer * (10 / 6));
    if (minimumMemoryBasedonSortBuffer > mapperMemory) {
      mapperMemory = minimumMemoryBasedonSortBuffer;
      suggestedParameter.put(MAPPER_MEMORY, TuningHelper.getContainerSize(mapperMemory));
      Double heapMemory = suggestedParameter.get(MAPPER_HEAP);
      if (heapMemory != null) {
        heapMemory = TuningHelper.getHeapSize(min(0.75 * mapperMemory, heapMemory));
        suggestedParameter.put(MAPPER_HEAP, heapMemory);
      } else {
        suggestedParameter.put(MAPPER_HEAP, TuningHelper.getHeapSize(0.75 * mapperMemory));
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