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

import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.exceptions.util.ExceptionUtils;
import com.linkedin.drelephant.mapreduce.heuristics.MapperSpillHeuristic;
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
import static com.linkedin.drelephant.tuning.hbt.MRConstant.MRConfigurationBuilder.*;
import static java.lang.Math.*;
import static com.linkedin.drelephant.mapreduce.heuristics.CommonConstantsHeuristic.ParameterKeys.*;
import static com.linkedin.drelephant.mapreduce.heuristics.CommonConstantsHeuristic.UtilizedParameterKeys.*;


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

  //todo: Reducer time is not valid heuristic , since we have to find ways to change number of reducer from azkaban.
  // mapreduce.job.reduces, pig.exec.reducers.bytes.per.reducer have not worked from azkaban
  static {
    validHeuristic = new HashSet<String>();
    validHeuristic.add(MAPPER_TIME_HEURISTIC);
    validHeuristic.add(MAPPER_SPEED_HEURISTIC);
    validHeuristic.add(MAPPER_MEMORY_HEURISTIC);
    validHeuristic.add(MAPPER_SPILL_HEURISTIC);
    validHeuristic.add(REDUCER_MEMORY_HEURISTIC);
    MRConstant.MRConfigurationBuilder.buildConfigurations(ElephantContext.instance().getAutoTuningConf());
  }

  MRApplicationData(AppResult result, Map<String, String> appliedParameter) {
    this.applicationID = result.id;
    this._result = result;
    this.suggestedParameter = new HashMap<String, Double>();
    this.failedHeuristics = new HashMap<String, AppHeuristicResult>();
    this.appliedParameter = appliedParameter;
    this.counterValues = new HashMap<String, Double>();
    processForSuggestedParameter();
    postFillSuggestedParameterWithAppliedParameter();
  }

  public Map<String, Double> getCounterValues() {
    return this.counterValues;
  }

  public String getApplicationID() {
    return this.applicationID;
  }

  /**
   * Fill the suggested parameters with the applied parameter ,
   * if there is no current suggesteted parameter for particular property.
   */
  private void postFillSuggestedParameterWithAppliedParameter() {
    if (!suggestedParameter.containsKey(MAPPER_MEMORY_HADOOP_CONF.getValue())) {
      suggestedParameter.put(MAPPER_MEMORY_HADOOP_CONF.getValue(),
          Double.parseDouble(appliedParameter.get(MAPPER_MEMORY_HEURISTICS_CONF.getValue())));
    }
    if (!suggestedParameter.containsKey(REDUCER_MEMORY_HADOOP_CONF.getValue())) {
      suggestedParameter.put(REDUCER_MEMORY_HADOOP_CONF.getValue(),
          Double.parseDouble(appliedParameter.get(REDUCER_MEMORY_HEURISTICS_CONF.getValue())));
    }
    if (!suggestedParameter.containsKey(MAPPER_HEAP_HADOOP_CONF.getValue())) {
      suggestedParameter.put(MAPPER_HEAP_HADOOP_CONF.getValue(),
          TuningHelper.parseMaxHeapSizeInMB(appliedParameter.get(MAPPER_HEAP_HEURISTICS_CONF.getValue())));
    }
    if (!suggestedParameter.containsKey(REDUCER_HEAP_HADOOP_CONF.getValue())) {
      suggestedParameter.put(REDUCER_HEAP_HADOOP_CONF.getValue(),
          TuningHelper.parseMaxHeapSizeInMB(appliedParameter.get(REDUCER_HEAP_HEURISTICS_CONF.getValue())));
    }
    if (!suggestedParameter.containsKey(SORT_BUFFER_HADOOP_CONF.getValue())) {
      suggestedParameter.put(SORT_BUFFER_HADOOP_CONF.getValue(),
          Double.parseDouble(appliedParameter.get(SORT_BUFFER_HEURISTICS_CONF.getValue())));
    }
    if (!suggestedParameter.containsKey(SORT_SPILL_HADOOP_CONF.getValue())) {
      suggestedParameter.put(SORT_SPILL_HADOOP_CONF.getValue(),
          Double.parseDouble(appliedParameter.get(SORT_SPILL_HEURISTICS_CONF.getValue())));
    }

    postFillSplitSize();
    if (debugEnabled) {
      for (String parameterName : suggestedParameter.keySet()) {
        logger.debug(" Suggested Parameter Test " + parameterName + "\t" + suggestedParameter.get(parameterName));
      }
    }
  }

  /**
   *
   */
  private void postFillSplitSize() {
    /**
     * If Algorithm have not suggested any split size  this time ,
     * then check for previously applied split size . If previous applied split size
     * is there in pig.max then put the same in suggested.
     * If prevoius applied split size is there mapreduce.split , then put the same in suggested.
     *
     */
    if (!suggestedParameter.containsKey(PIG_SPLIT_SIZE_HADOOP_CONF.getValue())) {
      if (appliedParameter.containsKey(PIG_MAX_SPLIT_SIZE_HEURISTICS_CONF.getValue())) {
        suggestedParameter.put(PIG_SPLIT_SIZE_HADOOP_CONF.getValue(),
            Double.parseDouble(appliedParameter.get(PIG_MAX_SPLIT_SIZE_HEURISTICS_CONF.getValue())));
        suggestedParameter.put(SPLIT_SIZE_HADOOP_CONF.getValue(),
            Double.parseDouble(appliedParameter.get(PIG_MAX_SPLIT_SIZE_HEURISTICS_CONF.getValue())));
      } else if (appliedParameter.containsKey(SPLIT_SIZE_HEURISTICS_CONF.getValue())) {
        suggestedParameter.put(PIG_SPLIT_SIZE_HADOOP_CONF.getValue(),
            Double.parseDouble(appliedParameter.get(SPLIT_SIZE_HEURISTICS_CONF.getValue())));
        suggestedParameter.put(SPLIT_SIZE_HADOOP_CONF.getValue(),
            Double.parseDouble(appliedParameter.get(SPLIT_SIZE_HEURISTICS_CONF.getValue())));
      }
    }
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
   * Not able to set number of reducer from Azkaban and hence not optimizing for
   * Reducer time
   * @param yarnAppHeuristicResult
   */
  private void processHeuristics(AppHeuristicResult yarnAppHeuristicResult) {
    if (yarnAppHeuristicResult.heuristicName.equals(MAPPER_MEMORY_HEURISTIC)) {
      processForMemory(yarnAppHeuristicResult, Mapper.name());
    } else if (yarnAppHeuristicResult.heuristicName.equals(REDUCER_MEMORY_HEURISTIC)) {
      processForMemory(yarnAppHeuristicResult, Reducer.name());
    } else if (yarnAppHeuristicResult.heuristicName.equals(MAPPER_TIME_HEURISTIC)) {
      processForNumberOfTask(yarnAppHeuristicResult, Mapper.name());
    } /*else if (yarnAppHeuristicResult.heuristicName.equals(REDUCER_TIME_HEURISTIC)) {
      processForNumberOfTask(yarnAppHeuristicResult, Reducer.name());
    }*/ else if (yarnAppHeuristicResult.heuristicName.equals(MAPPER_SPILL_HEURISTIC)) {
      processForMemoryBuffer(yarnAppHeuristicResult);
    }
  }

  /**
   * Process for Mapper Memory and Heap and reducer memory and heap.
   * Mapper Memory / Mapper Heap / Reducer Memory /Reducer heap
   * These values are decided based on maximum virtual memory used,
   * maximum physical memory used and maximum total committed heap usage.
   * @param yarnAppHeuristicResult
   * @param functionType
   */
  private void processForMemory(AppHeuristicResult yarnAppHeuristicResult, String functionType) {
    Double usedPhysicalMemoryMB = 0.0, usedVirtualMemoryMB = 0.0, usedHeapMemoryMB = 0.0;
    for (AppHeuristicResultDetails appHeuristicResultDetails : yarnAppHeuristicResult.yarnAppHeuristicResultDetails) {
      if (appHeuristicResultDetails.name.equals(MAX_VIRTUAL_MEMORY.getValue())) {
        usedVirtualMemoryMB = (double) MemoryFormatUtils.stringToBytes(appHeuristicResultDetails.value);
      }
      if (appHeuristicResultDetails.name.equals(MAX_PHYSICAL_MEMORY.getValue())) {
        usedPhysicalMemoryMB = (double) MemoryFormatUtils.stringToBytes(appHeuristicResultDetails.value);
      }
      if (appHeuristicResultDetails.name.equals(MAX_TOTAL_COMMITTED_HEAP_USAGE_MEMORY.getValue())) {
        usedHeapMemoryMB = (double) MemoryFormatUtils.stringToBytes(appHeuristicResultDetails.value);
      }
    }
    addCounterData(new String[]{functionType + " " + MAX_VIRTUAL_MEMORY.getValue(),
            functionType + " " + MAX_PHYSICAL_MEMORY.getValue(),
            functionType + " " + MAX_TOTAL_COMMITTED_HEAP_USAGE_MEMORY.getValue()}, usedVirtualMemoryMB,
        usedPhysicalMemoryMB, usedHeapMemoryMB);

    logDebuggingStatement(" Used Physical Memory " + yarnAppHeuristicResult.yarnAppResult.id + "_" + functionType + " "
            + usedPhysicalMemoryMB,
        " Used Virtual Memory " + yarnAppHeuristicResult.yarnAppResult.id + "_" + functionType + " "
            + usedVirtualMemoryMB,
        " Used heap Memory " + yarnAppHeuristicResult.yarnAppResult.id + "_" + functionType + " " + usedHeapMemoryMB);

    Double memoryMB =
        max(usedPhysicalMemoryMB, usedVirtualMemoryMB / (VIRTUALMEMORY_TO_PHYSICALMEMORY_RATIO.getValue()));
    Double heapSizeMax =
        TuningHelper.getHeapSize(min(HEAPSIZE_TO_MAPPERMEMORY_SAFE_RATIO.getValue() * memoryMB, usedHeapMemoryMB));
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
    suggestedParameter.put(MAPPER_MEMORY_HADOOP_CONF.getValue(), containerSize);
    suggestedParameter.put(MAPPER_HEAP_HADOOP_CONF.getValue(), heapSizeMax);
    logDebuggingStatement(" Memory Assigned " + heuristicsResultID + "_Mapper " + suggestedParameter.get(
        MAPPER_MEMORY_HADOOP_CONF.getValue()),
        " Heap Assigned " + heuristicsResultID + "_Mapper " + suggestedParameter.get(
            MAPPER_HEAP_HADOOP_CONF.getValue()));
  }

  private void addReducerMemoryAndHeapToSuggestedParameter(Double heapSizeMax, Double containerSize,
      String heuristicsResultID) {
    suggestedParameter.put(REDUCER_MEMORY_HADOOP_CONF.getValue(), containerSize);
    suggestedParameter.put(REDUCER_HEAP_HADOOP_CONF.getValue(), heapSizeMax);
    logDebuggingStatement(" Memory Assigned " + heuristicsResultID + "_Reducer " + suggestedParameter.get(
        REDUCER_MEMORY_HADOOP_CONF.getValue()),
        " Heap Assigned " + heuristicsResultID + "_Reducer " + suggestedParameter.get(
            REDUCER_HEAP_HADOOP_CONF.getValue()));
  }

  /**
   * todo : currently task for reducers are not processed  , since we have to find
   * out the way to pass number of reducer tasks from azkaban
   * @param yarnAppHeuristicResult
   * @param functionType
   */
  private void processForNumberOfTask(AppHeuristicResult yarnAppHeuristicResult, String functionType) {
    if (functionType.equals(Mapper.name())) {
      processForNumberOfTaskMapper(yarnAppHeuristicResult);
    } else if (functionType.equals(Reducer.name())) {
      processForNumberOfTaskReducer(yarnAppHeuristicResult);
    }
  }

  private void processForNumberOfTaskMapper(AppHeuristicResult yarnAppHeuristicResult) {
    long splitSize;
    splitSize = getNewSplitSize(yarnAppHeuristicResult);
    if (splitSize > 0) {
      suggestedParameter.put(SPLIT_SIZE_HADOOP_CONF.getValue(), splitSize * 1.0);
      suggestedParameter.put(PIG_SPLIT_SIZE_HADOOP_CONF.getValue(), splitSize * 1.0);
    }
  }

  private void processForNumberOfTaskReducer(AppHeuristicResult yarnAppHeuristicResult) {
    long numberOfReduceTask;
    numberOfReduceTask = getNumberOfReducer(yarnAppHeuristicResult);
    if (numberOfReduceTask > 0) {
      suggestedParameter.put(NUMBER_OF_REDUCER_CONF.getValue(), numberOfReduceTask * 1.0);
    }
  }

  /**
   * IF the mapper time heuristics is failed , its failed because either the
   * average input going to one task is too small or too large .
   * If its too small , increase the average task input size and that would be the
   * split size . If time is too big , decrease the average task input size and that would
   * be split size.
   * @param yarnAppHeuristicResult
   * @return
   */
  private long getNewSplitSize(AppHeuristicResult yarnAppHeuristicResult) {
    logger.info("Calculating Split Size ");
    double averageTaskInputSize = 0.0;
    double averageTaskTimeInMinute = 0.0;
    //long blockSize = 536870912l;
    long newSplitSize = 0l;
    for (AppHeuristicResultDetails appHeuristicResultDetails : yarnAppHeuristicResult.yarnAppHeuristicResultDetails) {
      logger.info("Names " + appHeuristicResultDetails.name);
      if (appHeuristicResultDetails.name.equals(AVERAGE_TASK_INPUT_SIZE.getValue())) {
        averageTaskInputSize = (double) MemoryFormatUtils.stringToBytes(appHeuristicResultDetails.value);
      }
      if (appHeuristicResultDetails.name.equals(AVERAGE_TASK_RUNTIME.getValue())) {
        averageTaskTimeInMinute = getTimeInMinute(appHeuristicResultDetails.value);
      }
    }
    addCounterData(
        new String[]{Mapper + " " + AVERAGE_TASK_INPUT_SIZE.getValue(), Mapper + " " + AVERAGE_TASK_RUNTIME.getValue()},
        averageTaskInputSize, averageTaskTimeInMinute);
    if (averageTaskTimeInMinute <= AVG_TASK_TIME_LOW_THRESHOLDS_FIRST.getValue()) {
      newSplitSize = (long) averageTaskInputSize * SPLIT_SIZE_INCREASE_FIRST.getValue();
    } else if (averageTaskTimeInMinute <= AVG_TASK_TIME_LOW_THRESHOLDS_SECOND.getValue()) {
      newSplitSize = (long) (averageTaskInputSize * SPLIT_SIZE_INCREASE_SECOND.getValue());
    } else if (averageTaskTimeInMinute >= AVG_TASK_TIME_HIGH_THRESHOLDS_FIRST.getValue()) {
      newSplitSize = (long) (averageTaskInputSize / SPLIT_SIZE_INCREASE_FIRST.getValue());
    } else if (averageTaskTimeInMinute >= AVG_TASK_TIME_HIGH_THRESHOLDS_SECOND.getValue()) {
      newSplitSize = (long) (averageTaskInputSize * SPLIT_SIZE_DECREASE.getValue());
    }
    logDebuggingStatement(" Average task input size " + averageTaskInputSize,
        " Average task runtime " + averageTaskTimeInMinute, " New Split Size " + newSplitSize);

    return newSplitSize;
  }

  /**
   * This method is created but currently not in used ,since changing number of reducers
   * from azkaban is todo .
   * @param yarnAppHeuristicResult
   * @return
   */
  private long getNumberOfReducer(AppHeuristicResult yarnAppHeuristicResult) {
    int numberoOfTasks = 0;
    double averageTaskTimeInMinute = 0.0;
    int newNumberOfReducer = 0;
    for (AppHeuristicResultDetails appHeuristicResultDetails : yarnAppHeuristicResult.yarnAppHeuristicResultDetails) {
      logger.info("Names " + appHeuristicResultDetails.name);
      if (appHeuristicResultDetails.name.equals(AVERAGE_TASK_RUNTIME.getValue())) {
        averageTaskTimeInMinute = getTimeInMinute(appHeuristicResultDetails.value);
      }
      if (appHeuristicResultDetails.name.equals(NUMBER_OF_TASK.getValue())) {
        numberoOfTasks = Integer.parseInt(appHeuristicResultDetails.value);
      }
    }
    addCounterData(
        new String[]{Reducer + " " + AVERAGE_TASK_RUNTIME.getValue(), Reducer + " " + NUMBER_OF_TASK.getValue()},
        averageTaskTimeInMinute, numberoOfTasks * 1.0);
    if (averageTaskTimeInMinute <= AVG_TASK_TIME_LOW_THRESHOLDS_FIRST.getValue()) {
      newNumberOfReducer = numberoOfTasks / SPLIT_SIZE_INCREASE_FIRST.getValue();
    } else if (averageTaskTimeInMinute <= AVG_TASK_TIME_LOW_THRESHOLDS_SECOND.getValue()) {
      newNumberOfReducer = (int) (numberoOfTasks * SPLIT_SIZE_DECREASE.getValue());
    } else if (averageTaskTimeInMinute >= AVG_TASK_TIME_HIGH_THRESHOLDS_FIRST.getValue()) {
      newNumberOfReducer = numberoOfTasks * SPLIT_SIZE_INCREASE_FIRST.getValue();
    } else if (averageTaskTimeInMinute >= AVG_TASK_TIME_HIGH_THRESHOLDS_SECOND.getValue()) {
      newNumberOfReducer = (int) (numberoOfTasks * SPLIT_SIZE_INCREASE_SECOND.getValue());
    }
    logDebuggingStatement(" Reducer Average task time " + averageTaskTimeInMinute,
        " Reducer Number of tasks " + numberoOfTasks * 1.0, " New number of reducer " + newNumberOfReducer);

    return newNumberOfReducer;
  }

  private double getTimeInMinute(String value) {
    value = value.replaceAll(" ", "");
    String timeSplit[] = value.split("hr|min|sec");
    double timeInMinutes = 0.0;
    if (timeSplit.length == TimeUnit.sec.ordinal() + 1) {
      timeInMinutes = timeInMinutes + Integer.parseInt(timeSplit[0]) * 60;
      timeInMinutes = timeInMinutes + Integer.parseInt(timeSplit[1]);
      timeInMinutes = timeInMinutes + Integer.parseInt(timeSplit[2]) * 1.0 / 60 * 1.0;
    } else if (timeSplit.length == TimeUnit.min.ordinal() + 1) {
      timeInMinutes = timeInMinutes + Integer.parseInt(timeSplit[0]);
      timeInMinutes = timeInMinutes + Integer.parseInt(timeSplit[1]) * 1.0 / 60 * 1.0;
    } else if (timeSplit.length == TimeUnit.hr.ordinal() + 1) {
      timeInMinutes = timeInMinutes + Integer.parseInt(timeSplit[0]) * 1.0 / 60 * 1.0;
    }
    return timeInMinutes;
  }

  /**
   * This will change value of sort buffer and sort spill
   * based on ratio of spill records to output records.
   * If buffer size is changed , then mapper memory should also be modified.
   * @param yarnAppHeuristicResult
   */
  private void processForMemoryBuffer(AppHeuristicResult yarnAppHeuristicResult) {
    float ratioOfDiskSpillsToOutputRecords = 0.0f;
    int newBufferSize = 0;
    float newSpillPercentage = 0.0f;

    Map<String, String> heuristicsResults = yarnAppHeuristicResult.yarnAppResult.getHeuristicsResultDetailsMap();
    String ratioOfSpillRecordsToOutputRecordsValue = heuristicsResults.get(
        MapperSpillHeuristic.class.getCanonicalName() + "_" + RATIO_OF_SPILLED_RECORDS_TO_OUTPUT_RECORDS.getValue());
    if (ratioOfSpillRecordsToOutputRecordsValue != null) {
      ratioOfDiskSpillsToOutputRecords = Float.parseFloat(ratioOfSpillRecordsToOutputRecordsValue);
    }
    int previousBufferSize = Integer.parseInt(appliedParameter.get(SORT_BUFFER.getValue()));
    float previousSortSpill = Float.parseFloat(appliedParameter.get(SORT_SPILL.getValue()));
    addCounterData(new String[]{RATIO_OF_SPILLED_RECORDS_TO_OUTPUT_RECORDS.getValue(), SORT_BUFFER.getValue(),
            SORT_SPILL.getValue()}, ratioOfDiskSpillsToOutputRecords * 1.0, previousBufferSize * 1.0,
        previousSortSpill * 1.0);
    if (ratioOfDiskSpillsToOutputRecords >= RATIO_OF_DISK_SPILL_TO_OUTPUT_RECORDS_THRESHOLD_FIRST.getValue()) {
      if (previousSortSpill <= SORT_SPILL_THRESHOLD_FIRST.getValue()) {
        newSpillPercentage = previousSortSpill + SORT_SPILL_INCREASE.getValue();
        newBufferSize = (int) (previousBufferSize * BUFFER_SIZE_INCREASE_FIRST.getValue());
      } else {
        newBufferSize = (int) (previousBufferSize * BUFFER_SIZE_INCREASE_SECOND.getValue());
      }
    } else if (ratioOfDiskSpillsToOutputRecords >= RATIO_OF_DISK_SPILL_TO_OUTPUT_RECORDS_THRESHOLD_SECOND.getValue()) {
      if (previousSortSpill <= SORT_SPILL_THRESHOLD_FIRST.getValue()) {
        newSpillPercentage = previousSortSpill + SORT_SPILL_INCREASE.getValue();
        newBufferSize = (int) (previousBufferSize * BUFFER_SIZE_INCREASE.getValue());
      } else {
        newBufferSize = (int) (previousBufferSize * BUFFER_SIZE_INCREASE_FIRST.getValue());
      }
    }
    suggestedParameter.put(SORT_BUFFER_HADOOP_CONF.getValue(), newBufferSize * 1.0);
    suggestedParameter.put(SORT_SPILL_HADOOP_CONF.getValue(), newSpillPercentage * 1.0);
    logDebuggingStatement(" Previous Buffer " + previousBufferSize, " Previous Split " + previousSortSpill,
        "Ratio of disk spills to output records " + ratioOfDiskSpillsToOutputRecords,
        "New Buffer Size " + newBufferSize * 1.0, " New Buffer Percentage " + newSpillPercentage);
    modifyMapperMemory();
  }

  private void modifyMapperMemory() {
    Double mapperMemory = suggestedParameter.get(MAPPER_MEMORY_HADOOP_CONF.getValue()) == null ? Double.parseDouble(
        appliedParameter.get(MAPPER_MEMORY_HEURISTIC)) : suggestedParameter.get(MAPPER_MEMORY_HADOOP_CONF.getValue());
    Double sortBuffer = suggestedParameter.get(SORT_BUFFER_HADOOP_CONF.getValue());
    Double minimumMemoryBasedonSortBuffer =
        max(sortBuffer + SORT_BUFFER_CUSHION.getValue(), sortBuffer * MINIMUM_MEMORY_SORT_BUFFER_RATIO.getValue());
    if (minimumMemoryBasedonSortBuffer > mapperMemory) {
      mapperMemory = minimumMemoryBasedonSortBuffer;
      suggestedParameter.put(MAPPER_MEMORY_HADOOP_CONF.getValue(), TuningHelper.getContainerSize(mapperMemory));
      Double heapMemory = suggestedParameter.get(MAPPER_HEAP_HADOOP_CONF.getValue());
      if (heapMemory != null) {
        heapMemory =
            TuningHelper.getHeapSize(min(HEAPSIZE_TO_MAPPERMEMORY_SAFE_RATIO.getValue() * mapperMemory, heapMemory));
        suggestedParameter.put(MAPPER_HEAP_HADOOP_CONF.getValue(), heapMemory);
      } else {
        suggestedParameter.put(MAPPER_HEAP_HADOOP_CONF.getValue(),
            TuningHelper.getHeapSize(HEAPSIZE_TO_MAPPERMEMORY_SAFE_RATIO.getValue() * mapperMemory));
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
        logger.debug(log);
      }
    }
  }
}