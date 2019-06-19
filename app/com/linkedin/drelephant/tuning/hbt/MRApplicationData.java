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
import com.linkedin.drelephant.mapreduce.heuristics.MapperMemoryHeuristic;
import com.linkedin.drelephant.mapreduce.heuristics.MapperSpillHeuristic;
import com.linkedin.drelephant.mapreduce.heuristics.MapperTimeHeuristic;
import com.linkedin.drelephant.mapreduce.heuristics.ReducerMemoryHeuristic;
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
  private Map<String, Double> appliedParameter = null;
  private Map<String, Double> counterValues = null;

  // todo: Reducer time is not valid heuristic , since we have to find ways to change number of reducer from azkaban.
  // mapreduce.job.reduces, pig.exec.reducers.bytes.per.reducer have not worked from azkaban
  static {
    validHeuristic = new HashSet<String>();
    validHeuristic.add(MAPPER_TIME_HEURISTIC);
    validHeuristic.add(MAPPER_MEMORY_HEURISTIC);
    validHeuristic.add(MAPPER_SPILL_HEURISTIC);
    validHeuristic.add(REDUCER_MEMORY_HEURISTIC);
    MRConstant.MRConfigurationBuilder.buildConfigurations(ElephantContext.instance().getAutoTuningConf());
  }

  /**
   *
   * @param result : Appresults : It contains application data.
   * @param appliedParameter :  Last applied parameter.
   * It process the application for suggested parameter
   */
  MRApplicationData(AppResult result, Map<String, Double> appliedParameter) {
    logger.info(" Process for following app id " + result.id);
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
   *Suggested parameter will suggest the parameters based on current application , but
   *if the no parameters are suggested , then fill the suggested parameter with previous value.
   * If no parameters are suggested , then it means heuristics are working fine
   * with previous applied parameters and hence those parameters will again be suggested
   * for this application
   * Fill the Mapper parameter , with previous values ,if not suggested
   * Fill reducer values if not suggested , only in the case reducer is there in the application
   */
  private void postFillSuggestedParameterWithAppliedParameter() {
    int numberOfReducer = getPreviousNumberOfReducer();
    postFillReducerParameter(numberOfReducer);
    postFillMapperParameter();
    if (debugEnabled) {
      for (String parameterName : suggestedParameter.keySet()) {
        logger.debug(" Suggested Parameter Test " + parameterName + "\t" + suggestedParameter.get(parameterName));
      }
    }
  }

  /**
   * This method will fill the reducer releated parameters from applied parameters ,
   * if they are not suggested .
   * @param numberOfReducer : Number of reducer of the application
   */
  private void postFillReducerParameter(int numberOfReducer) {
    if (numberOfReducer > 0) {
      if (!suggestedParameter.containsKey(REDUCER_MEMORY_HADOOP_CONF.getValue())) {
        suggestedParameter.put(REDUCER_MEMORY_HADOOP_CONF.getValue(),
            appliedParameter.get(REDUCER_MEMORY_HEURISTICS_CONF.getValue()));
      }
      if (!suggestedParameter.containsKey(REDUCER_HEAP_HADOOP_CONF.getValue())) {
        suggestedParameter.put(REDUCER_HEAP_HADOOP_CONF.getValue(),
            appliedParameter.get(REDUCER_HEAP_HEURISTICS_CONF.getValue()));
      }
    } else {
      suggestedParameter.put(REDUCER_MEMORY_HADOOP_CONF.getValue(), 0.0);
      suggestedParameter.put(REDUCER_HEAP_HADOOP_CONF.getValue(), 0.0);
    }
  }

  /**
   * Fill mapper releated parameters.
   */
  private void postFillMapperParameter() {
    if (!suggestedParameter.containsKey(MAPPER_MEMORY_HADOOP_CONF.getValue())) {
      suggestedParameter.put(MAPPER_MEMORY_HADOOP_CONF.getValue(),
          appliedParameter.get(MAPPER_MEMORY_HEURISTICS_CONF.getValue()));
    }
    if (!suggestedParameter.containsKey(MAPPER_HEAP_HADOOP_CONF.getValue())) {
      suggestedParameter.put(MAPPER_HEAP_HADOOP_CONF.getValue(),
          appliedParameter.get(MAPPER_HEAP_HEURISTICS_CONF.getValue()));
    }

    if (!suggestedParameter.containsKey(SORT_BUFFER_HADOOP_CONF.getValue())) {
      suggestedParameter.put(SORT_BUFFER_HADOOP_CONF.getValue(),
          appliedParameter.get(SORT_BUFFER_HEURISTICS_CONF.getValue()));
    }
    if (!suggestedParameter.containsKey(SORT_SPILL_HADOOP_CONF.getValue())) {
      suggestedParameter.put(SORT_SPILL_HADOOP_CONF.getValue(),
          appliedParameter.get(SORT_SPILL_HEURISTICS_CONF.getValue()));
    }
    postFillSplitSize();
  }

  /**
   * This method will get number of reducers from Reducer Memory
   * Heuristic
   * @return Number of Reducer
   */
  private int getPreviousNumberOfReducer() {
    String numberOfReducerText = this._result.getHeuristicsResultDetailsMap()
        .get(ReducerMemoryHeuristic.class.getCanonicalName() + "_" + NUMBER_OF_TASK.getValue());
    logger.debug(" Number of reducer  " + numberOfReducerText);
    int numOfReducer = 0;
    try {
      numOfReducer = Integer.parseInt(numberOfReducerText.trim());
    } catch (NumberFormatException e) {
      logger.error(" Error parsing reducer , hence assuming number of reducer 0");
    }
    return numOfReducer;
  }

  /**
   * If Algorithm have not suggested any split size  this time ,
   * then check for previously applied split size . If previous applied split size
   * is there in pig.max then put the same in suggested.
   * If prevoius applied split size is there mapreduce.split , then put the same in suggested.
   *
   */
  private void postFillSplitSize() {

    if (!suggestedParameter.containsKey(PIG_SPLIT_SIZE_HADOOP_CONF.getValue())) {
      if (appliedParameter.containsKey(PIG_MAX_SPLIT_SIZE_HEURISTICS_CONF.getValue())) {
        suggestedParameter.put(PIG_SPLIT_SIZE_HADOOP_CONF.getValue(),
            appliedParameter.get(PIG_MAX_SPLIT_SIZE_HEURISTICS_CONF.getValue()));
        suggestedParameter.put(SPLIT_SIZE_HADOOP_CONF.getValue(),
            appliedParameter.get(PIG_MAX_SPLIT_SIZE_HEURISTICS_CONF.getValue()));
      } else if (appliedParameter.containsKey(SPLIT_SIZE_HEURISTICS_CONF.getValue())) {
        suggestedParameter.put(PIG_SPLIT_SIZE_HADOOP_CONF.getValue(),
            appliedParameter.get(SPLIT_SIZE_HEURISTICS_CONF.getValue()));
        suggestedParameter.put(SPLIT_SIZE_HADOOP_CONF.getValue(),
            appliedParameter.get(SPLIT_SIZE_HEURISTICS_CONF.getValue()));
      }
    }
  }

  /**
   * Check for the failed Heuristics . Fix all the heuristics
   * by suggesting new parameters .
   * Even if there are no heuristics failure , then try to optimize for memory.
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
      for (Function_Name value : Function_Name.values()) {
        processForMemory(memoryHeuristics.get(value.name()), value.name());
      }
    }
    else if(isOnlySpecificTypeHeuristicsFailed(Reducer.name())) {
      logger.info(" No Reducer specific heuristic failed , hence trying to optimize memory for reducer ");
      processForMemory(memoryHeuristics.get(Reducer.name()),Reducer.name());
    }
    else if(isOnlySpecificTypeHeuristicsFailed(Mapper.name())) {
      logger.info(" No Mapper specific heuristic failed , hence trying to optimize memory for Mapper ");
      processForMemory(memoryHeuristics.get(Mapper.name()),Mapper.name());
    }
  }

  private boolean isOnlySpecificTypeHeuristicsFailed(String functionName){
    for(String heuristicName : failedHeuristics.keySet()){
      if(heuristicName.toLowerCase().contains(functionName.toLowerCase())){
        return false;
      }
    }
    return true;
  }

  public Map<String, Double> getSuggestedParameter() {
    return this.suggestedParameter;
  }

  /**
   * Only if the Heuristics severity is greater than 2 , then try to tune it.
   * @param yarnAppHeuristicResult : Heuristics details for the application
   * @return : If its valid heuristic for tuning.
   */
  private boolean isValidHeuristic(AppHeuristicResult yarnAppHeuristicResult) {
    if (validHeuristic.contains(yarnAppHeuristicResult.heuristicName)
        && yarnAppHeuristicResult.severity.getValue() > HEURISTIC_THRESHOLD) {
      return true;
    }
    return false;
  }

  /**
   * This method ,based on the heuristic failure , will call respective methods
   * for tuning the heuristic .
   *
   * Since Dr elephant runs on Java 1.5 , switch case is not used.
   * Not able to set number of reducer from Azkaban and hence not optimizing for
   * Reducer time
   * @param yarnAppHeuristicResult : App heuristics results.
   * Blanked Exception is caught ,since even if some unknown exception
   * comes , system should work as it is . In the case of unknown exception
   * previous applied parameters will be applied .
   */
  private void processHeuristics(AppHeuristicResult yarnAppHeuristicResult) {
    logger.info("Fixing Heuristics " + yarnAppHeuristicResult.heuristicName);
    try {
      if (yarnAppHeuristicResult.heuristicName.equals(MAPPER_MEMORY_HEURISTIC)) {
        processForMemory(yarnAppHeuristicResult, Mapper.name());
      } else if (yarnAppHeuristicResult.heuristicName.equals(REDUCER_MEMORY_HEURISTIC)) {
        processForMemory(yarnAppHeuristicResult, Reducer.name());
      } else if (yarnAppHeuristicResult.heuristicName.equals(MAPPER_TIME_HEURISTIC)) {
        processForNumberOfTask(yarnAppHeuristicResult, Mapper.name());
      } else if (yarnAppHeuristicResult.heuristicName.equals(MAPPER_SPILL_HEURISTIC)) {
        processForMemoryBuffer(yarnAppHeuristicResult);
      }
    } catch (Exception e) {
      logger.error(" Exception while processing heuristic ", e);
    }
  }

  /**
   * Process for Mapper Memory and Heap and reducer memory and heap.
   * Mapper Memory / Mapper Heap / Reducer Memory /Reducer heap
   * These values are decided based on maximum virtual memory used,
   * maximum physical memory used and maximum total committed heap usage.
   * Get previous used parameters ,
   * check if the previous used parameters are parsed successfully
   * suggest new parameters based on previous used parameters
   * @param yarnAppHeuristicResult : Heuristic details
   * @param functionType : Function types (Either mapper or reducer)
   */
  private void processForMemory(AppHeuristicResult yarnAppHeuristicResult, String functionType) {
    Map<String, String> heuristicsResults = yarnAppHeuristicResult.yarnAppResult.getHeuristicsResultDetailsMap();
    double usedPhysicalMemoryMB =
        getPreviousUsedMaxMemory(heuristicsResults, functionType, MAX_PHYSICAL_MEMORY.getValue());
    double usedVirtualMemoryMB =
        getPreviousUsedMaxMemory(heuristicsResults, functionType, MAX_VIRTUAL_MEMORY.getValue());
    double usedHeapMemoryMB =
        getPreviousUsedMaxMemory(heuristicsResults, functionType, MAX_TOTAL_COMMITTED_HEAP_USAGE_MEMORY.getValue());
    addCounterData(new String[]{functionType + " " + MAX_VIRTUAL_MEMORY.getValue(),
            functionType + " " + MAX_PHYSICAL_MEMORY.getValue(),
            functionType + " " + MAX_TOTAL_COMMITTED_HEAP_USAGE_MEMORY.getValue()}, usedVirtualMemoryMB,
        usedPhysicalMemoryMB, usedHeapMemoryMB);
    if (isMemoryPrerequisiteQualify(usedPhysicalMemoryMB)) {
      calculateAndUpdateNewValuesForMemory(usedPhysicalMemoryMB, usedVirtualMemoryMB, usedHeapMemoryMB, functionType,
          yarnAppHeuristicResult);
    }
  }

  /**
   * This method calculates new parameter for mapper and reducer based on
   * used parameter .
   * @param usedPhysicalMemoryMB : Previous used max physical memory
   * @param usedVirtualMemoryMB : Previous used max virtual memory
   * @param usedHeapMemoryMB : Previous used max Heap Memory
   * @param functionType : Function type
   * @param yarnAppHeuristicResult : Heuristic details
   */
  private void calculateAndUpdateNewValuesForMemory(double usedPhysicalMemoryMB, double usedVirtualMemoryMB,
      double usedHeapMemoryMB, String functionType, AppHeuristicResult yarnAppHeuristicResult) {
    double memoryMB =
        max(usedPhysicalMemoryMB, usedVirtualMemoryMB / (VIRTUALMEMORY_TO_PHYSICALMEMORY_RATIO.getValue()));
    double heapSizeMax =
        TuningHelper.getHeapSize(min(HEAPSIZE_TO_MAPPERMEMORY_SAFE_RATIO.getValue() * memoryMB, usedHeapMemoryMB));
    double containerSize = TuningHelper.getContainerSize(memoryMB);
    addParameterToSuggestedParameter(heapSizeMax, containerSize, yarnAppHeuristicResult.yarnAppResult.id, functionType);

    logDebuggingStatement(" Used Physical Memory " + yarnAppHeuristicResult.yarnAppResult.id + "_" + functionType + " "
            + usedPhysicalMemoryMB,
        " Used Virtual Memory " + yarnAppHeuristicResult.yarnAppResult.id + "_" + functionType + " "
            + usedVirtualMemoryMB,
        " Used heap Memory " + yarnAppHeuristicResult.yarnAppResult.id + "_" + functionType + " " + usedHeapMemoryMB);
  }

  /**
   *
   * @param usedPhysicalMemoryMB : Previous used memory
   * @return : If previous used memory is greater then zero , then return true
   * and suggest for new parameters
   */
  private boolean isMemoryPrerequisiteQualify(double usedPhysicalMemoryMB) {
    if (usedPhysicalMemoryMB > 0) {
      return true;
    } else {
      logger.info(" Unable to parse previous memory used parameter");
      return false;
    }
  }

  /**
   *
   * @param heuristicsResults : Contains all the details about heuristics
   * @param functionType : Mapper or Reducer
   * @param parameterName : Getting either physical memory ,virtual memory or heap
   * @return : parameter value
   */
  private double getPreviousUsedMaxMemory(Map<String, String> heuristicsResults, String functionType,
      String parameterName) {
    double maxUsedMemory = 0.0;
    try {
      if (functionType.equals(Mapper.name())) {
        if (heuristicsResults.containsKey(MapperMemoryHeuristic.class.getCanonicalName() + "_" + parameterName)) {
          maxUsedMemory = (double) MemoryFormatUtils.stringToBytes(
              heuristicsResults.get(MapperMemoryHeuristic.class.getCanonicalName() + "_" + parameterName));
        }
      } else {
        if (heuristicsResults.containsKey(ReducerMemoryHeuristic.class.getCanonicalName() + "_" + parameterName)) {
          maxUsedMemory = (double) MemoryFormatUtils.stringToBytes(
              heuristicsResults.get(ReducerMemoryHeuristic.class.getCanonicalName() + "_" + parameterName));
        }
      }
    } catch (IllegalArgumentException e) {
      logger.error(" Exception while parsing value to bytes . System should continue to work  ", e);
    }
    return maxUsedMemory;
  }

  /**
   * Add suggested parameter to Map
   * @param heapSizeMax : Max heap size suggested
   * @param containerSize : container size
   * @param id : Applicationn type
   * @param functionType : Mapper or Reducer
   */
  private void addParameterToSuggestedParameter(Double heapSizeMax, Double containerSize, String id,
      String functionType) {
    if (functionType.equals(Mapper.name())) {
      addMapperMemoryAndHeapToSuggestedParameter(heapSizeMax, containerSize, id);
    } else {
      addReducerMemoryAndHeapToSuggestedParameter(heapSizeMax, containerSize, id);
    }
  }

  /**
   * Add MApper suggested parameter to Map
   * @param heapSizeMax
   * @param containerSize
   * @param heuristicsResultID
   */
  private void addMapperMemoryAndHeapToSuggestedParameter(Double heapSizeMax, Double containerSize,
      String heuristicsResultID) {
    suggestedParameter.put(MAPPER_MEMORY_HADOOP_CONF.getValue(), containerSize);
    suggestedParameter.put(MAPPER_HEAP_HADOOP_CONF.getValue(), heapSizeMax);
    logDebuggingStatement(" Memory Assigned " + heuristicsResultID + "_Mapper " + suggestedParameter.get(
        MAPPER_MEMORY_HADOOP_CONF.getValue()),
        " Heap Assigned " + heuristicsResultID + "_Mapper " + suggestedParameter.get(
            MAPPER_HEAP_HADOOP_CONF.getValue()));
  }

  /**
   * Add reducer suggested parameter to Map
   * @param heapSizeMax
   * @param containerSize
   * @param heuristicsResultID
   */
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
    } /*else if (functionType.equals(Reducer.name())) {
      processForNumberOfTaskReducer(yarnAppHeuristicResult);
    }*/
  }

  /**
   * IF the mapper time heuristics is failed , its failed because either the
   * average input going to one task is too small or too large .
   * If its too small , increase the average task input size and that would be the
   * split size . If time is too big , decrease the average task input size and that would
   * be split size.
   * @param yarnAppHeuristicResult : Heuristic details about the application
   * @return : New split size
   */
  private void processForNumberOfTaskMapper(AppHeuristicResult yarnAppHeuristicResult) {
    logger.info("Calculating Split Size ");
    Map<String, String> heuristicsResults = yarnAppHeuristicResult.yarnAppResult.getHeuristicsResultDetailsMap();
    double averageTaskInputSize = getPreviousAverageTaskInputSize(heuristicsResults);
    double averageTaskTimeInMinute = getPreviousAverageTaskRunTime(heuristicsResults);
    addCounterData(
        new String[]{Mapper + " " + AVERAGE_TASK_INPUT_SIZE.getValue(), Mapper + " " + AVERAGE_TASK_RUNTIME.getValue()},
        averageTaskInputSize, averageTaskTimeInMinute);
    if (isSplitSizePrerequisiteQualify(averageTaskInputSize, averageTaskTimeInMinute)) {
      calculateAndUpdateNewValuesForSplitSize(averageTaskInputSize, averageTaskTimeInMinute);
    }
  }

  /**
   * Calcualte and update new split size
   * @param averageTaskInputSize
   * @param averageTaskTimeInMinute
   */
  private void calculateAndUpdateNewValuesForSplitSize(double averageTaskInputSize, double averageTaskTimeInMinute) {
    long newSplitSize = 0L;
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
    if (newSplitSize > 0) {
      suggestedParameter.put(SPLIT_SIZE_HADOOP_CONF.getValue(), newSplitSize * 1.0);
      suggestedParameter.put(PIG_SPLIT_SIZE_HADOOP_CONF.getValue(), newSplitSize * 1.0);
    }
  }

  private boolean isSplitSizePrerequisiteQualify(double averageTaskInputSize, double averageTaskTimeInMinute) {
    if (averageTaskInputSize > 0 && averageTaskTimeInMinute > 0) {
      return true;
    } else {
      logger.info(" Unable to parse previous Split records ");
      return false;
    }
  }

  /**
   *  Get Average Task Input Size from Mapper Time Heuristics .
   * @param heuristicsResults
   * @return
   */
  private double getPreviousAverageTaskInputSize(Map<String, String> heuristicsResults) {
    double averageTaskInputSize = 0.0;
    try {
      if (heuristicsResults.containsKey(
          MapperTimeHeuristic.class.getCanonicalName() + "_" + AVERAGE_TASK_INPUT_SIZE.getValue())) {
        averageTaskInputSize = (double) MemoryFormatUtils.stringToBytes(heuristicsResults.get(
            MapperTimeHeuristic.class.getCanonicalName() + "_" + AVERAGE_TASK_INPUT_SIZE.getValue()));
      }
    } catch (NumberFormatException e) {
      logger.error("Unable to parse Average Input Size  ", e);
    } catch (Exception e) {
      logger.error(" Unknown error have come while parsing average input size ", e);
    }
    return averageTaskInputSize;
  }

  /**
   * Get previous task run time from Mapper Time Heuristics
   * @param heuristicsResults
   * @return
   */
  private double getPreviousAverageTaskRunTime(Map<String, String> heuristicsResults) {
    double averageTaskRunTime = 0.0;
    try {
      if (heuristicsResults.containsKey(
          MapperTimeHeuristic.class.getCanonicalName() + "_" + AVERAGE_TASK_RUNTIME.getValue())) {
        averageTaskRunTime = TuningHelper.getTimeInMinute(heuristicsResults.get(
            MapperTimeHeuristic.class.getCanonicalName() + "_" + AVERAGE_TASK_RUNTIME.getValue()));
      }
    } catch (NumberFormatException e) {
      logger.error("Unable to parse Average Task Run Time  ", e);
    } catch (Exception e) {
      logger.error(" Unknown error have come while parsing average task run time  ", e);
    }
    return averageTaskRunTime;
  }

  /**
   * This will change value of sort buffer and sort spill
   * based on ratio of spill records to output records.
   * If buffer size is changed , then mapper memory should also be modified.
   * @param yarnAppHeuristicResult
   */
  private void processForMemoryBuffer(AppHeuristicResult yarnAppHeuristicResult) {
    Map<String, String> heuristicsResults = yarnAppHeuristicResult.yarnAppResult.getHeuristicsResultDetailsMap();
    float ratioOfDiskSpillsToOutputRecords = getRatioOfDiskSpillsToOutputRecords(heuristicsResults);
    int previousBufferSize = getPreviousBufferSize();
    float previousSortSpill = getPreviousSpill();
    addCounterData(new String[]{RATIO_OF_SPILLED_RECORDS_TO_OUTPUT_RECORDS.getValue(), SORT_BUFFER.getValue(),
            SORT_SPILL.getValue()}, ratioOfDiskSpillsToOutputRecords * 1.0, previousBufferSize * 1.0,
        previousSortSpill * 1.0);
    if (isMemoryBufferPrerequisiteQualify(ratioOfDiskSpillsToOutputRecords, previousBufferSize)) {
      calculateAndUpdateNewValuesForBuffer(ratioOfDiskSpillsToOutputRecords, previousBufferSize, previousSortSpill);
    }
  }

  private boolean isMemoryBufferPrerequisiteQualify(float ratioOfDiskSpillsToOutputRecords, int previousBufferSize) {
    if (ratioOfDiskSpillsToOutputRecords > 0 && previousBufferSize > 0) {
      return true;
    } else {
      logger.info(" Unable to parse previous memory used parameter");
      return false;
    }
  }

  /**
   *  This method will calcuate new Buffer values of Mapper . IF the buffer size
   *  changes , then mapper memory will also get modified .
   * @param ratioOfDiskSpillsToOutputRecords : Ratio of Disk Spill to output records
   * @param previousBufferSize : Previous buffer size
   * @param previousSortSpill : Previous spill percentage
   */
  private void calculateAndUpdateNewValuesForBuffer(float ratioOfDiskSpillsToOutputRecords, int previousBufferSize,
      float previousSortSpill) {
    int newBufferSize = 0;
    float newSpillPercentage = 0.0f;
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
    logDebuggingStatement(" Previous Buffer " + previousBufferSize, " Previous Split " + previousSortSpill,
        "Ratio of disk spills to output records " + ratioOfDiskSpillsToOutputRecords,
        "New Buffer Size " + newBufferSize * 1.0, " New Buffer Percentage " + newSpillPercentage);
    if (newBufferSize > 0) {
      suggestedParameter.put(SORT_BUFFER_HADOOP_CONF.getValue(), newBufferSize * 1.0);
      suggestedParameter.put(SORT_SPILL_HADOOP_CONF.getValue(), newSpillPercentage * 1.0);
      modifyMapperMemory();
    }
  }

  private int getPreviousBufferSize() {
    int previousBufferSize = 0;
    if (appliedParameter.containsKey(SORT_BUFFER.getValue())) {
      previousBufferSize = appliedParameter.get(SORT_BUFFER.getValue()).intValue();
    }
    return previousBufferSize;
  }

  private float getPreviousSpill() {
    float previousSpill = 0.0f;
    if (appliedParameter.containsKey(SORT_SPILL.getValue())) {
      previousSpill = appliedParameter.get(SORT_SPILL.getValue()).floatValue();
    }
    return previousSpill;
  }

  private float getRatioOfDiskSpillsToOutputRecords(Map<String, String> heuristicsResults) {
    String ratioOfSpillRecordsToOutputRecordsValue = heuristicsResults.get(
        MapperSpillHeuristic.class.getCanonicalName() + "_" + RATIO_OF_SPILLED_RECORDS_TO_OUTPUT_RECORDS.getValue());
    float ratioOfDiskSpillsToOutputRecords = 0.0f;
    try {
      if (ratioOfSpillRecordsToOutputRecordsValue != null) {
        ratioOfDiskSpillsToOutputRecords = Float.parseFloat(ratioOfSpillRecordsToOutputRecordsValue);
      }
    } catch (NumberFormatException e) {
      logger.error("Error parsing ration of spill records to output records ", e);
    }
    return ratioOfDiskSpillsToOutputRecords;
  }

  private void modifyMapperMemory() {
    Double mapperMemory = suggestedParameter.get(MAPPER_MEMORY_HADOOP_CONF.getValue()) == null ? appliedParameter.get(
        MAPPER_MEMORY_HEURISTIC) : suggestedParameter.get(MAPPER_MEMORY_HADOOP_CONF.getValue());
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

  /**
   * Adding all the counters for testing
   * @param counterNames
   * @param counterValue
   */
  private void addCounterData(String[] counterNames, Double... counterValue) {
    for (int i = 0; i < counterNames.length; i++) {
      counterValues.put(counterNames[i], counterValue[i]);
    }
  }

  /**
   * Debug statements .
   * @param statements
   */
  private void logDebuggingStatement(String... statements) {
    if (debugEnabled) {
      for (String log : statements) {
        logger.debug(log);
      }
    }
  }

  // Todo : This will be required once passing number of reducer from schduler figures out.
  //
  /*private void processForNumberOfTaskReducer(AppHeuristicResult yarnAppHeuristicResult) {
    long numberOfReduceTask;
    numberOfReduceTask = getNumberOfReducer(yarnAppHeuristicResult);
    if (numberOfReduceTask > 0) {
      suggestedParameter.put(NUMBER_OF_REDUCER_CONF.getValue(), numberOfReduceTask * 1.0);
    }
  }*/

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
        averageTaskTimeInMinute = TuningHelper.getTimeInMinute(appHeuristicResultDetails.value);
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
}