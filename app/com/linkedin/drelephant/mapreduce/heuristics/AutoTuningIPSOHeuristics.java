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

package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.mapreduce.data.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceCounterData;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.util.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.linkedin.drelephant.analysis.HDFSContext;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.mapreduce.data.MapReduceTaskData;
import com.linkedin.drelephant.mapreduce.data.MapReduceCounterData.CounterName;
import com.linkedin.drelephant.math.Statistics;

import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
//import org.datanucleus.store.rdbms.query.AbstractRDBMSQueryResult;


public class AutoTuningHeuristics implements Heuristic<MapReduceApplicationData> {
  private static final Logger logger = Logger.getLogger(AutoTuningHeuristics.class);

  // Severity parameters.
  private static final String DISK_SPEED_SEVERITY = "disk_speed_severity";
  private static final String RUNTIME_SEVERITY = "runtime_severity_in_min";

  // Default value of parameters
  private double[] diskSpeedLimits = {1d/2, 1d/4, 1d/8, 1d/32};  // Fraction of HDFS block size
  private double[] runtimeLimits = {5, 10, 15, 30};              // The Map task runtime in milli sec

  private List<MapReduceCounterData.CounterName> _counterNames = Arrays.asList(
      MapReduceCounterData.CounterName.HDFS_BYTES_READ,
      MapReduceCounterData.CounterName.S3_BYTES_READ,
      MapReduceCounterData.CounterName.S3A_BYTES_READ,
      MapReduceCounterData.CounterName.S3N_BYTES_READ
  );

  private HeuristicConfigurationData _heuristicConfData;

  private void loadParameters() {

    Map<String, String> paramMap = _heuristicConfData.getParamMap();
    String heuristicName = _heuristicConfData.getHeuristicName();

    double[] confDiskSpeedThreshold = Utils.getParam(paramMap.get(DISK_SPEED_SEVERITY), diskSpeedLimits.length);
    if (confDiskSpeedThreshold != null) {
      diskSpeedLimits = confDiskSpeedThreshold;
    }
    logger.info(heuristicName + " will use " + DISK_SPEED_SEVERITY + " with the following threshold settings: "
        + Arrays.toString(diskSpeedLimits));
    for (int i = 0; i < diskSpeedLimits.length; i++) {
      diskSpeedLimits[i] = diskSpeedLimits[i] * HDFSContext.DISK_READ_SPEED;
    }

    double[] confRuntimeThreshold = Utils.getParam(paramMap.get(RUNTIME_SEVERITY), runtimeLimits.length);
    if (confRuntimeThreshold != null) {
      runtimeLimits = confRuntimeThreshold;
    }
    logger.info(heuristicName + " will use " + RUNTIME_SEVERITY + " with the following threshold settings: " + Arrays
        .toString(runtimeLimits));
    for (int i = 0; i < runtimeLimits.length; i++) {
      runtimeLimits[i] = runtimeLimits[i] * Statistics.MINUTE_IN_MS;
    }
  }

  public AutoTuningHeuristics(HeuristicConfigurationData heuristicConfData) {
    this._heuristicConfData = heuristicConfData;
    //loadParameters();
  }

  @Override
  public HeuristicConfigurationData getHeuristicConfData() {
    return _heuristicConfData;
  }

  @Override
  public HeuristicResult apply(MapReduceApplicationData data) {

    if(!data.getSucceeded()) {
      return null;
    }
    long totalInputByteSize=0;

    MapReduceTaskData[] mapTasks = data.getMapperData();
    MapReduceTaskData[] reduceTasks = data.getReducerData();

    String memoryMapper = data.getConf().getProperty("mapreduce.map.memory.mb");
    String xmxMapper = data.getConf().getProperty("mapreduce.map.java.opts");
    String memoryReducer = data.getConf().getProperty("mapreduce.reduce.memory.mb");
    String xmxReducer = data.getConf().getProperty("mapreduce.reduce.java.opts");

    if(xmxMapper == null) {
      xmxMapper = data.getConf().getProperty("mapred.child.java.opts");
    }

    if(xmxReducer == null) {
      xmxReducer = data.getConf().getProperty("mapred.child.java.opts");
    }

    logger.info("Number of map tasks " + mapTasks.length);
    logger.info("Number of reduce tasks " + reduceTasks.length);





    List<Long> runtimesMs = new ArrayList<Long>();
    List<Long> inputByteSizes = new ArrayList<Long>();


    List<Long> mapPhysicalMemoryBytes = new ArrayList<Long>();
    List<Long> reducePhysicalMemoryBytes = new ArrayList<Long>();
    List<Long> mapVirtualMemoryBytes = new ArrayList<Long>();
    List<Long> reduceVirtualMemoryBytes = new ArrayList<Long>();
    List<Long> mapTotalCommittedHeapBytes = new ArrayList<Long>();
    List<Long> reduceTotalCommittedHeapBytes = new ArrayList<Long>();

    List<Long> mapSpilledRecords = new ArrayList<Long>();
    List<Long> reduceSpilledRecords = new ArrayList<Long>();
    List<Long> reduceShuffleBytes = new ArrayList<Long>();
    List<Long> mapOutputRecords = new ArrayList<Long>();
    List<Long> reduceInputRecords = new ArrayList<Long>();
    List<Long> reduceOutputRecords = new ArrayList<Long>();
    List<Long> mapGcTimeSpent = new ArrayList<Long>();
    List<Long> reduceGcTimeSpent = new ArrayList<Long>();

  //  List<Double> mapRatioSpillOutput = new ArrayList<Double>();
  //  List<Double> reduceRationSpillOutput = new ArrayList<Double>();

    MapReduceCounterData counters=null;
    for (MapReduceTaskData task : mapTasks) {
      if (task.isTimeAndCounterDataPresent()) {
        logger.info("Adding a map task " + task.getTaskId());
        counters=task.getCounters();
        long inputBytes = 0;
        for (MapReduceCounterData.CounterName counterName: _counterNames) {
          inputBytes += counters.get(counterName);
        }
        long runtimeMs = task.getTotalRunTimeMs();

        inputByteSizes.add(inputBytes);
        totalInputByteSize += inputBytes;
        runtimesMs.add(runtimeMs);


        mapPhysicalMemoryBytes.add(counters.get(CounterName.PHYSICAL_MEMORY_BYTES));
        mapVirtualMemoryBytes.add(counters.get(CounterName.VIRTUAL_MEMORY_BYTES));
        mapTotalCommittedHeapBytes.add(counters.get(CounterName.COMMITTED_HEAP_BYTES));
      //  mapSpilledRecords.add(counters.get(CounterName.SPILLED_RECORDS));
      //  mapOutputRecords.add(counters.get(CounterName.MAP_OUTPUT_RECORDS));
      //  mapGcTimeSpent.add(counters.get(CounterName.GC_MILLISECONDS));
      //  mapRatioSpillOutput.add(counters.get(CounterName.SPILLED_RECORDS)*1.0/counters.get(CounterName.MAP_OUTPUT_RECORDS)*1.0);


        logger.info("PHYSICAL_MEMORY_BYTES " + counters.get(CounterName.PHYSICAL_MEMORY_BYTES));
        logger.info("VIRTUAL_MEMORY_BYTES " + counters.get(CounterName.VIRTUAL_MEMORY_BYTES));
        logger.info("COMMITTED HEAP BYTES " + counters.get(CounterName.COMMITTED_HEAP_BYTES));
        logger.info("Mapper Memory " + memoryMapper);
        logger.info("Mapper XMX " + xmxMapper);

       // logger.info("SPILLED_RECORDS " + counters.get(CounterName.SPILLED_RECORDS));
       // logger.info("MAP_OUTPUT_RECORDS " + counters.get(CounterName.MAP_OUTPUT_RECORDS));
       // logger.info("GC_MILLISECONDS " + counters.get(CounterName.GC_MILLISECONDS));
       // logger.info("MAP_SPILL_RATIO " + counters.get(CounterName.SPILLED_RECORDS)*1.0/counters.get(CounterName.MAP_OUTPUT_RECORDS)*1.0);
      }
    }
    for (MapReduceTaskData task : reduceTasks) {
      if (task.isTimeAndCounterDataPresent()) {
        logger.info("Adding a reduce task " + task.getTaskId());
        counters=task.getCounters();
        reducePhysicalMemoryBytes.add(counters.get(CounterName.PHYSICAL_MEMORY_BYTES));
        reduceVirtualMemoryBytes.add(counters.get(CounterName.VIRTUAL_MEMORY_BYTES));
        reduceTotalCommittedHeapBytes.add(counters.get(CounterName.COMMITTED_HEAP_BYTES));
     //   reduceSpilledRecords.add(counters.get(CounterName.SPILLED_RECORDS));
     //   reduceOutputRecords.add(counters.get(CounterName.REDUCE_OUTPUT_RECORDS));
     //   reduceGcTimeSpent.add(counters.get(CounterName.GC_MILLISECONDS));
       // reduceRationSpillOutput.add(counters.get(CounterName.SPILLED_RECORDS)*1.0/counters.get(CounterName.MAP_OUTPUT_RECORDS)*1.0);


        logger.info("PHYSICAL_MEMORY_BYTES " + counters.get(CounterName.PHYSICAL_MEMORY_BYTES));
        logger.info("VIRTUAL_MEMORY_BYTES " + counters.get(CounterName.VIRTUAL_MEMORY_BYTES));
        logger.info("COMMITTED HEAP BYTES " + counters.get(CounterName.COMMITTED_HEAP_BYTES));
        logger.info("Reducer Memory " + memoryReducer);
        logger.info("Reducer XMX " + xmxReducer);
      //  logger.info("SPILLED_RECORDS " + counters.get(CounterName.SPILLED_RECORDS));
      //  logger.info("REDUCE_OUTPUT_RECORDS " + counters.get(CounterName.REDUCE_OUTPUT_RECORDS));
      //  logger.info("GC_MILLISECONDS " + counters.get(CounterName.GC_MILLISECONDS));
      //  logger.info("REDUCE_SPILL_RATIO " + counters.get(CounterName.SPILLED_RECORDS)*1.0/counters.get(CounterName.MAP_OUTPUT_RECORDS)*1.0);

      }
    }


    HeuristicResult result = new HeuristicResult("AutoTuningHeuristics",
        "AutoTuningHeuristics", Severity.LOW, 0);


    addResultDetail(result, mapPhysicalMemoryBytes, "Map Physical Memory Bytes");
    addResultDetail(result, mapVirtualMemoryBytes, "Map Virtual Memory Bytes");
    addResultDetail(result, mapTotalCommittedHeapBytes, "Map Total Committed Memory Bytes");
    addResultDetail(result, reducePhysicalMemoryBytes, "Reduce Physical Memory Bytes");
    addResultDetail(result, reduceVirtualMemoryBytes, "Reduce Virtual Memory Bytes");
    addResultDetail(result, reduceTotalCommittedHeapBytes, "Reduce Total Committed Memory Bytes");
    result.addResultDetail(" Requested Mapper Memory ", memoryMapper);
    result.addResultDetail(" Requested Reducer Memory ", memoryReducer);
    result.addResultDetail(" Requested Mapper XMX ", xmxMapper);
    result.addResultDetail(" Requested Reducer XMX ", xmxReducer);

   // addResultDetail(result, mapSpilledRecords, "Map Spilled Records");
   // addResultDetail(result, reduceSpilledRecords, "Reduce Spilled Records");


   // addResultDetail(result, mapOutputRecords, "Map Output Records");
   // addResultDetail(result, reduceOutputRecords, "Reduce Output Records");


    return result;
  }

  private void addResultDetail(HeuristicResult result, List<Long> values, String name)
  {
    if(values.size()>0)
    {
      Long minValue = Collections.min(values);
      Long percentile10Value = Statistics.percentile(values, 10);
      Long percentile25Value = Statistics.percentile(values, 25);
      Long percentile75Value = Statistics.percentile(values, 75);
      Long percentile90Value = Statistics.percentile(values, 90);
      Long maxValue =  Collections.max(values);
     // result.addResultDetail("Min " + name, minValue + "");
     // result.addResultDetail("percentile10 " + name, percentile10Value + "");
      // result.addResultDetail("percentile25 " + name, percentile25Value + "");
      //result.addResultDetail("percentile75 " + name, percentile75Value + "");
      result.addResultDetail("percentile90 " + name, percentile90Value + "");
      result.addResultDetail("max " + name, maxValue + "");
    }

  }

}
