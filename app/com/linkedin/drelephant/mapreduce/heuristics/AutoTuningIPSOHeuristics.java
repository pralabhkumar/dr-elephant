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

import com.linkedin.drelephant.analysis.HDFSContext;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.mapreduce.data.AutoTuningIPSOMemoryData;
import com.linkedin.drelephant.mapreduce.data.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceCounterData;
import com.linkedin.drelephant.mapreduce.data.MapReduceCounterData.CounterName;
import com.linkedin.drelephant.mapreduce.data.MapReduceTaskData;
import com.linkedin.drelephant.math.Statistics;
import com.linkedin.drelephant.util.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.linkedin.drelephant.mapreduce.heuristics.CommnConstantsMRAutoTuningIPSOHeuristics.*;

import org.apache.log4j.Logger;


//import org.datanucleus.store.rdbms.query.AbstractRDBMSQueryResult;
/*
Heuristics to collect memory data/counter values  about application previous exeution.
 */
public class AutoTuningIPSOHeuristics implements Heuristic<MapReduceApplicationData> {
  private static final Logger logger = Logger.getLogger(AutoTuningIPSOHeuristics.class);

  // Severity parameters.
  private static final String DISK_SPEED_SEVERITY = "disk_speed_severity";
  private static final String RUNTIME_SEVERITY = "runtime_severity_in_min";

  private Map<String, String> assignedParameterValues = null;

  // Default value of parameters
  private double[] diskSpeedLimits = {1d / 2, 1d / 4, 1d / 8, 1d / 32};  // Fraction of HDFS block size
  private double[] runtimeLimits = {5, 10, 15, 30};              // The Map task runtime in milli sec

  private List<CounterName> _counterNames =
      Arrays.asList(MapReduceCounterData.CounterName.HDFS_BYTES_READ, MapReduceCounterData.CounterName.S3_BYTES_READ,
          MapReduceCounterData.CounterName.S3A_BYTES_READ, MapReduceCounterData.CounterName.S3N_BYTES_READ);

  private HeuristicConfigurationData _heuristicConfData;

  private void loadParameters() {

    Map<String, String> paramMap = _heuristicConfData.getParamMap();
    String heuristicName = _heuristicConfData.getHeuristicName();

    double[] confDiskSpeedThreshold = Utils.getParam(paramMap.get(DISK_SPEED_SEVERITY), diskSpeedLimits.length);
    if (confDiskSpeedThreshold != null) {
      diskSpeedLimits = confDiskSpeedThreshold;
    }
    logger.info(heuristicName + " will use " + DISK_SPEED_SEVERITY + " with the following threshold settings: " + Arrays
        .toString(diskSpeedLimits));
    for (int i = 0; i < diskSpeedLimits.length; i++) {
      diskSpeedLimits[i] = diskSpeedLimits[i] * HDFSContext.DISK_READ_SPEED;
    }

    double[] confRuntimeThreshold = Utils.getParam(paramMap.get(RUNTIME_SEVERITY), runtimeLimits.length);
    if (confRuntimeThreshold != null) {
      runtimeLimits = confRuntimeThreshold;
    }
    logger.info(
        heuristicName + " will use " + RUNTIME_SEVERITY + " with the following threshold settings: " + Arrays.toString(
            runtimeLimits));
    for (int i = 0; i < runtimeLimits.length; i++) {
      runtimeLimits[i] = runtimeLimits[i] * Statistics.MINUTE_IN_MS;
    }
  }

  public AutoTuningIPSOHeuristics(HeuristicConfigurationData heuristicConfData) {
    this._heuristicConfData = heuristicConfData;
    //loadParameters();
  }

  @Override
  public HeuristicConfigurationData getHeuristicConfData() {
    return _heuristicConfData;
  }

  @Override
  public HeuristicResult apply(MapReduceApplicationData data) {
    if (!data.getSucceeded()) {
      return null;
    }
    setAssignedParameterValues(data);
    MapReduceTaskData[] mapTasks = data.getMapperData();
    MapReduceTaskData[] reduceTasks = data.getReducerData();
    logger.info("Number of map tasks " + mapTasks.length);
    logger.info("Number of reduce tasks " + reduceTasks.length);
    AutoTuningIPSOMemoryData memoryUsageDataForMap = collectMemoryUsageData(mapTasks, "map");
    AutoTuningIPSOMemoryData memoryUsageDataForReduce = collectMemoryUsageData(reduceTasks, "reduce");
    logger.info(" Memory Usage Data For Map " + memoryUsageDataForMap);
    logger.info(" Memory Usage Data For Reduce " + memoryUsageDataForReduce);
    return createHeuristicWithMemoryData(memoryUsageDataForMap, memoryUsageDataForReduce);
  }

  private void setAssignedParameterValues(MapReduceApplicationData data) {
    assignedParameterValues = new HashMap<String, String>();
    assignedParameterValues.put(ASSIGNED_PARAMETER_KEYS.MAPPER_MEMORY.getValue(),
        data.getConf().getProperty(ASSIGNED_PARAMETER_KEYS.MAPPER_MEMORY.getValue()));
    assignedParameterValues.put(ASSIGNED_PARAMETER_KEYS.REDUCER_MEMORY.getValue(),
        data.getConf().getProperty(ASSIGNED_PARAMETER_KEYS.REDUCER_MEMORY.getValue()));
    String heapMemoryMapper = data.getConf().getProperty(ASSIGNED_PARAMETER_KEYS.MAPPER_HEAP_MEMORY.getValue());
    String heapMemoryReducer = data.getConf().getProperty(ASSIGNED_PARAMETER_KEYS.REDUCER_HEAP_MEMORY.getValue());
    if (heapMemoryMapper == null) {
      heapMemoryMapper = data.getConf().getProperty(ASSIGNED_PARAMETER_KEYS.CHILD_HEAP_MEMORY.getValue());
    }
    if (heapMemoryReducer == null) {
      heapMemoryReducer = data.getConf().getProperty(ASSIGNED_PARAMETER_KEYS.CHILD_HEAP_MEMORY.getValue());
    }
    assignedParameterValues.put(ASSIGNED_PARAMETER_KEYS.MAPPER_HEAP_MEMORY.getValue(),
        heapMemoryMapper.replaceAll("\\s+", "\n"));
    assignedParameterValues.put(ASSIGNED_PARAMETER_KEYS.REDUCER_HEAP_MEMORY.getValue(),
        heapMemoryReducer.replaceAll("\\s+", "\n"));
  }

  private AutoTuningIPSOMemoryData collectMemoryUsageData(MapReduceTaskData[] tasks, String funtionType) {
    AutoTuningIPSOMemoryData memoryUsageData = new AutoTuningIPSOMemoryData(funtionType);
    MapReduceCounterData counters = null;
    for (MapReduceTaskData task : tasks) {
      if (task.isTimeAndCounterDataPresent()) {
        logger.info("Adding task " + task.getTaskId());
        counters = task.getCounters();
        memoryUsageData.addPhysicalMemoryBytes(counters.get(CounterName.PHYSICAL_MEMORY_BYTES));
        memoryUsageData.addVirtualMemoryBytes(counters.get(CounterName.VIRTUAL_MEMORY_BYTES));
        memoryUsageData.addTotalCommittedHeapBytes(counters.get(CounterName.COMMITTED_HEAP_BYTES));
      }
    }
    return memoryUsageData;
  }

  private HeuristicResult createHeuristicWithMemoryData(AutoTuningIPSOMemoryData mapData,
      AutoTuningIPSOMemoryData reduceData) {
    HeuristicResult result =
        new HeuristicResult(AUTO_TUNING_IPSO_HEURISTICS, AUTO_TUNING_IPSO_HEURISTICS, Severity.LOW, 0);

    addResultDetail(result, mapData.getPhysicalMemoryBytes(),
        UTILIZED_PARAMETER_KEYS.MAX_MAP_PHYSICAL_MEMORY_BYTES.getValue());
    addResultDetail(result, mapData.getVirtualMemoryBytes(),
        UTILIZED_PARAMETER_KEYS.MAX_MAP_VIRTUAL_MEMORY_BYTES.getValue());
    addResultDetail(result, mapData.getTotalCommittedHeapBytes(),
        UTILIZED_PARAMETER_KEYS.MAX_MAP_TOTAL_COMMITTED_MEMORY_BYTES.getValue());
    addResultDetail(result, reduceData.getPhysicalMemoryBytes(),
        UTILIZED_PARAMETER_KEYS.MAX_REDUCE_PHYSICAL_MEMORY_BYTES.getValue());
    addResultDetail(result, reduceData.getVirtualMemoryBytes(),
        UTILIZED_PARAMETER_KEYS.MAX_REDUCE_VIRTUAL_MEMORY_BYTES.getValue());
    addResultDetail(result, reduceData.getTotalCommittedHeapBytes(),
        UTILIZED_PARAMETER_KEYS.MAX_REDUCE_TOTAL_COMMITTED_MEMORY_BYTES.getValue());

    result.addResultDetail(" Requested Mapper Memory ",
        assignedParameterValues.get(ASSIGNED_PARAMETER_KEYS.MAPPER_MEMORY.getValue()));
    result.addResultDetail(" Requested Reducer Memory ",
        assignedParameterValues.get(ASSIGNED_PARAMETER_KEYS.REDUCER_MEMORY.getValue()));
    result.addResultDetail(" Requested Mapper XMX ",
        assignedParameterValues.get(ASSIGNED_PARAMETER_KEYS.MAPPER_HEAP_MEMORY.getValue()));
    result.addResultDetail(" Requested Reducer XMX ",
        assignedParameterValues.get(ASSIGNED_PARAMETER_KEYS.REDUCER_HEAP_MEMORY.getValue()));
    return result;
  }

  private void addResultDetail(HeuristicResult result, List<Long> values, String name) {
    if (values.size() > 0) {
      Long maxValue = Collections.max(values);
      result.addResultDetail(name, maxValue + "");
    }
  }
}
