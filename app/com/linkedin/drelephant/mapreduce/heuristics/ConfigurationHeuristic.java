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

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceApplicationData;

import static com.linkedin.drelephant.mapreduce.heuristics.CommonConstantsHeuristic.ASSIGNED_PARAMETER_KEYS.*;

import org.apache.log4j.Logger;


/*
Heuristics to collect memory data/counter values  about application previous exeution.
 */
public class ConfigurationHeuristic implements Heuristic<MapReduceApplicationData> {
  private static final Logger logger = Logger.getLogger(ConfigurationHeuristic.class);

  private HeuristicConfigurationData _heuristicConfData;

  public ConfigurationHeuristic(HeuristicConfigurationData heuristicConfData) {
    this._heuristicConfData = heuristicConfData;
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
    String mapperMemory = data.getConf().getProperty(MAPPER_MEMORY.getValue());
    String mapperHeap = data.getConf().getProperty(MAPPER_HEAP.getValue());
    if (mapperHeap == null) {
      mapperHeap = data.getConf().getProperty(CHILD_HEAP_SIZE.getValue());
    }
    String sortBuffer = data.getConf().getProperty(SORT_BUFFER.getValue());
    String sortFactor = data.getConf().getProperty(SORT_FACTOR.getValue());
    String sortSplill = data.getConf().getProperty(SORT_SPILL.getValue());
    String reducerMemory = data.getConf().getProperty(REDUCER_MEMORY.getValue());
    String reducerHeap = data.getConf().getProperty(REDUCER_HEAP.getValue());
    if (reducerHeap == null) {
      reducerHeap = data.getConf().getProperty(CHILD_HEAP_SIZE.getValue());
    }
    String splitSize = data.getConf().getProperty(SPLIT_SIZE.getValue());
    String pigSplitSize = data.getConf().getProperty(PIG_SPLIT_SIZE.getValue());
    HeuristicResult result =
        new HeuristicResult(_heuristicConfData.getClassName(), _heuristicConfData.getHeuristicName(), Severity.LOW, 0);

    result.addResultDetail("Mapper Memory", mapperMemory);
    result.addResultDetail("Mapper Heap", mapperHeap.replaceAll("\\s+", "\n"));
    result.addResultDetail("Reducer Memory", reducerMemory);
    result.addResultDetail("Reducer heap", reducerHeap.replaceAll("\\s+", "\n"));
    result.addResultDetail("Sort Buffer", sortBuffer);
    result.addResultDetail("Sort Factor", sortFactor);
    result.addResultDetail("Sort Splill", sortSplill);
    if (splitSize != null) {
      result.addResultDetail("Split Size", splitSize);
    }
    if (pigSplitSize != null) {
      result.addResultDetail("Pig Max Split Size", pigSplitSize);
    }

    return result;
  }
}
