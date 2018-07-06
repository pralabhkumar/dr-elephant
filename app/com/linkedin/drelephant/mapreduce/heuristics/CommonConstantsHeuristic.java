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

public class CommonConstantsHeuristic {

  public static final String MAPPER_SPEED = "Mapper Speed";
  public static final String TOTAL_INPUT_SIZE_IN_MB = "Total input size in MB";

  public enum UTILIZED_PARAMETER_KEYS {
    AVG_PHYSICAL_MEMORY("Avg Physical Memory (MB)"),
    MAX_PHYSICAL_MEMORY("Max Physical Memory (MB)"),
    MIN_PHYSICAL_MEMORY("Min Physical Memory (MB)"),
    AVG_VIRTUAL_MEMORY("Avg Virtual Memory (MB)"),
    MAX_VIRTUAL_MEMORY("Max Virtual Memory (MB)"),
    MIN_VIRTUAL_MEMORY("Min Virtual Memory (MB)"),
    AVG_TOTAL_COMMITTED_HEAP_USAGE_MEMORY("Avg Total Committed Heap Usage Memory (MB)"),
    MAX_TOTAL_COMMITTED_HEAP_USAGE_MEMORY("Max Total Committed Heap Usage Memory (MB)"),
    MIN_TOTAL_COMMITTED_HEAP_USAGE_MEMORY("Min Total Committed Heap Usage Memory (MB)");
    private String value;

    UTILIZED_PARAMETER_KEYS(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }
  }

  public enum ASSIGNED_PARAMETER_KEYS {
    MAPPER_MEMORY("mapreduce.map.memory.mb"),
    MAPPER_HEAP("mapreduce.map.java.opts"),
    SORT_BUFFER("mapreduce.task.io.sort.mb"),
    SORT_FACTOR("mapreduce.task.io.sort.factor"),
    SORT_SPILL("mapreduce.map.sort.spill.percent"),
    REDUCER_MEMORY("mapreduce.reduce.memory.mb"),
    REDUCER_HEAP("mapreduce.reduce.java.opts"),
    SPLIT_SIZE("mapreduce.input.fileinputformat.split.maxsize"),
    CHILD_HEAP_SIZE("mapred.child.java.opts"),
    PIG_SPLIT_SIZE("pig.maxCombinedSplitSize");

    private String value;

    ASSIGNED_PARAMETER_KEYS(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }
  }
}
