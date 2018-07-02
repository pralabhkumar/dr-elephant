package com.linkedin.drelephant.mapreduce.heuristics;

/*
Constants used in AutoTuningIPSO Heuristics
 */
public class CommnConstantsMRAutoTuningIPSOHeuristics {
  public static final String AUTO_TUNING_IPSO_HEURISTICS = "MapReduceConfiguration";

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
    MAPPER_HEAP_MEMORY("mapreduce.map.java.opts"),
    REDUCER_MEMORY("mapreduce.reduce.memory.mb"),
    REDUCER_HEAP_MEMORY("mapreduce.reduce.java.opts"),
    CHILD_HEAP_MEMORY("mapred.child.java.opts");
    private String value;

    ASSIGNED_PARAMETER_KEYS(String value) {
      this.value = value;
    }

    String getValue() {
      return value;
    }
  }
}
