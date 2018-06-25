package com.linkedin.drelephant.mapreduce.heuristics;

/*
Constants used in AutoTuningIPSO Heuristics
 */
public class CommnConstantsMRAutoTuningIPSOHeuristics {
  public static final String AUTO_TUNING_IPSO_HEURISTICS = "AutoTuningIPSOHeuristics";

  public enum UTILIZED_PARAMETER_KEYS {
    MAX_MAP_PHYSICAL_MEMORY_BYTES("max Map Physical Memory Bytes"),
    MAX_MAP_TOTAL_COMMITTED_MEMORY_BYTES("max Map Total Committed Memory Bytes"),
    MAX_MAP_VIRTUAL_MEMORY_BYTES("max Map Virtual Memory Bytes"),
    MAX_REDUCE_PHYSICAL_MEMORY_BYTES("max Reduce Physical Memory Bytes"),
    MAX_REDUCE_TOTAL_COMMITTED_MEMORY_BYTES("max Reduce Total Committed Memory Bytes"),
    MAX_REDUCE_VIRTUAL_MEMORY_BYTES("max Reduce Virtual Memory Bytes");
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
