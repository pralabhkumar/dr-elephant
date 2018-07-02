package com.linkedin.drelephant.tuning;

public class IPSOConstants {

  enum PARAMTER_CONSTRAINT {
    MAPPER_MEMORY("mapreduce.map.memory.mb"),
    MAPPER_HEAP_MEMORY("mapreduce.map.java.opts"),
    REDUCER_MEMORY("mapreduce.reduce.memory.mb"),
    REDUCER_HEAP_MEMORY("mapreduce.reduce.java.opts");
    private String value;

    PARAMTER_CONSTRAINT(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

  }

  enum NON_DERIVED_PARAMETER {
    MAPPER_HEAP_MEMORY("mapreduce.map.java.opts"), REDUCER_HEAP_MEMORY("mapreduce.reduce.java.opts");
    private String value;

    NON_DERIVED_PARAMETER(String value) {
      this.value = value;
    }

    public String getValue() {
      return this.value;
    }
  }
}
