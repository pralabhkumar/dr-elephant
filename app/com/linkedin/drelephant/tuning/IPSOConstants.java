package com.linkedin.drelephant.tuning;

public class IPSOConstants {

  enum PARAMTER_CONSTRAINT {
    MAPPER_MEMORY("mapreduce.map.memory.mb", 1, 1024D, 8192D),
    MAPPER_HEAP_MEMORY("mapreduce.map.java.opts", 2, 800D, 6144D),
    REDUCER_MEMORY("mapreduce.reduce.memory.mb", 3, 1024D, 8192D),
    REDUCER_HEAP_MEMORY("mapreduce.reduce.java.opts", 4, 800D, 6144D);
    private String value;
    private Integer constraintID;
    private Double lowerBound;
    private Double upperBound;

    PARAMTER_CONSTRAINT(String value, Integer constraintID, Double lowerBound, Double upperBound) {
      this.value = value;
      this.constraintID = constraintID;
      this.lowerBound = lowerBound;
      this.upperBound = upperBound;
    }

    public String getValue() {
      return value;
    }

    public Integer getConstraintID() {
      return constraintID;
    }

    public Double getLowerBound() {
      return lowerBound;
    }

    public Double getUpperBound() {
      return upperBound;
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
