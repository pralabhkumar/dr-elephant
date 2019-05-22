package com.linkedin.drelephant.tuning.hbt;

final class MRConstant {
  static final String MAPPER_TIME_HEURISTIC = "Mapper Time";
  static final String MAPPER_SPEED_HEURISTIC = "Mapper Speed";
  static final String MAPPER_MEMORY_HEURISTIC = "Mapper Memory";
  static final String MAPPER_SPILL_HEURISTIC = "Mapper Spill";
  static final String REDUCER_TIME_HEURISTIC = "Reducer Time";
  static final String REDUCER_MEMORY_HEURISTIC = "Reducer Memory";

  static final String MAPPER_MEMORY = "mapreduce.map.memory.mb";
  static final String MAPPER_HEAP = "mapreduce.map.java.opts";
  static final String REDUCER_MEMORY = "mapreduce.reduce.memory.mb";
  static final String REDUCER_HEAP = "mapreduce.reduce.java.opts";
  static final String MAP_SPLIT_SIZE = "mapreduce.input.fileinputformat.split.maxsize";
  static final String PIG_SPLIT_SIZE = "pig.maxCombinedSplitSize";
  static final String NUMBER_OF_REDUCER = "mapreduce.job.reduces";
  static final String BUFFER_SIZE = "mapreduce.task.io.sort.mb";
  static final String SPILL_PERCENTAGE = "mapreduce.map.sort.spill.percent";

  static final String MAX_VIRTUAL_MEMORY = "Max Virtual Memory (MB)";
  static final String MAX_PHYSICAL_MEMORY = "Max Physical Memory (MB)";
  static final String MAX_TOTAL_COMMITTED_HEAP_USAGE = "Max Total Committed Heap Usage Memory (MB)";
  static final String AVERAGE_TASK_INPUT_SIZE = "Average task input size";
  static final String AVERAGE_TASK_RUNTIME = "Average task runtime";
  static final String NUMBER_OF_TASK = "Number of tasks";
  static final String RATIO_OF_SPILLED_RECORDS_TO_OUTPUT_RECORDS = "Ratio of spilled records to output records";
  static final String SORT_BUFFER = "Sort Buffer";
  static final String SORT_SPILL = "Sort Spill";

  static final double VIRTUALMEMORY_TO_PHYSICALMEMORY_RATIO = 2.1;
  static final double HEAPSIZE_TO_MAPPERMEMORY_SAFE_RATIO = 0.75;
  static final double AVG_TASK_TIME_LOW_THRESHOLDS_FIRST = 1.0;
  static final double AVG_TASK_TIME_LOW_THRESHOLDS_SECOND = 2.0;
  static final double AVG_TASK_TIME_HIGH_THRESHOLDS_FIRST = 120;
  static final double AVG_TASK_TIME_HIGH_THRESHOLDS_SECOND = 60;
  static final int SPLIT_SIZE_INCREASE_FIRST = 2;
  static final double SPLIT_SIZE_INCREASE_SECOND = 1.2;
  static final double SPLIT_SIZE_DECREASE = 0.8;

  static final double RATIO_OF_DISK_SPILL_TO_OUTPUT_RECORDS_THRESHOLD_FIRST = 3.0;
  static final double RATIO_OF_DISK_SPILL_TO_OUTPUT_RECORDS_THRESHOLD_SECOND= 2.5;
  static final double SORT_SPILL_THRESHOLD_FIRST = 0.85;
  static final float SORT_SPILL_INCREASE=0.05f;
  static final double BUFFER_SIZE_INCREASE = 1.1;
  static final double BUFFER_SIZE_INCREASE_FIRST = 1.2;
  static final double BUFFER_SIZE_INCREASE_SECOND = 1.3;
  static final double SORT_BUFFER_CUSHION = 769;
  static final double MINIMUM_MEMORY_SORT_BUFFER_RATIO = (10 / 6);
  static final double HEAP_TO_MEMORY_SIZE_RATION = 0.75;


  enum Function_Name {Mapper, Reducer}
  enum TimeUnit{hr,min,sec}
}
