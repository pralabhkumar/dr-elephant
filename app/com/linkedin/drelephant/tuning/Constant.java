package com.linkedin.drelephant.tuning;

import java.util.HashMap;
import java.util.Map;


/**
 * Constants describe different TuningType , Algorithm Type
 * Execution Engine and managers .
 * These are  used in AutoTuningFlow .
 */
public class Constant {
  public enum TuningType {HBT,OBT}
  public enum AlgorithmType {PSO,PSO_IPSO,HBT}
  public enum ExecutionEngineTypes{MR,SPARK}
  public enum TypeofManagers{AbstractBaselineManager,AbstractFitnessManager,AbstractJobStatusManager,AbstractParameterGenerateManager}


  public static final String JVM_MAX_HEAP_MEMORY_REGEX = ".*-Xmx([\\d]+)([mMgG]).*";
  public static final double YARN_VMEM_TO_PMEM_RATIO = 2.1;
  public static final int MB_IN_ONE_GB = 1024;
  public static final int SORT_BUFFER_CUSHION = 769;
  public static final int DEFAULT_CONTAINER_HEAP_MEMORY = 1536;
  public static final int OPTIMAL_MAPPER_SPEED_BYTES_PER_SECOND = 10;
  public static final double HEAP_MEMORY_TO_CONTAINER_MEMORY_RATIO = 0.75D;
  public static final double SORT_BUFFER_THRESHOLD = 0.85D;
  public static final double MAPPER_MEMORY_SPILL_THRESHOLD_1 = 2.7;
  public static final double MAPPER_MEMORY_SPILL_THRESHOLD_2 = 2.2;
  public static final double MEMORY_TO_SORT_BUFFER_RATIO = 1.6;
  public static final double SPLIT_SIZE_TO_MEMORY_RATIO = 0.80;
  public static final double SPILL_PERCENTAGE_STEP_SIZE = 0.05;

}
