package com.linkedin.drelephant.tuning.hbt;

import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.analysis.ApplicationType;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.mapreduce.heuristics.MapperMemoryHeuristic;
import com.linkedin.drelephant.mapreduce.heuristics.MapperSpeedHeuristic;
import com.linkedin.drelephant.mapreduce.heuristics.MapperSpillHeuristic;
import com.linkedin.drelephant.mapreduce.heuristics.MapperTimeHeuristic;
import com.linkedin.drelephant.mapreduce.heuristics.ReducerMemoryHeuristic;
import com.linkedin.drelephant.mapreduce.heuristics.ReducerTimeHeuristic;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;


/**
 * This class contains constants require for Map reduce HBT .It also
 * contains configuration builder class, which is used to build the congfiguration
 */
final class MRConstant {
  private static final Logger logger = Logger.getLogger(MRConstant.class);
  private static boolean debugEnabled = logger.isDebugEnabled();
  static String MAPPER_TIME_HEURISTIC = null;
  static String MAPPER_SPEED_HEURISTIC = null;
  static String MAPPER_MEMORY_HEURISTIC = null;
  static String MAPPER_SPILL_HEURISTIC = null;
  static String REDUCER_TIME_HEURISTIC = null;
  static String REDUCER_MEMORY_HEURISTIC = null;
  private final static String APPLICATION_TYPE = "mapreduce";

  /**
   * Since Dr elephant runs on Java 1.5 , switch case is not used.
   */
  static {
    List<Heuristic> heuristics =
        ElephantContext.instance().getHeuristicsForApplicationType(new ApplicationType(APPLICATION_TYPE));
    for (Heuristic heuristic : heuristics) {
      if (heuristic.getHeuristicConfData().getClassName().equals(MapperTimeHeuristic.class.getCanonicalName())) {
        MAPPER_TIME_HEURISTIC = heuristic.getHeuristicConfData().getHeuristicName();
      } else if (heuristic.getHeuristicConfData()
          .getClassName()
          .equals(MapperSpeedHeuristic.class.getCanonicalName())) {
        MAPPER_SPEED_HEURISTIC = heuristic.getHeuristicConfData().getHeuristicName();
      } else if (heuristic.getHeuristicConfData()
          .getClassName()
          .equals(MapperMemoryHeuristic.class.getCanonicalName())) {
        MAPPER_MEMORY_HEURISTIC = heuristic.getHeuristicConfData().getHeuristicName();
      } else if (heuristic.getHeuristicConfData()
          .getClassName()
          .equals(MapperSpillHeuristic.class.getCanonicalName())) {
        MAPPER_SPILL_HEURISTIC = heuristic.getHeuristicConfData().getHeuristicName();
      } else if (heuristic.getHeuristicConfData()
          .getClassName()
          .equals(ReducerTimeHeuristic.class.getCanonicalName())) {
        REDUCER_TIME_HEURISTIC = heuristic.getHeuristicConfData().getHeuristicName();
      } else if (heuristic.getHeuristicConfData()
          .getClassName()
          .equals(ReducerMemoryHeuristic.class.getCanonicalName())) {
        REDUCER_MEMORY_HEURISTIC = heuristic.getHeuristicConfData().getHeuristicName();
      }
    }
  }

  enum Function_Name {Mapper, Reducer}

  enum TimeUnit {hr, min, sec}

  /**
   * Configuration properties for MR HBT . This property can set , if wants to
   * override default values , in AutoTuningConf.xml
   * Note : Generally default values are good (and based on the testing and experimentation)
   */

  private static final String VIRTUALMEMORY_TO_PHYSICALMEMORY_RATIO_NAME =
      "hbt.mr.vitualmemory.to.physicalmemory.ratio";
  private static final String HEAPSIZE_TO_MAPPERMEMORY_SAFE_RATIO_NAME = "hbt.mr.heapsize.mappermemory.safe.ratio";
  private static final String AVG_TASK_TIME_LOW_THRESHOLDS_FIRST_NAME = "hbt.mr.avg.tasktime.low.threshold.first";
  private static final String AVG_TASK_TIME_LOW_THRESHOLDS_SECOND_NAME = "hbt.mr.avg.tasktime.low.threshold.second";
  private static final String AVG_TASK_TIME_HIGH_THRESHOLDS_FIRST_NAME = "hbt.mr.avg.tasktime.high.threshold.first";
  private static final String AVG_TASK_TIME_HIGH_THRESHOLDS_SECOND_NAME = "hbt.mr.avg.tasktime.high.threshold.second";
  private static final String SPLIT_SIZE_INCREASE_FIRST_NAME = "hbt.mr.splitsize.increase.first";
  private static final String SPLIT_SIZE_INCREASE_SECOND_NAME = "hbt.mr.splitsize.increase.second";
  private static final String SPLIT_SIZE_DECREASE_NAME = "hbt.mr.splitsize.decrease";
  private static final String RATIO_OF_DISK_SPILL_TO_OUTPUT_RECORDS_THRESHOLD_FIRST_NAME =
      "hbt.mr.diskspill.outputrecord.ratio.first";
  private static final String RATIO_OF_DISK_SPILL_TO_OUTPUT_RECORDS_THRESHOLD_SECOND_NAME =
      "hbt.mr.diskspill.outputrecord.ratio.second";
  private static final String SORT_SPILL_THRESHOLD_FIRST_NAME = "hbt.mr.sortspill.threshold.first";
  private static final String SORT_SPILL_INCREASE_NAME = "hbt.mr.sortspill.increase";
  private static final String BUFFER_SIZE_INCREASE_NAME = "hbt.mr.buffersize.increase";
  private static final String BUFFER_SIZE_INCREASE_FIRST_NAME = "hbt.mr.buffersize.increase.first";
  private static final String BUFFER_SIZE_INCREASE_SECOND_NAME = "hbt.mr.buffersize.increase.second";
  private static final String SORT_BUFFER_CUSHION_NAME = "hbt.mr.sortbuffer.cushion";
  private static final String MINIMUM_MEMORY_SORT_BUFFER_RATIO_NAME = "hbt.mr.minimum.memory.sortbuffer.ration";

  /**
   * This class used to create configuration required for HBT MR.
   */
  static class MRConfigurationBuilder {
    static HBTConfiguration<Double> VIRTUALMEMORY_TO_PHYSICALMEMORY_RATIO = null;
    static HBTConfiguration<Double> HEAPSIZE_TO_MAPPERMEMORY_SAFE_RATIO = null;
    static HBTConfiguration<Double> AVG_TASK_TIME_LOW_THRESHOLDS_FIRST = null;
    static HBTConfiguration<Double> AVG_TASK_TIME_LOW_THRESHOLDS_SECOND = null;
    static HBTConfiguration<Double> AVG_TASK_TIME_HIGH_THRESHOLDS_FIRST = null;
    static HBTConfiguration<Double> AVG_TASK_TIME_HIGH_THRESHOLDS_SECOND = null;
    static HBTConfiguration<Integer> SPLIT_SIZE_INCREASE_FIRST = null;
    static HBTConfiguration<Double> SPLIT_SIZE_INCREASE_SECOND = null;
    static HBTConfiguration<Double> SPLIT_SIZE_DECREASE = null;
    static HBTConfiguration<Double> RATIO_OF_DISK_SPILL_TO_OUTPUT_RECORDS_THRESHOLD_FIRST = null;
    static HBTConfiguration<Double> RATIO_OF_DISK_SPILL_TO_OUTPUT_RECORDS_THRESHOLD_SECOND = null;
    static HBTConfiguration<Double> SORT_SPILL_THRESHOLD_FIRST = null;
    static HBTConfiguration<Float> SORT_SPILL_INCREASE = null;
    static HBTConfiguration<Double> BUFFER_SIZE_INCREASE = null;
    static HBTConfiguration<Double> BUFFER_SIZE_INCREASE_FIRST = null;
    static HBTConfiguration<Double> BUFFER_SIZE_INCREASE_SECOND = null;
    static HBTConfiguration<Double> SORT_BUFFER_CUSHION = null;
    static HBTConfiguration<Double> MINIMUM_MEMORY_SORT_BUFFER_RATIO = null;

    static void buildConfigurations(Configuration configuration) {
      VIRTUALMEMORY_TO_PHYSICALMEMORY_RATIO =
          new HBTConfiguration<Double>().setConfigurationName(VIRTUALMEMORY_TO_PHYSICALMEMORY_RATIO_NAME)
              .setValue(configuration.getDouble(VIRTUALMEMORY_TO_PHYSICALMEMORY_RATIO_NAME, 2.1))
              .setDoc(
                  "This is virtual memory to physical memory ration , set up at yarn level <yarn.nodemanager.vmem-pmem-ratio>");
      HEAPSIZE_TO_MAPPERMEMORY_SAFE_RATIO =
          new HBTConfiguration<Double>().setConfigurationName(HEAPSIZE_TO_MAPPERMEMORY_SAFE_RATIO_NAME)
              .setValue(configuration.getDouble(HEAPSIZE_TO_MAPPERMEMORY_SAFE_RATIO_NAME, 0.75))
              .setDoc("This is safe value for heap to mapper memory .This is based on guidlines and experimentation ");
      AVG_TASK_TIME_LOW_THRESHOLDS_FIRST =
          new HBTConfiguration<Double>().setConfigurationName(AVG_TASK_TIME_LOW_THRESHOLDS_FIRST_NAME)
              .setValue(configuration.getDouble(AVG_TASK_TIME_LOW_THRESHOLDS_FIRST_NAME, 1.0))
              .setDoc(
                  "If the task time is less then this in minutes , then its critical and task split size should be increased");
      AVG_TASK_TIME_LOW_THRESHOLDS_SECOND =
          new HBTConfiguration<Double>().setConfigurationName(AVG_TASK_TIME_LOW_THRESHOLDS_SECOND_NAME)
              .setValue(configuration.getDouble(AVG_TASK_TIME_LOW_THRESHOLDS_SECOND_NAME, 2.0))
              .setDoc(
                  "If the task time is less then this in minutes , then its severe and task split size should be increased ");
      AVG_TASK_TIME_HIGH_THRESHOLDS_FIRST =
          new HBTConfiguration<Double>().setConfigurationName(AVG_TASK_TIME_HIGH_THRESHOLDS_FIRST_NAME)
              .setValue(configuration.getDouble(AVG_TASK_TIME_HIGH_THRESHOLDS_FIRST_NAME, 120))
              .setDoc("If the task time is greater then this time then its critical and split size should be decrease");
      AVG_TASK_TIME_HIGH_THRESHOLDS_SECOND =
          new HBTConfiguration<Double>().setConfigurationName(AVG_TASK_TIME_HIGH_THRESHOLDS_SECOND_NAME)
              .setValue(configuration.getDouble(AVG_TASK_TIME_HIGH_THRESHOLDS_SECOND_NAME, 60))
              .setDoc("If the task time is greater then this time then its severe and split size should be decrease");

      SPLIT_SIZE_INCREASE_FIRST = new HBTConfiguration<Integer>().setConfigurationName(SPLIT_SIZE_INCREASE_FIRST_NAME)
          .setValue(configuration.getInt(SPLIT_SIZE_INCREASE_FIRST_NAME, 2))
          .setDoc(
              "If the task time is in critical range , then increase the split by this factor or decrease the split by the same factor");

      SPLIT_SIZE_INCREASE_SECOND = new HBTConfiguration<Double>().setConfigurationName(SPLIT_SIZE_INCREASE_SECOND_NAME)
          .setValue(configuration.getDouble(SPLIT_SIZE_INCREASE_SECOND_NAME, 1.2))
          .setDoc("If the task time is in severe range , then increase the split by this factor");

      SPLIT_SIZE_DECREASE = new HBTConfiguration<Double>().setConfigurationName(SPLIT_SIZE_DECREASE_NAME)
          .setValue(configuration.getDouble(SPLIT_SIZE_DECREASE_NAME, 0.8))
          .setDoc("If the task time is in severe range on higher side , then decrease the split by this factor");

      RATIO_OF_DISK_SPILL_TO_OUTPUT_RECORDS_THRESHOLD_FIRST = new HBTConfiguration<Double>().setConfigurationName(
          RATIO_OF_DISK_SPILL_TO_OUTPUT_RECORDS_THRESHOLD_FIRST_NAME)
          .setValue(configuration.getDouble(RATIO_OF_DISK_SPILL_TO_OUTPUT_RECORDS_THRESHOLD_FIRST_NAME, 3.0))
          .setDoc(
              "If the disk spill to output records is greater then this , then lot of spill are happening and its critical");

      RATIO_OF_DISK_SPILL_TO_OUTPUT_RECORDS_THRESHOLD_SECOND = new HBTConfiguration<Double>().setConfigurationName(
          RATIO_OF_DISK_SPILL_TO_OUTPUT_RECORDS_THRESHOLD_SECOND_NAME)
          .setValue(configuration.getDouble(RATIO_OF_DISK_SPILL_TO_OUTPUT_RECORDS_THRESHOLD_SECOND_NAME, 2.5))
          .setDoc(
              "If the disk spill to output records is greater then this , then spills are happening and its severe");

      SORT_SPILL_THRESHOLD_FIRST = new HBTConfiguration<Double>().setConfigurationName(SORT_SPILL_THRESHOLD_FIRST_NAME)
          .setValue(configuration.getDouble(SORT_SPILL_THRESHOLD_FIRST_NAME, 0.85))
          .setDoc(
              "If the buffer spill percentage is less then this value , then we can increase the spill percentage to reduce ration of disk spill to output records ");

      SORT_SPILL_INCREASE = new HBTConfiguration<Float>().setConfigurationName(SORT_SPILL_INCREASE_NAME)
          .setValue(configuration.getFloat(SORT_SPILL_INCREASE_NAME, 0.05f))
          .setDoc(
              "If the buffer spill percentage is less then SORT_SPILL_THRESHOLD_FIRST , then increase the spill percentage by this factor  ");

      BUFFER_SIZE_INCREASE = new HBTConfiguration<Double>().setConfigurationName(BUFFER_SIZE_INCREASE_NAME)
          .setValue(configuration.getDouble(BUFFER_SIZE_INCREASE_NAME, 1.1))
          .setDoc("Increase the buffer size by this factor to reduce spill to output records");

      BUFFER_SIZE_INCREASE_FIRST = new HBTConfiguration<Double>().setConfigurationName(BUFFER_SIZE_INCREASE_FIRST_NAME)
          .setValue(configuration.getDouble(BUFFER_SIZE_INCREASE_FIRST_NAME, 1.2))
          .setDoc("Increase the buffer size by this factor to reduce spill to output records , if its sever issue ");

      BUFFER_SIZE_INCREASE_SECOND =
          new HBTConfiguration<Double>().setConfigurationName(BUFFER_SIZE_INCREASE_SECOND_NAME)
              .setValue(configuration.getDouble(BUFFER_SIZE_INCREASE_SECOND_NAME, 1.3))
              .setDoc(
                  "Increase the buffer size by this factor to reduce spill to output records , if buffer spill percentage cannot be increased");

      SORT_BUFFER_CUSHION = new HBTConfiguration<Double>().setConfigurationName(SORT_BUFFER_CUSHION_NAME)
          .setValue(configuration.getDouble(SORT_BUFFER_CUSHION_NAME, 769))
          .setDoc("Safe/Minimum difference between Mapper Memory and buffer");

      MINIMUM_MEMORY_SORT_BUFFER_RATIO =
          new HBTConfiguration<Double>().setConfigurationName(MINIMUM_MEMORY_SORT_BUFFER_RATIO_NAME)
              .setValue(configuration.getDouble(MINIMUM_MEMORY_SORT_BUFFER_RATIO_NAME, (10 / 6)))
              .setDoc("Safe ratio between Mapper Memory and buffer size");

      if (debugEnabled) {
        logger.debug(" HBT MR configurations ");
        logger.debug(VIRTUALMEMORY_TO_PHYSICALMEMORY_RATIO);
        logger.debug(HEAPSIZE_TO_MAPPERMEMORY_SAFE_RATIO);
        logger.debug(AVG_TASK_TIME_LOW_THRESHOLDS_FIRST);
        logger.debug(AVG_TASK_TIME_LOW_THRESHOLDS_SECOND);
        logger.debug(AVG_TASK_TIME_HIGH_THRESHOLDS_FIRST);
        logger.debug(AVG_TASK_TIME_HIGH_THRESHOLDS_SECOND);
        logger.debug(SPLIT_SIZE_INCREASE_FIRST);
        logger.debug(SPLIT_SIZE_INCREASE_SECOND);
        logger.debug(SPLIT_SIZE_DECREASE);
        logger.debug(RATIO_OF_DISK_SPILL_TO_OUTPUT_RECORDS_THRESHOLD_FIRST);
        logger.debug(RATIO_OF_DISK_SPILL_TO_OUTPUT_RECORDS_THRESHOLD_SECOND);
        logger.debug(SORT_SPILL_INCREASE);
        logger.debug(BUFFER_SIZE_INCREASE);
        logger.debug(BUFFER_SIZE_INCREASE_FIRST);
        logger.debug(BUFFER_SIZE_INCREASE_SECOND);
        logger.debug(SORT_BUFFER_CUSHION);
        logger.debug(MINIMUM_MEMORY_SORT_BUFFER_RATIO);
      }
    }
  }
}
