package com.linkedin.drelephant.tuning.engine;

import com.linkedin.drelephant.tuning.AutoTuningOptimizeManager;
import com.linkedin.drelephant.tuning.OptimizationAlgoFactory;
import com.linkedin.drelephant.tuning.foundation.ExecutionEngine;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningParameter;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;


public class MRExecutionEngine implements ExecutionEngine {
  private final Logger logger = Logger.getLogger(getClass());
  private String executionEngineName = "MR";

  @Override
  public String getExecutionEngineName() {
    return this.executionEngineName;
  }

  @Override
  public void computeValuesOfDerivedConfigurationParameters(List<TuningParameter> derivedParameterList,
      List<JobSuggestedParamValue> jobSuggestedParamValues) {
    Map<String, Double> jobSuggestedParamValueMap = new HashMap<String, Double>();
    for (JobSuggestedParamValue jobSuggestedParamValue : jobSuggestedParamValues) {
      jobSuggestedParamValueMap.put(jobSuggestedParamValue.tuningParameter.paramName,
          jobSuggestedParamValue.paramValue);
    }

    for (TuningParameter derivedParameter : derivedParameterList) {
      logger.info("Computing value of derived param: " + derivedParameter.paramName);
      Double paramValue = null;
      if (derivedParameter.paramName.equals("mapreduce.reduce.java.opts")) {
        String parentParamName = "mapreduce.reduce.memory.mb";
        if (jobSuggestedParamValueMap.containsKey(parentParamName)) {
          paramValue = 0.75 * jobSuggestedParamValueMap.get(parentParamName);
        }
      } else if (derivedParameter.paramName.equals("mapreduce.map.java.opts")) {
        String parentParamName = "mapreduce.map.memory.mb";
        if (jobSuggestedParamValueMap.containsKey(parentParamName)) {
          paramValue = 0.75 * jobSuggestedParamValueMap.get(parentParamName);
        }
      } else if (derivedParameter.paramName.equals("mapreduce.input.fileinputformat.split.maxsize")) {
        String parentParamName = "pig.maxCombinedSplitSize";
        if (jobSuggestedParamValueMap.containsKey(parentParamName)) {
          paramValue = jobSuggestedParamValueMap.get(parentParamName);
        }
      }

      if (paramValue != null) {
        JobSuggestedParamValue jobSuggestedParamValue = new JobSuggestedParamValue();
        jobSuggestedParamValue.paramValue = paramValue;
        jobSuggestedParamValue.tuningParameter = derivedParameter;
        jobSuggestedParamValues.add(jobSuggestedParamValue);
      }
    }
  }

  /**
   * Check if the parameters violated constraints
   * Constraint 1: sort.mb > 60% of map.memory: To avoid heap memory failure
   * Constraint 2: map.memory - sort.mb < 768: To avoid heap memory failure
   * Constraint 3: pig.maxCombinedSplitSize > 1.8*mapreduce.map.memory.mb
   * @param jobSuggestedParamValueList List of suggested param values
   * @param jobType Job type
   * @return true if the constraint is violated, false otherwise
   */

  @Override
  public Boolean isParamConstraintViolated(List<JobSuggestedParamValue> jobSuggestedParamValueList,
      TuningAlgorithm tuningAlgorithm) {
    logger.info("Checking whether parameter values are within constraints");
    Integer violations = 0;

    if (tuningAlgorithm.jobType.equals(TuningAlgorithm.JobType.PIG)) {
      AutoTuningOptimizeManager optimizeManager = OptimizationAlgoFactory.getOptimizationAlogrithm(tuningAlgorithm);
      if (optimizeManager != null) {
        violations = optimizeManager.numberOfConstraintsViolated(jobSuggestedParamValueList);
      } else {
        Double mrSortMemory = null;
        Double mrMapMemory = null;
        Double pigMaxCombinedSplitSize = null;

        for (JobSuggestedParamValue jobSuggestedParamValue : jobSuggestedParamValueList) {
          if (jobSuggestedParamValue.tuningParameter.paramName.equals("mapreduce.task.io.sort.mb")) {
            mrSortMemory = jobSuggestedParamValue.paramValue;
          } else if (jobSuggestedParamValue.tuningParameter.paramName.equals("mapreduce.map.memory.mb")) {
            mrMapMemory = jobSuggestedParamValue.paramValue;
          } else if (jobSuggestedParamValue.tuningParameter.paramName.equals("pig.maxCombinedSplitSize")) {
            pigMaxCombinedSplitSize = jobSuggestedParamValue.paramValue / FileUtils.ONE_MB;
          }
        }
        if (mrSortMemory != null && mrMapMemory != null) {
          if (mrSortMemory > 0.6 * mrMapMemory) {
            logger.info("Constraint violated: Sort memory > 60% of map memory");
            violations++;
          }
          if (mrMapMemory - mrSortMemory < 768) {
            logger.info("Constraint violated: Map memory - sort memory < 768 mb");
            violations++;
          }
        }

        if (pigMaxCombinedSplitSize != null && mrMapMemory != null && (pigMaxCombinedSplitSize > 1.8 * mrMapMemory)) {
          logger.info("Constraint violated: Pig max combined split size > 1.8 * map memory");
          violations++;
        }
      }
    }
    if (violations == 0) {
      return false;
    } else {
      logger.info("Number of constraint(s) violated: " + violations);
      return true;
    }
  }
}
