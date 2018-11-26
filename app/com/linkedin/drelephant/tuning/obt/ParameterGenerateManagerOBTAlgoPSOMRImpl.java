package com.linkedin.drelephant.tuning.obt;

import com.linkedin.drelephant.tuning.ExecutionEngine;
import com.linkedin.drelephant.tuning.engine.MRExecutionEngine;
import java.util.List;
import models.JobSuggestedParamSet;
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import models.TuningParameter;
import org.apache.log4j.Logger;
import play.libs.Json;
import org.apache.hadoop.conf.Configuration;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.FileUtils;

public class ParameterGenerateManagerOBTAlgoPSOMRImpl<T extends MRExecutionEngine> extends ParameterGenerateManagerOBTAlgoPSOImpl<T>{
  T mrExecutionEngine;
  private final Logger logger = Logger.getLogger(getClass());
  public ParameterGenerateManagerOBTAlgoPSOMRImpl(T mrExecutionEngine) {
    this.mrExecutionEngine = mrExecutionEngine;
  }



  @Override
  protected List<JobSuggestedParamSet> getPendingParamSets() {
    List<JobSuggestedParamSet> pendingParamSetList = mrExecutionEngine.getPendingJobs()
        .eq(JobSuggestedParamSet.TABLE.tuningAlgorithm
            + "." + TuningAlgorithm.TABLE.optimizationAlgo, TuningAlgorithm.OptimizationAlgo.PSO.name())
        .eq(JobSuggestedParamSet.TABLE.isParamSetDefault, 0)
        .findList();
    return pendingParamSetList;
  }

  @Override
  protected List<TuningJobDefinition> getTuningJobDefinitions() {
    return mrExecutionEngine.getTuningJobDefinitionsForParameterSuggestion()
        .eq(TuningJobDefinition.TABLE.tuningAlgorithm
            + "." + TuningAlgorithm.TABLE.optimizationAlgo, TuningAlgorithm.OptimizationAlgo.PSO.name())
        .findList();
  }


  /**
   * Check if the parameters violated constraints
   * Constraint 1: sort.mb > 60% of map.memory: To avoid heap memory failure
   * Constraint 2: map.memory - sort.mb < 768: To avoid heap memory failure
   * Constraint 3: pig.maxCombinedSplitSize > 1.8*mapreduce.map.memory.mb
   * @param jobSuggestedParamValueList List of suggested param values
   * @return true if the constraint is violated, false otherwise
   */


@Override
  public boolean isParamConstraintViolated(List<JobSuggestedParamValue> jobSuggestedParamValueList) {
    Double mrSortMemory = null;
    Double mrMapMemory = null;
    Double pigMaxCombinedSplitSize = null;
    Integer violations = 0;
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
    if (violations == 0) {
      return false;
    } else {
      logger.info("Number of constraint(s) violated: " + violations);
      return true;
    }
  }

  @Override
  public void computeValuesOfDerivedConfigurationParameters(List<TuningParameter> derivedParameterList,
      List<JobSuggestedParamValue> jobSuggestedParamValueList) {
    mrExecutionEngine.computeValuesOfDerivedConfigurationParameters(derivedParameterList,jobSuggestedParamValueList);
  }
}
