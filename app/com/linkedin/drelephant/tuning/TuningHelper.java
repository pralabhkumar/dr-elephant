package com.linkedin.drelephant.tuning;

import java.util.ArrayList;
import java.util.List;
import models.JobDefinition;
import models.JobSuggestedParamSet;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import models.TuningJobExecutionParamSet;
import models.TuningParameter;
import org.apache.log4j.Logger;


public class TuningHelper {
  private static final Logger logger = Logger.getLogger(TuningHelper.class);
  private static boolean debugEnabled = logger.isDebugEnabled();
  public static List<TuningParameter> getTuningParameterList(TuningJobDefinition tuningJobDefinition) {
    List<TuningParameter> tuningParameterList = TuningParameter.find.where()
        .eq(TuningParameter.TABLE.tuningAlgorithm + "." + TuningAlgorithm.TABLE.id,
            tuningJobDefinition.tuningAlgorithm.id)
        .eq(TuningParameter.TABLE.isDerived, 0)
        .findList();
    return tuningParameterList;
  }

  public static JobSuggestedParamSet getDefaultParameterValuesforJob(TuningJobDefinition tuningJobDefinition) {
    JobSuggestedParamSet defaultJobParamSet = JobSuggestedParamSet.find.where()
        .eq(JobSuggestedParamSet.TABLE.jobDefinition + "." + JobDefinition.TABLE.id, tuningJobDefinition.job.id)
        .eq(JobSuggestedParamSet.TABLE.isParamSetDefault, 1)
        .order()
        .desc(JobSuggestedParamSet.TABLE.id)
        .setMaxRows(1)
        .findUnique();
    return defaultJobParamSet;
  }

  public static TuningJobDefinition getTuningJobDefinition(JobDefinition job) {
    TuningJobDefinition tuningJobDefinition = TuningJobDefinition.find.select("*")
        .fetch(TuningJobDefinition.TABLE.job, "*")
        .where()
        .eq(TuningJobDefinition.TABLE.job + "." + JobDefinition.TABLE.id, job.id)
        .eq(TuningJobDefinition.TABLE.tuningEnabled, 1)
        .findUnique();
    return tuningJobDefinition;
  }

  public static List<TuningParameter> getDerivedParameterList(TuningJobDefinition tuningJobDefinition) {
    List<TuningParameter> derivedParameterList = new ArrayList<TuningParameter>();
    derivedParameterList = TuningParameter.find.where()
        .eq(TuningParameter.TABLE.tuningAlgorithm + "." + TuningAlgorithm.TABLE.id,
            tuningJobDefinition.tuningAlgorithm.id)
        .eq(TuningParameter.TABLE.isDerived, 1)
        .findList();
    return derivedParameterList;
  }

  public static List<TuningJobExecutionParamSet> getTuningJobExecutionFromDefinition(JobDefinition jobDefinition) {
    List<TuningJobExecutionParamSet> tuningJobExecutionParamSets =
        TuningJobExecutionParamSet.find.fetch(TuningJobExecutionParamSet.TABLE.jobSuggestedParamSet, "*")
            .fetch(TuningJobExecutionParamSet.TABLE.jobExecution, "*")
            .where()
            .eq(TuningJobExecutionParamSet.TABLE.jobSuggestedParamSet + '.' + JobSuggestedParamSet.TABLE.jobDefinition
                + '.' + JobDefinition.TABLE.id, jobDefinition.id)
            .order()
            .desc("job_execution_id")
            .findList();
    return tuningJobExecutionParamSets;
  }

  public static int getNumberOfValidSuggestedParamExecution(List<TuningJobExecutionParamSet> tuningJobExecutionParamSets) {
    int autoAppliedExecution = 0;
    for (TuningJobExecutionParamSet tuningJobExecutionParam : tuningJobExecutionParamSets) {
      if (tuningJobExecutionParam.jobSuggestedParamSet.isParamSetSuggested
          && !tuningJobExecutionParam.jobSuggestedParamSet.paramSetState.equals(
          JobSuggestedParamSet.ParamSetStatus.DISCARDED)) {
        autoAppliedExecution++;
      }
    }
    if (debugEnabled) {
      logger.debug(" Total number of executions after auto applied enabled " + autoAppliedExecution);
    }
    return autoAppliedExecution;
  }
}
