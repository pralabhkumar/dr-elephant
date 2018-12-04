package com.linkedin.drelephant.tuning;

import com.linkedin.drelephant.tuning.engine.MRExecutionEngine;
import com.linkedin.drelephant.tuning.hbt.FitnessManagerHBT;
import com.linkedin.drelephant.tuning.hbt.MRApplicationData;
import com.linkedin.drelephant.tuning.hbt.MRJob;
import com.linkedin.drelephant.tuning.hbt.ParameterGenerateManagerHBT;
import com.linkedin.drelephant.tuning.hbt.ParameterGenerateManagerHBTMR;
import java.util.List;
import java.util.Map;
import models.AppHeuristicResult;
import models.AppHeuristicResultDetails;
import models.AppResult;
import models.JobSuggestedParamSet;

import static org.junit.Assert.*;
import static play.test.Helpers.*;
import static common.DBTestUtil.*;
import static common.DBTestUtil.*;


public class ParameterGenerateManagerTestRunner implements Runnable {

  private void populateTestData() {
    try {
      initMRHBT();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void run() {
    populateTestData();
    testParamGeneraterHBTMR();
  }

  public void testParamGeneraterHBTMR() {
    MRExecutionEngine mrExecutionEngine = new MRExecutionEngine();
    List<AppResult> results = AppResult.find.select("*")
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, "*")
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS + "." + AppHeuristicResult.TABLE.APP_HEURISTIC_RESULT_DETAILS, "*")
        .where()
        .eq(AppResult.TABLE.FLOW_EXEC_ID, "https://ltx1-holdemaz01.grid.linkedin.com:8443/executor?execid=5416293")
        .eq(AppResult.TABLE.JOB_EXEC_ID,
            "https://ltx1-holdemaz01.grid.linkedin.com:8443/executor?execid=5416293&job=countByCountryFlow_countByCountry&attempt=0")
        .findList();
    assertTrue(" Number of MapReduce Application ", results.size() == 2);

    MRJob mrJob = new MRJob(results, mrExecutionEngine);
    Map<String, String> appliedParameter = mrJob.getAppliedParameter();

    assertTrue(" Mapper Memory " + appliedParameter.size(), appliedParameter.get("Mapper Memory").equals("2048"));
    assertTrue(" Reducer Memory ", appliedParameter.get("Reducer Memory").equals("2048"));
    assertTrue(" Sort Buffer ", appliedParameter.get("Sort Buffer").equals("100"));
    assertTrue(" Sort Spill ", appliedParameter.get("Sort Spill").equals("0.80"));
    assertTrue(" Split Size ", appliedParameter.get("Split Size").equals("9223372036854775807"));
    mrJob.analyzeAllApplications();
    List<MRApplicationData> mrApplicationDatas = mrJob.getApplicationAnalyzedData();
    for (MRApplicationData mrApplicationData : mrApplicationDatas) {
      assertTrue(" Application IDs ", mrApplicationData.getApplicationID().equals("application_1458194917883_1453361")
          || mrApplicationData.getApplicationID().equals("application_1458194917883_1453362"));
      Map<String, Double> suggestedParameter = mrApplicationData.getSuggestedParameter();
      Map<String, Double> usedParameter = mrApplicationData.getCounterValues();
      if (mrApplicationData.getApplicationID().equals("application_1458194917883_1453361")) {
        assertTrue("Mapper Max Virtual Memory (MB) ", usedParameter.get("Mapper Max Virtual Memory (MB)") == 1426.0);
        assertTrue("Mapper Max Physical Memory (MB)", usedParameter.get("Mapper Max Physical Memory (MB)") == 595.0);
        assertTrue("Mapper Max Total Committed Heap Usage Memory (MB)",
            usedParameter.get("Mapper Max Total Committed Heap Usage Memory (MB)") == 427.0);
        assertTrue(" Mapper Memory Suggested ", suggestedParameter.get("mapreduce.map.memory.mb") == 1024.0);
        assertTrue(" Mapper Memory Heap Suggested ", suggestedParameter.get("mapreduce.map.java.opts") == 600.0);

        assertTrue("Reducer Max Virtual Memory (MB)", usedParameter.get("Reducer Max Virtual Memory (MB)") == 1350.0);
        assertTrue("Reducer Max Physical Memory (MB)", usedParameter.get("Reducer Max Physical Memory (MB)") == 497.0);
        assertTrue("Reducer Max Total Committed Heap Usage Memory (MB)",
            usedParameter.get("Reducer Max Total Committed Heap Usage Memory (MB)") == 300.0);
        assertTrue(" Reducer Memory Suggested " + suggestedParameter.get("mapreduce.reduce.memory.mb"),
            suggestedParameter.get("mapreduce.reduce.memory.mb") == 1024.0);
        assertTrue("Reducer Memory Heap Suggested ", suggestedParameter.get("mapreduce.reduce.java.opts") == 600.0);
      }
      if (mrApplicationData.getApplicationID().equals("application_1458194917883_1453362")) {
        assertTrue("Mapper Max Virtual Memory (MB) ", usedParameter.get("Mapper Max Virtual Memory (MB)") == 2200.0);
        assertTrue("Mapper Max Physical Memory (MB)", usedParameter.get("Mapper Max Physical Memory (MB)") == 595.0);
        assertTrue("Mapper Max Total Committed Heap Usage Memory (MB)",
            usedParameter.get("Mapper Max Total Committed Heap Usage Memory (MB)") == 300.0);
        assertTrue(" Mapper Memory Suggested ", suggestedParameter.get("mapreduce.map.memory.mb") == 2048.0);
        assertTrue(" Mapper Memory Heap Suggested ", suggestedParameter.get("mapreduce.map.java.opts") == 600.0);

        assertTrue("Reducer Max Virtual Memory (MB)", usedParameter.get("Reducer Max Virtual Memory (MB)") == 2100.0);
        assertTrue("Reducer Max Physical Memory (MB)", usedParameter.get("Reducer Max Physical Memory (MB)") == 497.0);
        assertTrue("Reducer Max Total Committed Heap Usage Memory (MB)",
            usedParameter.get("Reducer Max Total Committed Heap Usage Memory (MB)") == 300.0);
        assertTrue(" Reducer Memory Suggested " + suggestedParameter.get("mapreduce.reduce.memory.mb"),
            suggestedParameter.get("mapreduce.reduce.memory.mb") == 1024.0);
        assertTrue("Reducer Memory Heap Suggested ", suggestedParameter.get("mapreduce.reduce.java.opts") == 600.0);
      }
    }

/*

    ParameterGenerateManagerHBTMR<MRExecutionEngine> parameterGenerateManagerHBTMR =
        new ParameterGenerateManagerHBTMR<MRExecutionEngine>(mrExecutionEngine);

   // parameterGenerateManagerHBTMR.
*/





   /* AbstractParameterGenerateManager parameterGenerateManager =
        new ParameterGenerateManagerHBTMR<MRExecutionEngine>(new MRExecutionEngine());
    List<JobTuningInfo> jobTuningInfos = parameterGenerateManager.detectJobsForParameterGeneration();
    assertTrue(" Job Needed for Param Generation " + jobTuningInfos.size(), jobTuningInfos.size() == 1);
    assertTrue(" Job State " + jobTuningInfos.get(0).getTunerState().replaceAll("\\{\\}", ""),
        jobTuningInfos.get(0).getTunerState().replaceAll("\\{\\}", "").length() == 0);

    List<JobTuningInfo> updatedJobTuningInfoList = parameterGenerateManager.generateParameters(jobTuningInfos);
    assertTrue(" Job State After param generation " + updatedJobTuningInfoList.get(0).getTunerState(),
        updatedJobTuningInfoList.get(0).getTunerState().replaceAll("\\{\\}", "").length() > 0);

    List<JobSuggestedParamSet> jobSuggestedParamSets = JobSuggestedParamSet.find.select("*")
        .where()
        .eq(JobSuggestedParamSet.TABLE.paramSetState,JobSuggestedParamSet.ParamSetStatus.CREATED)
        .findList();

    assertTrue(" Parameter Created  "+jobSuggestedParamSets.size(),jobSuggestedParamSets.size()==0);

    boolean isDataBaseUpdated = parameterGenerateManager.updateDatabase(updatedJobTuningInfoList);

    assertTrue(" Updated database with param generation "+isDataBaseUpdated,isDataBaseUpdated);

    List<JobSuggestedParamSet> jobSuggestedParamSets1 = JobSuggestedParamSet.find.select("*")
        .where()
        .eq(JobSuggestedParamSet.TABLE.paramSetState,JobSuggestedParamSet.ParamSetStatus.CREATED)
        .findList();


    assertTrue(" Parameter Created  "+jobSuggestedParamSets1.size(),jobSuggestedParamSets1.size()==1);
*/

  }
}
