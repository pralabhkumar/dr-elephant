package com.linkedin.drelephant.tuning;

import com.linkedin.drelephant.tuning.engine.MRExecutionEngine;
import com.linkedin.drelephant.tuning.hbt.FitnessManagerHBT;
import com.linkedin.drelephant.tuning.hbt.MRApplicationData;
import com.linkedin.drelephant.tuning.hbt.MRJob;
import com.linkedin.drelephant.tuning.hbt.ParameterGenerateManagerHBT;
import com.linkedin.drelephant.tuning.hbt.ParameterGenerateManagerHBTMR;
import java.text.DecimalFormat;
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
  private Map<String, String> appliedParameter = null;
  private List<MRApplicationData> mrApplicationDatas = null;
  private Map<String, Double> suggestedParameter = null;

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
    testMemoryAndNumberOfTaskRecommendations();
    testNumberOfReducerTaskAndMapperSpillRecommendations();
    testNoHeuristicFailRecommendations();
  }

  private void testMemoryAndNumberOfTaskRecommendations() {
    MRExecutionEngine mrExecutionEngine = new MRExecutionEngine();
    List<AppResult> results = AppResult.find.select("*")
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, "*")
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS + "." + AppHeuristicResult.TABLE.APP_HEURISTIC_RESULT_DETAILS, "*")
        .where()
        .eq(AppResult.TABLE.FLOW_EXEC_ID, "https://ltx1-holdemaz01.grid.linkedin.com:8443/executor?execid=5416293")
        .eq(AppResult.TABLE.JOB_EXEC_ID,
            "https://ltx1-holdemaz01.grid.linkedin.com:8443/executor?execid=5416293&job=countByCountryFlow_countByCountry&attempt=0")
        .findList();
    processData(mrExecutionEngine, results);
    assertTrue(" Number of MapReduce Application ", results.size() == 2);
    assertTrue(" Mapper Memory " + appliedParameter.size(), appliedParameter.get("Mapper Memory").equals("2048"));
    assertTrue(" Reducer Memory ", appliedParameter.get("Reducer Memory").equals("2048"));
    assertTrue(" Sort Buffer ", appliedParameter.get("Sort Buffer").equals("100"));
    assertTrue(" Sort Spill ", appliedParameter.get("Sort Spill").equals("0.80"));
    assertTrue(" Split Size ", appliedParameter.get("Split Size").equals("9223372036854775807"));

    testApplicationRecommendedMemoryParameter(mrApplicationDatas);
    testApplicationRecommendedNumberoFTasks(mrApplicationDatas);
    testJobRecommendedMemoryParameter(suggestedParameter);
  }

  private void testNumberOfReducerTaskAndMapperSpillRecommendations() {
    MRExecutionEngine mrExecutionEngine = new MRExecutionEngine();
    List<AppResult> results = AppResult.find.select("*")
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, "*")
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS + "." + AppHeuristicResult.TABLE.APP_HEURISTIC_RESULT_DETAILS, "*")
        .where()
        .eq(AppResult.TABLE.FLOW_EXEC_ID, "https://ltx1-faroaz02.grid.linkedin.com:8443/executor?execid=1200332")
        .eq(AppResult.TABLE.JOB_EXEC_ID,
            "https://ltx1-faroaz02.grid.linkedin.com:8443/executor?execid=1200332&job=untitled&attempt=0")
        .findList();
    assertTrue(" Number of MapReduce Application " + results.size(), results.size() == 1);
    processData(mrExecutionEngine, results);
    testApplicationRecommendedReducerTaskMapperSpill(mrApplicationDatas);
    testJobRecommendedReducerTaskAndMapperSpill(suggestedParameter);
  }

  private void testNoHeuristicFailRecommendations() {
    MRExecutionEngine mrExecutionEngine = new MRExecutionEngine();
    List<AppResult> results = AppResult.find.select("*")
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, "*")
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS + "." + AppHeuristicResult.TABLE.APP_HEURISTIC_RESULT_DETAILS, "*")
        .where()
        .eq(AppResult.TABLE.FLOW_EXEC_ID, "https://ltx1-faroaz01.grid.linkedin.com:8443/executor?execid=1342802")
        .eq(AppResult.TABLE.JOB_EXEC_ID,
            "https://ltx1-faroaz01.grid.linkedin.com:8443/executor?execid=1342802&job=fetl-dupe_fetlDupeLog&attempt=0")
        .findList();
    assertTrue(" Number of MapReduce Application " + results.size(), results.size() == 1);
    processData(mrExecutionEngine, results);
    testApplicationRecommendedForRU(mrApplicationDatas);
    testJobRecommendedForRU(suggestedParameter);
  }

  private void processData(MRExecutionEngine mrExecutionEngine, List<AppResult> results) {
    MRJob mrJob = new MRJob(results, mrExecutionEngine);
    appliedParameter = mrJob.getAppliedParameter();
    mrJob.analyzeAllApplications();
    mrApplicationDatas = mrJob.getApplicationAnalyzedData();
    mrJob.processJobForParameter();
    suggestedParameter = mrJob.getJobSuggestedParameter();
  }

  private void testApplicationRecommendedReducerTaskMapperSpill(List<MRApplicationData> mrApplicationDatas) {
    for (MRApplicationData mrApplicationData : mrApplicationDatas) {
      assertTrue(" Application IDs ", mrApplicationData.getApplicationID().equals("application_1540411174627_3924329"));
      Map<String, Double> suggestedParameter = mrApplicationData.getSuggestedParameter();
      Map<String, Double> usedParameter = mrApplicationData.getCounterValues();
      assertTrue("Reducer Number of tasks) " + usedParameter.get("Reducer Number of tasks"),
          usedParameter.get("Reducer Number of tasks") == 51);
      assertTrue(
          "Reducer Average task runtime " + Math.round(usedParameter.get("Reducer Average task runtime") * 100) / 100.0,
          Math.round(usedParameter.get("Reducer Average task runtime") * 100) / 100.0 == 176.83);
      assertTrue(" Number of Reducer recommended ", suggestedParameter.get("mapreduce.job.reduces") == 102);

      assertTrue("Ratio of spilled records to output records " + usedParameter.get(
          "Ratio of spilled records to output records"),
          Math.round(usedParameter.get("Ratio of spilled records to output records") * 100) / 100.0 == 3.13);
      assertTrue("Sort Buffer " + usedParameter.get("Sort Buffer"), usedParameter.get("Sort Buffer") == 100);
      assertTrue("Sort Spill " + usedParameter.get("Sort Spill"),
          Math.round(usedParameter.get("Sort Spill") * 100) / 100.0 == 0.80);
      assertTrue("Suggested Sort Buffer " + suggestedParameter.get("mapreduce.task.io.sort.mb"),
          suggestedParameter.get("mapreduce.task.io.sort.mb") == 120);
      assertTrue("Suggested Sort Spill " + suggestedParameter.get("mapreduce.map.sort.spill.percent"),
          Math.round(suggestedParameter.get("mapreduce.map.sort.spill.percent") * 100) / 100.0 == 0.85);
    }
  }

  private void testJobRecommendedReducerTaskAndMapperSpill(Map<String, Double> suggestedParameter) {
    assertTrue(" Total  Parameter Suggested ", suggestedParameter.keySet().size() == 3);
    assertTrue(" Number of Reducer recommended ", suggestedParameter.get("mapreduce.job.reduces") == 102);
    assertTrue(" Suggested Sort Buffer  ", suggestedParameter.get("mapreduce.task.io.sort.mb") == 120);
    assertTrue(" Suggested Sort Spill ",
        Math.round(suggestedParameter.get("mapreduce.map.sort.spill.percent") * 100) / 100.0 == 0.85);
  }

  private void testApplicationRecommendedMemoryParameter(List<MRApplicationData> mrApplicationDatas) {
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
        assertTrue(" Mapper Memory Recommended ", suggestedParameter.get("mapreduce.map.memory.mb") == 1024.0);
        assertTrue(" Mapper Memory Heap Recommended ", suggestedParameter.get("mapreduce.map.java.opts") == 600.0);

        assertTrue("Reducer Max Virtual Memory (MB)", usedParameter.get("Reducer Max Virtual Memory (MB)") == 1350.0);
        assertTrue("Reducer Max Physical Memory (MB)", usedParameter.get("Reducer Max Physical Memory (MB)") == 497.0);
        assertTrue("Reducer Max Total Committed Heap Usage Memory (MB)",
            usedParameter.get("Reducer Max Total Committed Heap Usage Memory (MB)") == 300.0);
        assertTrue(" Reducer Memory Recommended " + suggestedParameter.get("mapreduce.reduce.memory.mb"),
            suggestedParameter.get("mapreduce.reduce.memory.mb") == 1024.0);
        assertTrue("Reducer Memory Heap Recommended ", suggestedParameter.get("mapreduce.reduce.java.opts") == 600.0);
      }
      if (mrApplicationData.getApplicationID().equals("application_1458194917883_1453362")) {
        assertTrue("Mapper Max Virtual Memory (MB) ", usedParameter.get("Mapper Max Virtual Memory (MB)") == 2200.0);
        assertTrue("Mapper Max Physical Memory (MB)", usedParameter.get("Mapper Max Physical Memory (MB)") == 595.0);
        assertTrue("Mapper Max Total Committed Heap Usage Memory (MB)",
            usedParameter.get("Mapper Max Total Committed Heap Usage Memory (MB)") == 300.0);
        assertTrue(" Mapper Memory Recommended ", suggestedParameter.get("mapreduce.map.memory.mb") == 2048.0);
        assertTrue(" Mapper Memory Heap Recommended ", suggestedParameter.get("mapreduce.map.java.opts") == 600.0);

        assertTrue("Reducer Max Virtual Memory (MB)", usedParameter.get("Reducer Max Virtual Memory (MB)") == 2100.0);
        assertTrue("Reducer Max Physical Memory (MB)", usedParameter.get("Reducer Max Physical Memory (MB)") == 497.0);
        assertTrue("Reducer Max Total Committed Heap Usage Memory (MB)",
            usedParameter.get("Reducer Max Total Committed Heap Usage Memory (MB)") == 300.0);
        assertTrue(" Reducer Memory Recommended " + suggestedParameter.get("mapreduce.reduce.memory.mb"),
            suggestedParameter.get("mapreduce.reduce.memory.mb") == 1024.0);
        assertTrue("Reducer Memory Heap Recommended ", suggestedParameter.get("mapreduce.reduce.java.opts") == 600.0);
      }
    }
  }

  private void testJobRecommendedMemoryParameter(Map<String, Double> suggestedParameter) {
    assertTrue(" Total  Parameter Suggested ", suggestedParameter.keySet().size() == 6);
    assertTrue(" Mapper Memory Suggested ", suggestedParameter.get("mapreduce.map.memory.mb") == 2048.0);
    assertTrue(" Mapper Memory Heap Recommended ", suggestedParameter.get("mapreduce.map.java.opts") == 600.0);
    assertTrue(" Reducer Memory Suggested ", suggestedParameter.get("mapreduce.reduce.memory.mb") == 1024.0);
    assertTrue(" Reducer Memory Heap Recommended ", suggestedParameter.get("mapreduce.reduce.java.opts") == 600.0);
    assertTrue(" Split Size Recommneded ", suggestedParameter.get("pig.maxCombinedSplitSize") == 161480704);
    assertTrue(" Number of Reducer ", suggestedParameter.get("mapreduce.job.reduces") == 370);
  }

  private void testApplicationRecommendedNumberoFTasks(List<MRApplicationData> mrApplicationDatas) {
    for (MRApplicationData mrApplicationData : mrApplicationDatas) {
      assertTrue(" Application IDs ", mrApplicationData.getApplicationID().equals("application_1458194917883_1453361")
          || mrApplicationData.getApplicationID().equals("application_1458194917883_1453362"));
      Map<String, Double> suggestedParameter = mrApplicationData.getSuggestedParameter();
      Map<String, Double> usedParameter = mrApplicationData.getCounterValues();
      if (mrApplicationData.getApplicationID().equals("application_1458194917883_1453361")) {
        assertTrue("Mapper Average task input size) " + usedParameter.get("Mapper Average task input size"),
            usedParameter.get("Mapper Average task input size") == 80740352);
        assertTrue(
            "Mapper Average task runtime " + Math.round(usedParameter.get("Mapper Average task runtime") * 100) / 100.0,
            Math.round(usedParameter.get("Mapper Average task runtime") * 100) / 100.0 == 0.47);
        assertTrue(" Split Size Recommneded ", suggestedParameter.get("pig.maxCombinedSplitSize") == 161480704);
      }
      if (mrApplicationData.getApplicationID().equals("application_1458194917883_1453362")) {
        assertTrue("Reducer Number of tasks) " + usedParameter.get("Reducer Number of tasks"),
            usedParameter.get("Reducer Number of tasks") == 741.0);
        assertTrue("Reducer Average task runtime "
                + Math.round(usedParameter.get("Reducer Average task runtime") * 100) / 100.0,
            Math.round(usedParameter.get("Reducer Average task runtime") * 100) / 100.0 == 0.12);
        assertTrue(" Number of Reducer recommended ", suggestedParameter.get("mapreduce.job.reduces") == 370);
      }
    }
  }

  private void testApplicationRecommendedForRU(List<MRApplicationData> mrApplicationDatas) {
    for (MRApplicationData mrApplicationData : mrApplicationDatas) {
      assertTrue(" Application IDs ", mrApplicationData.getApplicationID().equals("application_1540411174627_1086799"));
      Map<String, Double> suggestedParameter = mrApplicationData.getSuggestedParameter();
      Map<String, Double> usedParameter = mrApplicationData.getCounterValues();
      assertTrue("Mapper Max Virtual Memory (MB) " + usedParameter.get("Mapper Max Virtual Memory (MB)"),
          usedParameter.get("Mapper Max Virtual Memory (MB)") == 2330);
      assertTrue("Mapper Max Physical Memory (MB)", usedParameter.get("Mapper Max Physical Memory (MB)") == 325);
      assertTrue("Mapper Max Total Committed Heap Usage Memory (MB)",
          usedParameter.get("Mapper Max Total Committed Heap Usage Memory (MB)") == 619);
      assertTrue(" Mapper Memory Recommended ", suggestedParameter.get("mapreduce.map.memory.mb") == 2048.0);
      assertTrue(" Mapper Memory Heap Recommended " + suggestedParameter.get("mapreduce.map.java.opts"),
          suggestedParameter.get("mapreduce.map.java.opts") == 619);

      assertTrue("Reducer Max Virtual Memory (MB)", usedParameter.get("Reducer Max Virtual Memory (MB)") == 2335);
      assertTrue("Reducer Max Physical Memory (MB)", usedParameter.get("Reducer Max Physical Memory (MB)") == 425);
      assertTrue("Reducer Max Total Committed Heap Usage Memory (MB)",
          usedParameter.get("Reducer Max Total Committed Heap Usage Memory (MB)") == 619);
      assertTrue(" Reducer Memory Recommended " + suggestedParameter.get("mapreduce.reduce.memory.mb"),
          suggestedParameter.get("mapreduce.reduce.memory.mb") == 2048.0);
      assertTrue("Reducer Memory Heap Recommended ", suggestedParameter.get("mapreduce.reduce.java.opts") == 619);
    }
  }
  private void testJobRecommendedForRU(Map<String, Double> suggestedParameter) {

    assertTrue(" Total  Parameter Suggested ", suggestedParameter.keySet().size() == 4);
    assertTrue(" Mapper Memory Suggested ", suggestedParameter.get("mapreduce.map.memory.mb") == 2048.0);
    assertTrue(" Mapper Memory Heap Recommended ", suggestedParameter.get("mapreduce.map.java.opts") == 619.0);
    assertTrue(" Reducer Memory Suggested ", suggestedParameter.get("mapreduce.reduce.memory.mb") == 2048.0);
    assertTrue(" Reducer Memory Heap Recommended ", suggestedParameter.get("mapreduce.reduce.java.opts") == 619);
    
  }
}
