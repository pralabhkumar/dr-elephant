package com.linkedin.drelephant.spark.exception;

import com.linkedin.drelephant.analysis.AnalyticJob;
import com.linkedin.drelephant.analysis.HadoopApplicationData;
import com.linkedin.drelephant.exceptions.spark.ExceptionFingerprintingFactory;
import com.linkedin.drelephant.exceptions.spark.ExceptionFingerprintingRunner;
import models.AppResult;
import models.JobExecution;

import static common.DBTestUtil.*;

import static org.junit.Assert.*;
import static play.test.Helpers.*;
import static common.DBTestUtil.*;

public class ExceptionFingerprintingRunnerTest implements Runnable {
  private HadoopApplicationData data ;
  private AnalyticJob _analyticJob;
  public ExceptionFingerprintingRunnerTest(HadoopApplicationData data,AnalyticJob analyticJob){
    this._analyticJob = analyticJob;
    this.data = data;
  }
  private void populateTestData() {
    try {
      initDBIPSO();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  public void run() {
    populateTestData();
    AppResult _appResult = AppResult.find.byId("application_1458194917883_1453361");
    ExceptionFingerprintingRunner runner = new ExceptionFingerprintingRunner(_analyticJob,_appResult,data,
        ExceptionFingerprintingFactory.ExecutionEngineTypes.SPARK);
    runner.run();
    JobExecution jobExecution =
        JobExecution.find.where().eq(JobExecution.TABLE.jobExecId, "https://ltx1-holdemaz01.grid.linkedin.com:8443/executor?execid=5416293&job=countByCountryFlow_countByCountry&attempt=0").findUnique();
    assertTrue("job execution status  "+jobExecution.autoTuningFault, jobExecution.autoTuningFault == true);
  }

}
