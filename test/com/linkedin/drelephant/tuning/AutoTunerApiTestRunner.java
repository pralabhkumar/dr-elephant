package com.linkedin.drelephant.tuning;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import models.FlowExecution;
import static org.junit.Assert.*;
import static play.test.Helpers.*;
import static common.DBTestUtil.*;



public class AutoTunerApiTestRunner implements Runnable {
  @Override
  public void run() {
    ExecutorService executor = Executors.newFixedThreadPool(5);
    final List<FlowExecution> flowExecutions = new ArrayList<FlowExecution>();
    for (int i = 0; i < 50; i++) {
      final TuningInput tuningInput = new TuningInput();
      tuningInput.setFlowExecId(1 + "");
      tuningInput.setFlowExecUrl(1 + "");
      tuningInput.setFlowDefId(i + "");
      tuningInput.setFlowDefUrl(i + "");
      final AutoTuningAPIHelper autoTuningAPIHelper = new AutoTuningAPIHelper();
      Runnable worker = new Thread() {
        public void run() {
          flowExecutions.add(autoTuningAPIHelper.getFlowExecution(tuningInput));
        }
      };
      executor.execute(worker);
    }
    executor.shutdown();
    try {
      executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    for (FlowExecution _flowExecution : flowExecutions) {
      assertTrue(" flow execution ", _flowExecution != null);
    }
  }
}
