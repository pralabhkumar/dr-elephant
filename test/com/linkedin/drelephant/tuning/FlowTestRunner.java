package com.linkedin.drelephant.tuning;

import com.linkedin.drelephant.tuning.Schduler.AzkabanJobStatusManager;
import com.linkedin.drelephant.tuning.engine.MRExecutionEngine;
import com.linkedin.drelephant.tuning.engine.SparkExecutionEngine;
import com.linkedin.drelephant.tuning.hbt.BaselineManagerHBT;
import com.linkedin.drelephant.tuning.hbt.FitnessManagerHBT;
import com.linkedin.drelephant.tuning.hbt.TuningTypeManagerHBT;
import com.linkedin.drelephant.tuning.obt.BaselineManagerOBT;
import com.linkedin.drelephant.tuning.obt.FitnessManagerOBTAlgoIPSO;
import com.linkedin.drelephant.tuning.obt.FitnessManagerOBTAlgoPSO;
import com.linkedin.drelephant.tuning.obt.TuningTypeManagerOBTAlgoIPSO;
import com.linkedin.drelephant.tuning.obt.TuningTypeManagerOBTAlgoPSO;
import java.util.List;

import static org.junit.Assert.*;
import static play.test.Helpers.*;


public class FlowTestRunner implements Runnable {
  @Override
  public void run() {
    Flow flow = new Flow();
    testPipeline(flow);
    testCreateBaseLineManagersPipeline(flow);
    testCreateJobStatusManagersPipeline(flow);
    testCreateFitnessManagersPipeline(flow);
    testCreateTuningTypeManagersPipeline(flow);
    testCreateTuningTypeManagersPipeline(flow);
  }

  private void testPipeline(Flow flow) {
    List<List<Manager>> pipelines = flow.getPipeline();
    assertTrue(" Total Number of pipeline ", pipelines.size() == 4);
  }

  private void testCreateBaseLineManagersPipeline(Flow flow) {
    List<List<Manager>> pipelines = flow.getPipeline();
    List<Manager> baseLineManagers = pipelines.get(0);
    assertTrue(" Total Number of Base line Managers ", baseLineManagers.size() == 2);
    assertTrue(" HBT Baseline Manager ", baseLineManagers.get(0) instanceof BaselineManagerHBT);
    assertTrue(" OBT Baseline Manager ", baseLineManagers.get(1) instanceof BaselineManagerOBT);
  }

  private void testCreateJobStatusManagersPipeline(Flow flow) {
    List<List<Manager>> pipelines = flow.getPipeline();
    List<Manager> jobStatusManagers = pipelines.get(1);
    assertTrue(" Total Number of Base line Managers ", jobStatusManagers.size() == 1);
    assertTrue(" Azkaban Job Status Manager ", jobStatusManagers.get(0) instanceof AzkabanJobStatusManager);
  }

  private void testCreateFitnessManagersPipeline(Flow flow) {
    List<List<Manager>> pipelines = flow.getPipeline();
    List<Manager> fitnessManagers = pipelines.get(2);
    assertTrue(" Total Number of fitness Managers  ", fitnessManagers.size() == 3);
    assertTrue(" FitnessManagerHBT ", fitnessManagers.get(0) instanceof FitnessManagerHBT);
    assertTrue(" FitnessManagerOBTPSO ", fitnessManagers.get(1) instanceof FitnessManagerOBTAlgoPSO);
    assertTrue(" FitnessManagerOBTIPSO ", fitnessManagers.get(2) instanceof FitnessManagerOBTAlgoIPSO);
  }

  private void testCreateTuningTypeManagersPipeline(Flow flow) {
    List<List<Manager>> pipelines = flow.getPipeline();
    List<Manager> tuningTypeManagers = pipelines.get(3);
    assertTrue(" Total Number of tuningType Managers   ", tuningTypeManagers.size() == 6);

    assertTrue(" TuningTypeManagerHBT ", tuningTypeManagers.get(0) instanceof TuningTypeManagerHBT);
    assertTrue(" TuningTypeManagerHBTMR ",
        ((TuningTypeManagerHBT) tuningTypeManagers.get(0))._executionEngine instanceof MRExecutionEngine);

    assertTrue(" TuningTypeManagerHBT ", tuningTypeManagers.get(1) instanceof TuningTypeManagerHBT);
    assertTrue(" TuningTypeManagerHBTSpark ",
        ((TuningTypeManagerHBT) tuningTypeManagers.get(1))._executionEngine instanceof SparkExecutionEngine);

    assertTrue(" TuningTypeManagerOBTPSO ", tuningTypeManagers.get(2) instanceof TuningTypeManagerOBTAlgoPSO);
    assertTrue(" TuningTypeManagerOBTPSOMR ",
        ((TuningTypeManagerOBTAlgoPSO) tuningTypeManagers.get(2))._executionEngine instanceof MRExecutionEngine);

    assertTrue(" TuningTypeManagerOBTPSO ", tuningTypeManagers.get(3) instanceof TuningTypeManagerOBTAlgoPSO);
    assertTrue(" TuningTypeManagerOBTPSOSpark ",
        ((TuningTypeManagerOBTAlgoPSO) tuningTypeManagers.get(3))._executionEngine instanceof SparkExecutionEngine);

    assertTrue(" TuningTypeManagerOBTIPSO ", tuningTypeManagers.get(4) instanceof TuningTypeManagerOBTAlgoIPSO);
    assertTrue(" TuningTypeManagerOBTIPSOMR ",
        ((TuningTypeManagerOBTAlgoIPSO) tuningTypeManagers.get(4))._executionEngine instanceof MRExecutionEngine);

    assertTrue(" TuningTypeManagerOBTIPSO ", tuningTypeManagers.get(5) instanceof TuningTypeManagerOBTAlgoIPSO);
    assertTrue(" TuningTypeManagerOBTPSOSpark ",
        ((TuningTypeManagerOBTAlgoIPSO) tuningTypeManagers.get(5))._executionEngine instanceof SparkExecutionEngine);
  }
}
 /* StringBuffer stringBuffer = new StringBuffer();
    for(Manager manager :  tuningTypeManagers){
      stringBuffer.append(manager.getClass().getName()).append(" ");
    }*/
