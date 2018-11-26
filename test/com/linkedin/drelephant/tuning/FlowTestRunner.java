package com.linkedin.drelephant.tuning;

import com.linkedin.drelephant.tuning.Schduler.AzkabanJobStatusManager;
import com.linkedin.drelephant.tuning.engine.MRExecutionEngine;
import com.linkedin.drelephant.tuning.engine.SparkExecutionEngine;
import com.linkedin.drelephant.tuning.hbt.BaselineManagerHBT;
import com.linkedin.drelephant.tuning.hbt.FitnessManagerHBT;
import com.linkedin.drelephant.tuning.hbt.ParameterGenerateManagerHBT;
import com.linkedin.drelephant.tuning.hbt.ParameterGenerateManagerHBTMR;
import com.linkedin.drelephant.tuning.hbt.ParameterGenerateManagerHBTSpark;
import com.linkedin.drelephant.tuning.obt.BaselineManagerOBT;
import com.linkedin.drelephant.tuning.obt.FitnessManagerOBTAlgoIPSO;
import com.linkedin.drelephant.tuning.obt.FitnessManagerOBTAlgoPSO;
import com.linkedin.drelephant.tuning.obt.ParameterGenerateManagerOBTAlgoPSOIPSOImpl;
import com.linkedin.drelephant.tuning.obt.ParameterGenerateManagerOBTAlgoPSO;
import com.linkedin.drelephant.tuning.obt.ParameterGenerateManagerOBTAlgoPSOIPSOMRImpl;
import com.linkedin.drelephant.tuning.obt.ParameterGenerateManagerOBTAlgoPSOIPSOSparkImpl;
import com.linkedin.drelephant.tuning.obt.ParameterGenerateManagerOBTAlgoPSOImpl;
import com.linkedin.drelephant.tuning.obt.ParameterGenerateManagerOBTAlgoPSOMRImpl;
import com.linkedin.drelephant.tuning.obt.ParameterGenerateManagerOBTAlgoPSOSparkImpl;
import java.util.List;

import static org.junit.Assert.*;
import static play.test.Helpers.*;


public class FlowTestRunner implements Runnable {
  @Override
  public void run() {
    AutoTuningFlow autoTuningFlow = new AutoTuningFlow();
    testPipeline(autoTuningFlow);
    testCreateBaseLineManagersPipeline(autoTuningFlow);
    testCreateJobStatusManagersPipeline(autoTuningFlow);
    testCreateFitnessManagersPipeline(autoTuningFlow);
    testCreateTuningTypeManagersPipeline(autoTuningFlow);
    testCreateTuningTypeManagersPipeline(autoTuningFlow);
  }

  private void testPipeline(AutoTuningFlow autoTuningFlow) {
    List<List<Manager>> pipelines = autoTuningFlow.getPipeline();
    assertTrue(" Total Number of pipeline ", pipelines.size() == 4);
  }

  private void testCreateBaseLineManagersPipeline(AutoTuningFlow autoTuningFlow) {
    List<List<Manager>> pipelines = autoTuningFlow.getPipeline();
    List<Manager> baseLineManagers = pipelines.get(0);
    assertTrue(" Total Number of Base line Managers ", baseLineManagers.size() == 2);
    assertTrue(" HBT Baseline Manager ", baseLineManagers.get(0) instanceof BaselineManagerHBT);
    assertTrue(" OBT Baseline Manager ", baseLineManagers.get(1) instanceof BaselineManagerOBT);
  }

  private void testCreateJobStatusManagersPipeline(AutoTuningFlow autoTuningFlow) {
    List<List<Manager>> pipelines = autoTuningFlow.getPipeline();
    List<Manager> jobStatusManagers = pipelines.get(1);
    assertTrue(" Total Number of Base line Managers ", jobStatusManagers.size() == 1);
    assertTrue(" Azkaban Job Status Manager ", jobStatusManagers.get(0) instanceof AzkabanJobStatusManager);
  }

  private void testCreateFitnessManagersPipeline(AutoTuningFlow autoTuningFlow) {
    List<List<Manager>> pipelines = autoTuningFlow.getPipeline();
    List<Manager> fitnessManagers = pipelines.get(2);
    assertTrue(" Total Number of fitness Managers  ", fitnessManagers.size() == 3);
    assertTrue(" FitnessManagerHBT ", fitnessManagers.get(0) instanceof FitnessManagerHBT);
    assertTrue(" FitnessManagerOBTPSO ", fitnessManagers.get(1) instanceof FitnessManagerOBTAlgoPSO);
    assertTrue(" FitnessManagerOBTIPSO ", fitnessManagers.get(2) instanceof FitnessManagerOBTAlgoIPSO);
  }

  private void testCreateTuningTypeManagersPipeline(AutoTuningFlow autoTuningFlow) {
    List<List<Manager>> pipelines = autoTuningFlow.getPipeline();
    List<Manager> tuningTypeManagers = pipelines.get(3);
    assertTrue(" Total Number of tuningType Managers   ", tuningTypeManagers.size() == 6);

   // assertTrue(" TuningTypeManagerHBT ", tuningTypeManagers.get(0) instanceof ParameterGenerateManagerHBT);
    assertTrue(" TuningTypeManagerHBTMR ",
        (tuningTypeManagers.get(0) instanceof ParameterGenerateManagerHBTMR));

   // assertTrue(" TuningTypeManagerHBT ", tuningTypeManagers.get(1) instanceof ParameterGenerateManagerHBT);
    assertTrue(" TuningTypeManagerHBTSpark ",
        (tuningTypeManagers.get(1) instanceof ParameterGenerateManagerHBTSpark));

   // assertTrue(" TuningTypeManagerOBTPSO ", tuningTypeManagers.get(2) instanceof ParameterGenerateManagerOBTAlgoPSO);
    assertTrue(" TuningTypeManagerOBTPSOMR ",
        (tuningTypeManagers.get(2) instanceof ParameterGenerateManagerOBTAlgoPSOMRImpl));

  //  assertTrue(" TuningTypeManagerOBTPSO ", tuningTypeManagers.get(3) instanceof ParameterGenerateManagerOBTAlgoPSO);
    assertTrue(" TuningTypeManagerOBTPSOSpark ",
        (tuningTypeManagers.get(3) instanceof ParameterGenerateManagerOBTAlgoPSOSparkImpl));

  //  assertTrue(" TuningTypeManagerOBTIPSO ", tuningTypeManagers.get(4) instanceof ParameterGenerateManagerOBTAlgoPSOIPSOImpl);
    assertTrue(" TuningTypeManagerOBTIPSOMR ",
        (tuningTypeManagers.get(4) instanceof ParameterGenerateManagerOBTAlgoPSOIPSOMRImpl));

  //  assertTrue(" TuningTypeManagerOBTIPSO ", tuningTypeManagers.get(5) instanceof ParameterGenerateManagerOBTAlgoPSOIPSOImpl);
    assertTrue(" TuningTypeManagerOBTPSOSpark ",
        (tuningTypeManagers.get(5) instanceof ParameterGenerateManagerOBTAlgoPSOIPSOSparkImpl));
  }
}
 /* StringBuffer stringBuffer = new StringBuffer();
    for(Manager manager :  tuningTypeManagers){
      stringBuffer.append(manager.getClass().getName()).append(" ");
    }*/
