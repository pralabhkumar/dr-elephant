package com.linkedin.drelephant.tuning;

import com.linkedin.drelephant.tuning.Schduler.AzkabanJobStatusManager;
import com.linkedin.drelephant.tuning.engine.MRExecutionEngine;
import com.linkedin.drelephant.tuning.engine.SparkExecutionEngine;
import com.linkedin.drelephant.tuning.hbt.BaselineManagerHBT;
import com.linkedin.drelephant.tuning.hbt.FitnessManagerHBT;
import com.linkedin.drelephant.tuning.hbt.TuningTypeManagerHBT;
import com.linkedin.drelephant.tuning.obt.FitnessManagerOBTAlgoIPSO;
import com.linkedin.drelephant.tuning.obt.TuningTypeManagerOBT;
import com.linkedin.drelephant.tuning.obt.BaselineManagerOBT;
import com.linkedin.drelephant.tuning.obt.FitnessManagerOBTAlgoPSO;
import com.linkedin.drelephant.tuning.obt.TuningTypeManagerOBTAlgoIPSO;
import com.linkedin.drelephant.tuning.obt.TuningTypeManagerOBTAlgoPSO;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;


public class Flow {
  List<List<Manager>> pipelines = null;
  private static final Logger logger = Logger.getLogger(Flow.class);

  public Flow() {
    pipelines = new ArrayList<List<Manager>>();
    createBaseLineManagersPipeline();
    createJobStatusManagersPipeline();
    createFitnessManagersPipeline();
    createTuningTypemManagersPipeline();
  }

  public void executeFlow() {
    for (final List<Manager> pipelineType : this.pipelines) {
      new Thread(new Runnable() {
        @Override
        public void run() {
          for (Manager manager : pipelineType) {
            Boolean execute = manager.execute();
            logger.info(" Manager execution Status " + execute + " " + manager.getManagerName());
          }
        }
      }).start();
    }
  }

  public void createBaseLineManagersPipeline() {
    List<Manager> baselineManagers = new ArrayList<Manager>();
    for (Constant.TuningType tuningType : Constant.TuningType.values()) {
      baselineManagers.add(
          ManagerFactory.getManager(tuningType.name(), null, null, AbstractBaselineManager.class.getName()));
    }
    this.pipelines.add(baselineManagers);
  }

  public void createJobStatusManagersPipeline() {
    List<Manager> jobStatusManagers = new ArrayList<Manager>();
    jobStatusManagers.add(ManagerFactory.getManager(null, null, null, AbstractJobStatusManager.class.getName()));
    //jobStatusManagers.add(new JobStatusManagerOBT());
    this.pipelines.add(jobStatusManagers);
  }

  public void createFitnessManagersPipeline() {
    List<Manager> fitnessManagers = new ArrayList<Manager>();
    for (Constant.TuningType tuningType : Constant.TuningType.values()) {
      for (Constant.AlgotihmType algotihmType : Constant.AlgotihmType.values()) {
        Manager manager = ManagerFactory.getManager(tuningType.name(), algotihmType.name(), null,
            AbstractFitnessManager.class.getName());
        if (manager != null) {
          fitnessManagers.add(manager);
        }
      }
    }
    this.pipelines.add(fitnessManagers);
  }

  public void createTuningTypemManagersPipeline() {
    List<Manager> algorithmManagers = new ArrayList<Manager>();
    for (Constant.TuningType tuningType : Constant.TuningType.values()) {
      for (Constant.AlgotihmType algotihmType : Constant.AlgotihmType.values()) {
        for (Constant.ExecutionEngineTypes executionEngineTypes : Constant.ExecutionEngineTypes.values()) {
          Manager manager =
              ManagerFactory.getManager(tuningType.name(), algotihmType.name(), executionEngineTypes.name(),
                  AbstractTuningTypeManager.class.getName());
          if (manager != null) {
            algorithmManagers.add(manager);
          }
        }
      }
    }
    this.pipelines.add(algorithmManagers);
  }
}
