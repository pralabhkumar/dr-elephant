package com.linkedin.drelephant.tuning;

import com.linkedin.drelephant.tuning.Schduler.AzkabanJobStatusManager;
import com.linkedin.drelephant.tuning.engine.MRExecutionEngine;
import com.linkedin.drelephant.tuning.engine.SparkExecutionEngine;
import com.linkedin.drelephant.tuning.hbt.AlgorithmManagerHBT;
import com.linkedin.drelephant.tuning.hbt.BaselineManagerHBT;
import com.linkedin.drelephant.tuning.hbt.FitnessManagerHBT;
import com.linkedin.drelephant.tuning.obt.AlgorithmManagerOBT;
import com.linkedin.drelephant.tuning.obt.BaselineManagerOBT;
import com.linkedin.drelephant.tuning.obt.FitnessManagerOBT;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Flow {
  Map<String, List<Manager>> pipelines = null;

  public Flow() {
    pipelines = new HashMap<String, List<Manager>>();
    createBaseLineManagersPipeline();
    createJobStatusManagersPipeline();
    createFitnessManagersPipeline();
    createAlgorithmManagersPipeline();
  }

  public void executeFlow() {
    for (final String pipelineType : this.pipelines.keySet()) {
      new Thread(new Runnable() {
        @Override
        public void run() {
          for (Manager manager : pipelines.get(pipelineType)) {
            manager.execute();
          }
        }
      }).start();
    }
  }

  public void createBaseLineManagersPipeline() {
    List<Manager> baselineManagers = new ArrayList<Manager>();
    baselineManagers.add(new BaselineManagerHBT());
    baselineManagers.add(new BaselineManagerOBT());
    this.pipelines.put("baselineManager", baselineManagers);
  }

  public void createJobStatusManagersPipeline() {
    List<Manager> jobStatusManagers = new ArrayList<Manager>();
    jobStatusManagers.add(new AzkabanJobStatusManager());
    //jobStatusManagers.add(new JobStatusManagerOBT());
    this.pipelines.put("jobStatusManagers", jobStatusManagers);
  }

  public void createFitnessManagersPipeline() {
    List<Manager> fitnessManagers = new ArrayList<Manager>();
    fitnessManagers.add(new FitnessManagerHBT());
    fitnessManagers.add(new FitnessManagerOBT());
    this.pipelines.put("fitnessManagers", fitnessManagers);
  }

  public void createAlgorithmManagersPipeline() {
    List<Manager> algorithmManagers = new ArrayList<Manager>();
    algorithmManagers.add(new AlgorithmManagerHBT(new com.linkedin.drelephant.tuning.engine.MRExecutionEngine()));
    algorithmManagers.add(new AlgorithmManagerHBT(new com.linkedin.drelephant.tuning.engine.SparkExecutionEngine()));
    algorithmManagers.add(new AlgorithmManagerOBT(new MRExecutionEngine()));
    algorithmManagers.add(new AlgorithmManagerOBT(new SparkExecutionEngine()));
    this.pipelines.put("algorithmManager", algorithmManagers);
  }
}
