package com.linkedin.drelephant.tuning;

import java.util.HashMap;
import java.util.Map;


public class Constant {
  public enum TuningType {HBT,OBT}
  public enum AlgotihmType{PSO,PSO_IPSO,HBT}
  public enum ExecutionEngineTypes{MR,SPARK}
  public enum TypeofManagers{AbstractBaselineManager,AbstractFitnessManager,AbstractJobStatusManager,AbstractParameterGenerateManager}

}
