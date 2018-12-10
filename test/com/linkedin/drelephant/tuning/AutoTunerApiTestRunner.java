package com.linkedin.drelephant.tuning;
import static org.junit.Assert.*;
import static play.test.Helpers.*;
import static common.DBTestUtil.*;
import static common.DBTestUtil.*;

public class AutoTunerApiTestRunner implements Runnable {
  private  AutoTuningAPIHelper _autoTuningAPIHelper;
  private TuningInput _tuningInput;
  public AutoTunerApiTestRunner(AutoTuningAPIHelper autoTuningAPIHelper, TuningInput tuningInput){
    this._autoTuningAPIHelper=autoTuningAPIHelper;
    this._tuningInput=tuningInput;
  }
  @Override
  public void run() {
     assertTrue(" Flow Execution ", _autoTuningAPIHelper.getFlowExecution(this._tuningInput) !=null);
  }
}
