package com.linkedin.drelephant.tuning;

public interface Manager {
  /*
   Use to execute the logic of all the managers .
   */
  boolean execute();
  /*
     Manager Name
   */
  String getManagerName();

}
