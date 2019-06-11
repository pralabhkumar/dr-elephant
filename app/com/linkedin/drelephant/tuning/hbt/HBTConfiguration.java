package com.linkedin.drelephant.tuning.hbt;

import com.linkedin.drelephant.exceptions.util.EFConfiguration;


public class HBTConfiguration <T>{
  private String configurationName;
  private T value;
  private String doc;

  public String getConfigurationName() {
    return configurationName;
  }

  public HBTConfiguration<T> setConfigurationName(String configurationName) {
    this.configurationName = configurationName;
    return this;
  }

  public T getValue() {
    return value;
  }

  public HBTConfiguration<T> setValue(T value) {
    this.value = value;
    return this;
  }

  public String getDoc() {
    return doc;
  }

  public HBTConfiguration<T> setDoc(String doc) {
    this.doc = doc;
    return this;
  }

  @Override
  public String toString() {
    return "HBTConfiguration{" + "configurationName='" + configurationName + '\'' + ", value=" + value + ", doc='" + doc
        + '\'' + '}';
  }
}
