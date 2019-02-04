package com.linkedin.drelephant.exceptions.util;

public class EFConfiguration<T> {

  private String configurationName;
  private T value;
  private String doc;

  public String getConfigurationName() {
    return configurationName;
  }

  public EFConfiguration<T> setConfigurationName(String configurationName) {
    this.configurationName = configurationName;
    return this;
  }

  public T getValue() {
    return value;
  }

  public EFConfiguration<T> setValue(T value) {
    this.value = value;
    return this;
  }

  public String getDoc() {
    return doc;
  }

  public EFConfiguration<T> setDoc(String doc) {
    this.doc = doc;
    return this;
  }

  @Override
  public String toString() {
    return "EFConfiguration{" + "configurationName='" + configurationName + '\'' + ", value=" + value + ", doc='" + doc
        + '\'' + '}';
  }
}
