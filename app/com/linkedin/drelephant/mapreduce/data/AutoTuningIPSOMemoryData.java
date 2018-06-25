package com.linkedin.drelephant.mapreduce.data;

import java.util.ArrayList;
import java.util.List;


/*
Class contains memory data used by the application.
It will be different for Map and Reducer
 */
public class AutoTuningIPSOMemoryData {
  private String functionType = null;
  private List<Long> physicalMemoryBytes = new ArrayList<Long>();
  private List<Long> virtualMemoryBytes = new ArrayList<Long>();
  private List<Long> totalCommittedHeapBytes = new ArrayList<Long>();

  public AutoTuningIPSOMemoryData(String functionType) {
    this.functionType = functionType;
  }

  public void addPhysicalMemoryBytes(Long physicalMemoryByte) {
    this.physicalMemoryBytes.add(physicalMemoryByte);
  }

  public void addVirtualMemoryBytes(Long virtualMemoryByte) {
    this.virtualMemoryBytes.add(virtualMemoryByte);
  }

  public void addTotalCommittedHeapBytes(Long totalCommittedHeapByte) {
    this.totalCommittedHeapBytes.add(totalCommittedHeapByte);
  }

  public List<Long> getPhysicalMemoryBytes() {
    return physicalMemoryBytes;
  }

  public List<Long> getVirtualMemoryBytes() {
    return virtualMemoryBytes;
  }

  public List<Long> getTotalCommittedHeapBytes() {
    return totalCommittedHeapBytes;
  }

  @Override
  public String toString() {
    return "AutoTuningIPSOMemoryData{" + "functionType='" + functionType + '\'' + ", physicalMemoryBytes="
        + physicalMemoryBytes + ", virtualMemoryBytes=" + virtualMemoryBytes + ", totalCommittedHeapBytes="
        + totalCommittedHeapBytes + '}';
  }
}
