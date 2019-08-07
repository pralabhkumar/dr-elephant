package com.linkedin.drelephant.analysis.code;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.antlr.runtime.tree.BaseTree;

public class Statement {
  private int sequenceNumber;
  private String type;
  private String optimizeStatement;
  private String comment;
  private List<String> inputSources;
  private List<String> outputSinks;
  private Map<String, String> metaData;
  private String originalStatement;
  private String frameWorkParsableQuery;
  private BaseTree parseAST;
  private Boolean isCheckpoint = false;

  public Statement(int sequenceNumber, String originalStatement) {
    this.sequenceNumber = sequenceNumber;
    this.originalStatement = originalStatement;
    this.metaData = new HashMap<String, String>();
    this.inputSources = new ArrayList<String>();
    this.outputSinks = new ArrayList<String>();
  }

  public int getSequenceNumber() {
    return sequenceNumber;
  }

  public String getOriginalStatement() {
    return originalStatement;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getOptimizeStatement() {
    return optimizeStatement;
  }

  public void setOptimizeStatement(String optimizeStatement) {
    this.optimizeStatement = optimizeStatement;
  }


  public BaseTree getBaseTree() {
    return parseAST;
  }

  public void setBaseTree(BaseTree parseAST) {
    this.parseAST = parseAST;
  }


  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  public List<String> getInputSources() {
    return inputSources;
  }

  public void setInputSources(List<String> inputSources) {
    this.inputSources = inputSources;
  }

  public List<String> getOutputSinks() {
    return outputSinks;
  }

  public void setOutputSinks(List<String> outputSinks) {
    this.outputSinks = outputSinks;
  }

  public Map<String, String> getMetaData() {
    return metaData;
  }

  public void setMetaData(Map<String, String> metaData) {
    this.metaData = metaData;
  }


  public String getFrameWorkParsableQuery() {
    return frameWorkParsableQuery;
  }

  public void setFrameWorkParsableQuery(String frameWorkParsableQuery) {
    this.frameWorkParsableQuery = frameWorkParsableQuery;
  }

  public Boolean getIsCheckpoint() {
    return isCheckpoint;
  }

  public void setIsCheckpoint(Boolean isCheckpoint) {
    this.isCheckpoint = isCheckpoint;
  }

  @Override
  public String toString() {
    return "Statement{" + "sequenceNumber=" + sequenceNumber + ", type='" + type + '\'' + ", optimizeStatement='"
        + optimizeStatement + '\'' + ", comment='" + comment + '\'' + ", inputSources=" + inputSources
        + ", outputSinks=" + outputSinks + ", metaData=" + metaData + ", originalStatement='" + originalStatement + '\''
        + ", frameWorkParsableQuery='" + frameWorkParsableQuery + '\'' + '}';
  }
}
