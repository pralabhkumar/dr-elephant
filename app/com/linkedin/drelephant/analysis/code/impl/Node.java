package com.linkedin.drelephant.analysis.code.impl;

import com.linkedin.drelephant.analysis.code.Statement;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.json.JSONException;
import org.apache.hadoop.hive.ql.parse.ASTNode;


public class Node {
  private String outputTable = null;
  private String query = null;
  private List<Node> parents = null;
  private List<Node> children = null;
  private Statement statement = null;
  private int nodeWeight = 0;
  private int nodeComplexity = 0;

  public int getNodeComplexity() {
    return nodeComplexity;
  }

  public void setNodeComplexity(int nodeComplexity) {
    this.nodeComplexity = nodeComplexity;
  }

  public void generateNodeComplexity() throws ParseException, IOException, JSONException {
    String queryForHiveParsing = statement.getFrameWorkParsableQuery();
    String queryLevelFeatures =
        QueryLevelFeature.generateFeatures((ASTNode) statement.getBaseTree(), queryForHiveParsing);
    int numberOfShuffleOperation = Integer.parseInt(queryLevelFeatures.split(QueryLevelFeature.DELIMITER)[0]);
    //int queryComplexity = Constant.getMLPriority(queryLevelFeatures.replaceAll("<FEATURE_DELIMETER>", "@"));
    /*int queryComplexity = 1;
    if (queryComplexity == 0) {
      this.nodeComplexity = 3;
    } else if (queryComplexity == 1) {
      this.nodeComplexity = 2;
    } else {
      this.nodeComplexity = 1;
    }*/
    if (numberOfShuffleOperation == 1) {
      this.nodeComplexity = 1;
    } else if (numberOfShuffleOperation == 2) {
      this.nodeComplexity = 2;
    } else if (numberOfShuffleOperation > 2) {
      this.nodeComplexity = 3;
    }
  }

  public Node(String outputTable, Statement statement) {
    this.outputTable = outputTable;
    this.statement = statement;
    parents = new ArrayList<Node>();
    children = new ArrayList<Node>();
    if (statement != null) {
      query = statement.getFrameWorkParsableQuery();
    }
  }

  public String getOutputTable() {
    return this.outputTable;
  }

  public void addChild(Node node) {
    this.children.add(node);
  }

  public List<Node> getChildren() {
    return this.children;
  }

  public void addParent(Node node) {
    this.parents.add(node);
  }

  public List<Node> getParent() {
    return this.parents;
  }

  public Integer getNodeWeight() {
    return nodeWeight;
  }

  public void setNodeWeight(Integer nodeWeight) {
    this.nodeWeight = nodeWeight;
  }

  public String getQuery() {
    return query;
  }

  public Statement getStatement() {
    return statement;
  }

  public int getINDegree() {
    return this.getParent().size();
  }

  public int getOUTDegree() {
    return this.getChildren().size();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Node node = (Node) o;
    return Objects.equals(outputTable, node.outputTable);
  }

  @Override
  public int hashCode() {

    return Objects.hash(outputTable);
  }

  @Override
  public String toString() {
    if (this.getStatement() == null) {
      return "Node{" + "outputTable='" + outputTable + '\'' + ", parents=" + parents.size() + ", children="
          + children.size() + ", nodeWeight=" + nodeWeight + ", nodeComplexity=" + nodeComplexity + " Checkpoinint="
          + "false" + '}';
    } else {
      return "Node{" + "outputTable='" + outputTable + '\'' + ", parents=" + parents.size() + ", children="
          + children.size() + ", nodeWeight=" + nodeWeight + ", nodeComplexity=" + nodeComplexity + " Checkpoinint="
          + this.getStatement().getIsCheckpoint() + '}';
    }
  }
}