package com.linkedin.drelephant.analysis.code.impl;

import com.linkedin.drelephant.analysis.code.Statement;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.apache.hadoop.hive.ql.parse.ASTNode;

public class DAG {
  private static final Logger logger = Logger.getLogger(DAG.class);
  private Map<String, Node> outputTableMap = null;
  private Node rootNode = null;

  public DAG() {
    outputTableMap = new HashMap<String, Node>();
  }

  public Node getNodeGivenOutputTable(String outputTable) {
    return outputTableMap.get(outputTable);
  }

  public void generateDAG(List<Statement> statemens) throws ParseException, JSONException, IOException {
    for (Statement statement : statemens) {
      if (((ASTNode) statement.getBaseTree()).dump().contains("TOK_SELEXPR")) {
        List<String> inputTables = statement.getInputSources();
        List<String> outputTables = statement.getOutputSinks();
        for (String outputTable : outputTables) {
          Node node = new Node(outputTable, statement);
          addParents(node, inputTables);
          node.generateNodeComplexity();
          outputTableMap.put(outputTable, node);
        }
      }
    }
    generateNodeWeights();
    generateCheckPoint();
  }

  private void addParents(Node node, List<String> inputTables) {
    for (String inputTable : inputTables) {
      Node parentNode = outputTableMap.get(inputTable);
      if (parentNode == null) {
        parentNode = new Node(inputTable, null);
        outputTableMap.put(inputTable, parentNode);
      }
      parentNode.addChild(node);
      node.addParent(parentNode);
    }
  }

  public int getTotalNodesinGraph() {
    return outputTableMap.size();
  }

  private void generateNodeWeights() {
    List<Node> roots = getRootNodes();
    for (Node root : roots) {
      addWeightstoNode(root, 0);
    }
  }

  private void addWeightstoNode(Node node, int weightSoFar) {
    if (node == null) {
      return;
    } else {
      int nodeComplexity = node.getNodeComplexity();
      if (nodeComplexity + weightSoFar >= node.getNodeWeight()) {
        node.setNodeWeight(nodeComplexity + weightSoFar);
      }
      for (Node child : node.getChildren()) {
        addWeightstoNode(child, node.getNodeWeight());
      }
    }
  }

  public void dumpGraph() {
    List<Node> roots = getRootNodes();
    logger.info(" Number of Root Nodes " + roots.size());
    logger.info(" Root Nodes " + roots);
    logger.info(" Total Nodes in graph " + getTotalNodesinGraph());
    for (Node root : roots) {
      traverseGraph(root, 0, 0);
    }
  }

  public List<Node> getRootNodes() {
    List<Node> roots = new ArrayList<Node>();
    for (String table : outputTableMap.keySet()) {
      Node node = outputTableMap.get(table);
      if (node.getQuery() == null) {
        roots.add(node);
      }
    }
    return roots;
  }

  public void traverseGraph(Node node, int space, int level) {
    if (node == null) {
      return;
    } else {
      StringBuffer spaces = new StringBuffer();
      for (int i = 0; i < space; i++) {
        spaces.append("\t");
      }
      logger.info("L" + level + "\t" + spaces + " Node " + node);
      space++;
      level++;

      for (Node child : node.getChildren()) {

        traverseGraph(child, space, level);
      }
      space--;
      level--;
    }
  }

  public void generateCheckPoint() {
    List<Node> rootNodes = getRootNodes();
    for (Node root : rootNodes) {
      addCheckPoint(root, 0);
    }
  }

  private void addCheckPoint(Node node, int previousCheckpointWeight) {
    if (node == null) {
      return;
    } else {
      int weightNode = node.getNodeWeight();
      if (weightNode - previousCheckpointWeight >= 6) {
        if (!node.getStatement().getIsCheckpoint()) {
          logger.info("Checkpointing " + node.getStatement().getOutputSinks());
        }
        node.getStatement().setIsCheckpoint(true);
        logger.info("Previous Checkpoint " + previousCheckpointWeight + "," + weightNode);
        previousCheckpointWeight = weightNode;
      }
      for (Node child : node.getChildren()) {
        addCheckPoint(child, previousCheckpointWeight);
      }
    }
  }
}
