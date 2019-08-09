package com.linkedin.drelephant.analysis.code.optimizers.hive;

import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;


class QueryLevelFeature {

  public static String DELIMITER = "<FEATURE_DELIMETER>";
  static String  generateFeatures(ASTNode node, String query) {
    int numberofShuffleOperations = traverseTree(node, 0);
    int lengthOFASTTree = depthOfTree(node);
    int branchesinASTTree = totalBranches(node, 0);
    return numberofShuffleOperations + DELIMITER + lengthOFASTTree + DELIMITER + branchesinASTTree;
  }

  private static int traverseTree(org.apache.hadoop.hive.ql.lib.Node node, int operation) {
    if (node == null) {
      return operation;
    } else {
      if (node.getChildren() == null) {
        return operation;
      }
      for (Node rootNode : node.getChildren()) {
        if (rootNode.toString().equals("TOK_ORDERBY") || rootNode.toString().equals("TOK_GROUPBY")
            || rootNode.toString().equals("TOK_UNION") || rootNode.toString().equals("TOK_JOIN") || rootNode.toString()
            .equals("TOK_LEFTOUTERJOIN") || rootNode.toString().equals("TOK_RIGHTOUTERJOIN") || rootNode.toString()
            .equals("TOK_CROSSJOIN") || rootNode.toString().equals("TOK_ORDERBY") || rootNode.toString()
            .equals("TOK_DISTRIBUTEBY") || rootNode.toString().equals("TOK_CLUSTERBY") || rootNode.toString()
            .equals("TOK_FULLOUTERJOIN")) {
          operation = operation + 1;
        }
        operation = traverseTree(rootNode, operation);
      }
    }
    return operation;
  }

  private static  int totalBranches(org.apache.hadoop.hive.ql.lib.Node node, int branch) {
    if (node.getChildren() == null) {
      System.out.println(node.toString());
      return branch + 1;
    } else {
      for (Node rootNode : node.getChildren()) {
        branch = totalBranches(rootNode, branch);
      }
      return branch;
    }
  }

  private static int depthOfTree(org.apache.hadoop.hive.ql.lib.Node node) {
    if (node.getChildren() == null) {
      return 0;
    } else {
      int maxDepth = 0;
      for (Node rootNode : node.getChildren()) {
        int depth = depthOfTree(rootNode);
        if (depth > maxDepth) {
          maxDepth = depth;
        }
      }
      return maxDepth + 1;
    }
  }
}

