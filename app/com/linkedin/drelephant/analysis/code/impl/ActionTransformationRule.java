package com.linkedin.drelephant.analysis.code.impl;

import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.analysis.code.CodeOptimizationRule;
import com.linkedin.drelephant.analysis.code.Script;
import com.linkedin.drelephant.analysis.code.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.hadoop.hive.ql.parse.ASTNode;


public class ActionTransformationRule implements CodeOptimizationRule {
  private static final Logger logger = Logger.getLogger(ActionTransformationRule.class);
  private String ruleName = null;
  private Script script = null;
  private List<String> tablesConvertedToViews = null;

  public ActionTransformationRule(String ruleName) {
    this.ruleName = ruleName;
    this.tablesConvertedToViews = new ArrayList<String>();
  }

  @Override
  public void processRule(Script script) {
    try {
      this.script = script;
      DAG dagGeneration = new DAG();
      dagGeneration.generateDAG(script.getCode().getStatements());
      dagGeneration.dumpGraph();
      Set<String> tempOutputTables = getOutputIntersectInputTable(script.getCode().getStatements());
      for (String tempTable : tempOutputTables) {
        logger.info(" Following are the temperoray tables " + tempTable);
      }
      for (Statement statement : script.getCode().getStatements()) {
        processStatementforRule(statement, tempOutputTables);
      }
      if (tablesConvertedToViews.size() > 0) {
        for (String temperorayTables : tablesConvertedToViews) {
          this.script.getOptimizationComment().append(temperorayTables + "\n");
        }
      }
    } catch (Exception e) {
      logger.error(" Unable to process for rule  ", e);
    }
  }

  private void processStatementforRule(Statement statement, Set<String> tempOutputTables) {
    List<String> outputTables = statement.getOutputSinks();
    String astTree = ((ASTNode) statement.getBaseTree()).dump();

    if (isStatementQualifyRule(outputTables, tempOutputTables, astTree)) {
      changeStatement(statement);
    }
  }

  private void changeStatement(Statement statement) {
    tablesConvertedToViews.addAll(statement.getOutputSinks());
    // this.script.getOptimizationComment().append("Change the following temperoray  table to view in Spark 'CREATE OR REPLACE VIEW'  "+statement.getOutputSinks()+"\n");
    //this.script.setOptimizationComment(this.script.getOptimizationComment()+"\t"+statement.getOriginalStatement());
  }

  private boolean isStatementQualifyRule(List<String> outputTables, Set<String> tempOutputTables, String astTree) {
    if (outputTables != null && !Collections.disjoint(tempOutputTables, outputTables) && astTree.contains(
        "TOK_SELEXPR")) {
      return true;
    } else {
      return false;
    }
  }

  private Set<String> getOutputIntersectInputTable(List<Statement> statements) {
    Set<String> globalOutputTables = new HashSet<String>();
    Set<String> globalInputTables = new HashSet<String>();
    for (Statement statement : statements) {
      if (statement.getOutputSinks() != null && !statement.getIsCheckpoint()) {
        globalOutputTables.addAll(statement.getOutputSinks());
      }
      if (statement.getInputSources() != null) {
        globalInputTables.addAll(statement.getInputSources());
      }
    }
    globalOutputTables.retainAll(globalInputTables);
    return globalOutputTables;
  }

  @Override
  public String getRuleName() {
    return this.ruleName;
  }

  @Override
  public String getSeverity() {
    logger.info(" Size of the tables to create views "+tablesConvertedToViews.size());
    if (tablesConvertedToViews.size() == 1) {
      logger.info(" Severity is  "+Severity.LOW);
      return Severity.LOW.getText();
    } else if (tablesConvertedToViews.size() == 2) {
      logger.info(" Severity is  "+Severity.MODERATE);
      return Severity.MODERATE.getText();
    } else if (tablesConvertedToViews.size() == 3) {
      logger.info(" Severity is  "+Severity.SEVERE);
      return Severity.SEVERE.getText();
    } else if (tablesConvertedToViews.size() >3){
      logger.info(" Severity is  "+Severity.CRITICAL);
      return Severity.CRITICAL.getText();
    }
    logger.info("MAtches noting  Severity is  "+Severity.LOW);
    return Severity.LOW.getText();
  }
}
