package com.linkedin.drelephant.analysis.code.dataset;


import java.util.ArrayList;
import java.util.List;


/**
 * Code consist of seperate executable statements . For e.g in Hive Script
 * each query will be a statement.
 *
 * This will be simillar to DAO and creation of this will be dependent on the specific framework
 * optimization.
 */

public class Code {
  private List<Statement> statements = null;
  public Code(){
    statements = new ArrayList<>();
  }

  public List<Statement> getStatements() {
    return statements;
  }

  public void setStatements(List<Statement> statements) {
    this.statements = statements;
  }

  @Override
  public String toString() {
    return "Code{" + "statements=" + statements + '}';
  }
}
