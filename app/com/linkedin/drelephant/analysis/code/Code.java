package com.linkedin.drelephant.analysis.code;

import java.util.ArrayList;
import java.util.List;


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
