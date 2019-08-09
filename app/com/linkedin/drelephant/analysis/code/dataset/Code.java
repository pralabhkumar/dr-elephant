package com.linkedin.drelephant.analysis.code.dataset;

import com.linkedin.drelephant.analysis.code.dataset.Statement;
import java.util.ArrayList;
import java.util.List;


public class Code {
  private List<com.linkedin.drelephant.analysis.code.dataset.Statement> statements = null;
  public Code(){
    statements = new ArrayList<>();
  }

  public List<com.linkedin.drelephant.analysis.code.dataset.Statement> getStatements() {
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
