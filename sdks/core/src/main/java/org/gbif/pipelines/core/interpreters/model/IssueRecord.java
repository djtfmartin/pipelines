package org.gbif.pipelines.core.interpreters.model;

import java.util.List;

public interface IssueRecord extends Record {

  List<String> getIssueList();

  void setIssueList(List<String> issues);
}
