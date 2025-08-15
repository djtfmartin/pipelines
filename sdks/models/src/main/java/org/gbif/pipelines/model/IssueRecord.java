package org.gbif.pipelines.model;

import java.util.List;

public interface IssueRecord extends Record {

  List<String> getIssueList();

  void setIssueList(List<String> issues);
}
