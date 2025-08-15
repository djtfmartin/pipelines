package org.gbif.pipelines.model;

import java.util.Set;
import org.gbif.api.vocabulary.InterpretationRemark;
import org.gbif.api.vocabulary.OccurrenceIssue;

public interface Issues {
  IssueRecord getIssues();

  void setIssues(IssueRecord issues);

  void addIssue(Set<String> issues);

  void addIssues(Set<InterpretationRemark> issues);

  void addIssueSet(Set<OccurrenceIssue> issues);

  void addIssue(InterpretationRemark issue);

  void addIssue(String issue);
}
