package org.gbif.pipelines.core.interpreters.model;

import org.gbif.api.vocabulary.InterpretationRemark;
import org.gbif.api.vocabulary.OccurrenceIssue;

import java.util.Set;

public interface Issues {
    IssueRecord getIssues();

    void setIssues(IssueRecord issues);

    void addIssue(Set<String> issues);

    void addIssues(Set<InterpretationRemark> issues);

    void addIssueSet(Set<OccurrenceIssue> issues);

    void addIssue(InterpretationRemark issue);

    void addIssue(String issue);
}
