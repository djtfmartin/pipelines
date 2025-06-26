package org.gbif.pipelines.core.interpreters.model;

import java.util.List;

public interface AmplificationRecord extends Record {

    List<Amplification> getAmplificationItems();
    void setAmplificationItems(List<Amplification> amplificationItems);

    IssueRecord getIssues();
    void setIssues(IssueRecord issues);
}
