package org.gbif.pipelines.core.interpreters.model;

import java.util.List;

public interface MultimediaRecord extends Record {

    List<Multimedia> getMultimediaItems();
    void setMultimediaItems(List<Multimedia> multimediaItems);

    IssueRecord getIssues();
    void setIssues(IssueRecord issues);
}
