package org.gbif.pipelines.core.interpreters.model;

import java.util.List;

public interface ImageRecord extends Record {

    List<Image> getImageItems();
    void setImageItems(List<Image> imageItems);

    IssueRecord getIssues();
    void setIssues(IssueRecord issues);
}
