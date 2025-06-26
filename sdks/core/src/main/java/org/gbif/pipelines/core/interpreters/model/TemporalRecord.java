package org.gbif.pipelines.core.interpreters.model;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.jetbrains.annotations.NotNull;

import java.util.Set;

public interface TemporalRecord extends Record {

    // id
    String getId();
    void setId(String id);

    // coreId
    String getCoreId();
    void setCoreId(String coreId);

    // parentId
    String getParentId();
    void setParentId(String parentId);

    // created
    Long getCreated();
    void setCreated(Long created);

    // year
    Integer getYear();
    void setYear(Integer year);

    // month
    Integer getMonth();
    void setMonth(Integer month);

    // day
    Integer getDay();
    void setDay(Integer day);

    // eventDate
    EventDate getEventDate();
    void setEventDate(EventDate eventDate);

    // startDayOfYear
    Integer getStartDayOfYear();
    void setStartDayOfYear(Integer startDayOfYear);

    // endDayOfYear
    Integer getEndDayOfYear();
    void setEndDayOfYear(Integer endDayOfYear);

    // modified
    String getModified();
    void setModified(String modified);

    // dateIdentified
    String getDateIdentified();
    void setDateIdentified(String dateIdentified);

    // datePrecision
    String getDatePrecision();
    void setDatePrecision(String datePrecision);

    // issues
    IssueRecord getIssues();
    void setIssues(IssueRecord issues);
}
