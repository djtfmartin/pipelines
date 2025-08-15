package org.gbif.pipelines.model;

public interface TemporalRecord extends Record, Issues {

  // parentId
  String getParentId();

  void setParentId(String parentId);

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
}
