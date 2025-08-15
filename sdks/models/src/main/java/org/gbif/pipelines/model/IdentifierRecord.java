package org.gbif.pipelines.model;

public interface IdentifierRecord extends Record, Issues {
  void setUniqueKey(String occurrenceId);

  void setInternalId(String sha1);

  String getInternalId();

  void setAssociatedKey(String tr);

  void addIssue(String gbifIdAbsent);

  String getUniqueKey();

  String getAssociatedKey();

  IssueRecord getIssues();

  Long getFirstLoaded();
}
