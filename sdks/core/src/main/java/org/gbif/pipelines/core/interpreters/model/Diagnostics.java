package org.gbif.pipelines.core.interpreters.model;

import java.util.List;

public interface Diagnostics {

  String getMatchType();

  void setMatchType(String matchType);

  Integer getConfidence();

  void setConfidence(Integer confidence);

  String getStatus();

  void setStatus(String status);

  List<String> getLineage();

  void setLineage(List<String> lineage);

  String getNote();

  void setNote(String note);
}
