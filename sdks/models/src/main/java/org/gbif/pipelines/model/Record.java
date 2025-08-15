package org.gbif.pipelines.model;

public interface Record {

  String getId();

  void setId(String id);

  String getCoreId();

  void setCoreId(String id);

  Long getCreated();

  void setCreated(Long created);
}
