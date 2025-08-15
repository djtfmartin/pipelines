package org.gbif.pipelines.model;

public interface AgentIdentifier {
  String getIdentifier();

  String getType();

  String getValue();

  void setIdentifier(String identifier);

  void setType(String type);

  void setValue(String value);
}
