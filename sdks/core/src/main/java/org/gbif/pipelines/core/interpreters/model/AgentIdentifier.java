package org.gbif.pipelines.core.interpreters.model;

public interface AgentIdentifier {
  String getIdentifier();

  String getType();

  String getValue();

  AgentIdentifier setIdentifier(String identifier);

  AgentIdentifier setType(String type);

  AgentIdentifier setValue(String value);
}
