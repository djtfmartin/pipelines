package org.gbif.pipelines.core.interpreters.model;

public interface MachineTag {

    String getNamespace();
    void setNamespace(String namespace);

    String getName();
    void setName(String name);

    String getValue();
    void setValue(String value);
}