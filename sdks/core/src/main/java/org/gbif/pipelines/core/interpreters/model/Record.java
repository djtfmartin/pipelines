package org.gbif.pipelines.core.interpreters.model;


public interface Record extends Issues {

    String getId();

    void setId(String id);

    Long getCreated();

    void setCreated(Long created);

}
