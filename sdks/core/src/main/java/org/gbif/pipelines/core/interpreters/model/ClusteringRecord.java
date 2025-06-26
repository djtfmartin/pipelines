package org.gbif.pipelines.core.interpreters.model;

public interface ClusteringRecord extends Record {
    void setIsClustered(boolean clustered);

    Boolean getIsClustered();
}
