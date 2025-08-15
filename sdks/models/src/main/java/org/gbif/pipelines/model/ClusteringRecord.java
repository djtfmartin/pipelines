package org.gbif.pipelines.model;

public interface ClusteringRecord extends Record, Issues {
  void setIsClustered(boolean clustered);

  Boolean getIsClustered();
}
