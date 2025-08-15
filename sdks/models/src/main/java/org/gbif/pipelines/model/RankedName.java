package org.gbif.pipelines.model;

public interface RankedName {

  String getKey();

  void setKey(String key);

  String getName();

  void setName(String name);

  String getRank();

  void setRank(String rank);
}
