package org.gbif.pipelines.model;

public interface EventDate {
  String getGte();

  void setGte(String gte);

  String getLte();

  void setLte(String lte);

  String getInterval();

  void setInterval(String interval);
}
