package org.gbif.pipelines.model;

public interface MultiTaxonRecord extends Record, Issues {
  void setId(String id);

  //  void setTaxonRecords(List<T> trs);

  void setCoreId(String s);

  void setParentId(String s);

  //  List<T> getTaxonRecords();
}
