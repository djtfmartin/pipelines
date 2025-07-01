package org.gbif.pipelines.core.interpreters.model;

import java.util.List;
import org.jetbrains.annotations.NotNull;

public interface MultiTaxonRecord extends Record, Issues {
  void setId(String id);

  void setTaxonRecords(List<TaxonRecord> trs);

  void setCoreId(@NotNull String s);

  void setParentId(@NotNull String s);

  List<TaxonRecord> getTaxonRecords();
}
