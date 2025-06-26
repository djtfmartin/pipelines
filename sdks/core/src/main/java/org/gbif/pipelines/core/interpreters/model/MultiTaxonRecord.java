package org.gbif.pipelines.core.interpreters.model;

import org.jetbrains.annotations.NotNull;

import java.util.List;

public interface MultiTaxonRecord extends Record {
    void setId(String id);
    void setTaxonRecords(List<TaxonRecord> trs);
    void setCoreId(@NotNull String s);
    void setParentId(@NotNull String s);
}
