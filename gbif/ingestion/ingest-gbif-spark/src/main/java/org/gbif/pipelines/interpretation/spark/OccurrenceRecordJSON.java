package org.gbif.pipelines.interpretation.spark;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class OccurrenceRecordJSON implements java.io.Serializable {

  private String id;
  private String coreId;
  private String parentId;

  // joined records
  private String metadata;
  private String identifier;
  private String clustering;
  private String basic;
  private String temporal;
  private String location;
  private String multiTaxon;
  private String grscicoll;
  private String multimedia;
  private String dnaDerivedData;
  private String verbatim;
}
