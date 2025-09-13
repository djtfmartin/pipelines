package org.gbif.pipelines.interpretation.spark;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.io.avro.ExtendedRecord;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class OccurrenceRecordKyro implements java.io.Serializable {

  private String id;
  private String coreId;
  private String parentId;
  private ExtendedRecord verbatim;

  // joined records
  private byte[] identifier;
  private byte[] clustering;
  private byte[] basic;
  private byte[] temporal;
  private byte[] location;
  private byte[] multiTaxon;
  private byte[] grscicoll;
  private byte[] multimedia;
  private byte[] dnaDerivedData;

}
