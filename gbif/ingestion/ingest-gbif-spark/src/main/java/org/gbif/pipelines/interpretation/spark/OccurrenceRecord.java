package org.gbif.pipelines.interpretation.spark;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class OccurrenceRecord implements java.io.Serializable {

  private MetadataRecord metadata;
  private IdentifierRecord identifier;
  private ClusteringRecord clustering;
  private BasicRecord basic;
  private TemporalRecord temporal;
  private LocationRecord location;
  private MultiTaxonRecord multiTaxon;
  private GrscicollRecord grscicoll;
  private MultimediaRecord multimedia;
  private DnaDerivedDataRecord dnaDerivedData;
  private ExtendedRecord verbatim;
}
