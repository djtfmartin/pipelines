package org.gbif.pipelines.interpretation.spark;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OccurrenceSimple {
  String id;
  String basic;
  String verbatim;
  String location;
  String taxon;
  String temporal;
  String grscicoll;
}
