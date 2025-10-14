package org.gbif.pipelines.interpretation.spark;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Occurrence {
  String id;
  String verbatim;
  String identifier;
  String basic;
  String location;
  String taxon;
  String temporal;
  String grscicoll;
}
