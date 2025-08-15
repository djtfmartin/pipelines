package org.gbif.pipelines.json;

import java.util.List;
import lombok.Builder;
import lombok.Data;

/** Represents authorship data for a scientific name. */
@Data
@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
public class Authorship {
  private List<String> authors;
  private List<String> exAuthors;
  private Boolean empty;
  private String year;
}
