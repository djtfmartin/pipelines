package org.gbif.pipelines.json;

import java.util.List;
import lombok.Builder;
import lombok.Data;

/** Models a set of GADM features at different levels. */
@Data
@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
public class GadmFeatures {
  private String level0Gid;
  private String level1Gid;
  private String level2Gid;
  private String level3Gid;
  private String level0Name;
  private String level1Name;
  private String level2Name;
  private String level3Name;
  private List<String> gids;
}
