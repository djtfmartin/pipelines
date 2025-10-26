package org.gbif.pipelines.core.config.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class EsConfig implements Serializable {

  private static final long serialVersionUID = -2392370864481517738L;

  private String esHosts;
  private String esSchemaPath;
  private String esAlias;
  private String indexRefreshInterval;
  private Integer indexMaxResultWindow;
  private String unassignedNodeDelay;
  private Boolean useSlowlog;
  private String indexSearchSlowlogThresholdQueryWarn;
  private String indexSearchSlowlogThresholdQueryInfo;
  private String indexSearchSlowlogThresholdFetchWarn;
  private String indexSearchSlowlogThresholdFetchInfo;
  private String indexSearchSlowlogLevel;
  private Integer searchQueryTimeoutSec;
  private Integer searchQueryAttempts;
}
