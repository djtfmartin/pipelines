package org.gbif.pipelines.core.config.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class StandaloneConfig implements Serializable {

  RegistryConfig registry;
  MessagingConfig messaging;
  Integer numberOfShards = 12;
  Double idThresholdPercent = 50d;
  List<String> skipInstallationsList = new ArrayList<>();
}
