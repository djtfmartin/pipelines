package org.gbif.pipelines.core.config.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class MessagingConfig implements Serializable {
  String queueName;
  String host;
  Integer port;
  String virtualHost;
  String username;
  String password;
  Integer prefetchCount;
}
