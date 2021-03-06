package org.gbif.pipelines.parsers.config;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

/** Models the ws configuration. If you want to create an instance, use {@link WsConfigFactory} */
@Getter
@Data
@AllArgsConstructor
public final class WsConfig implements Serializable {

  private static final long serialVersionUID = -9019714539955270670L;
  // ws path
  private final String basePath;
  // timeout in seconds
  private final long timeout;
  // cache size in bytes
  private final long cacheSize;
  //Retry configuration
  private final PipelinesRetryConfig pipelinesRetryConfig;

  public WsConfig(String basePath, String timeout, String cacheSizeMb, PipelinesRetryConfig pipelinesRetryConfig) {
    this.basePath = basePath;
    this.timeout = Long.parseLong(timeout);
    this.cacheSize = Long.parseLong(cacheSizeMb) * 1024L * 1024L;
    this.pipelinesRetryConfig = pipelinesRetryConfig;
  }
}

