package org.gbif.pipelines.parsers.config;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

@Data
@AllArgsConstructor(staticName = "create")
@Getter
public final class KvConfig implements Serializable {

  private static final long serialVersionUID = -9019714539959567270L;
  // ws path
  private final String basePath;
  // timeout in seconds
  private final long timeout;
  // cache size in mb
  private final long cacheSizeMb;
  //
  private final String tableName;

  private final String zookeeperUrl;

  private final int numOfKeyBuckets;

  private final boolean restOnly;
}

