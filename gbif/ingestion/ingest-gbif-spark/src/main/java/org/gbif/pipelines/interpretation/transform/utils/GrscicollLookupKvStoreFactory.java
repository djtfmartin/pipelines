package org.gbif.pipelines.interpretation.transform.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.grscicoll.GrscicollLookupKVStoreFactory;
import org.gbif.kvs.grscicoll.GrscicollLookupRequest;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.grscicoll.GrscicollLookupResponse;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class GrscicollLookupKvStoreFactory {

  private static KeyValueStore<GrscicollLookupRequest, GrscicollLookupResponse> kvStore;
  private static final Object LOCK = new Object();

  public static KeyValueStore<GrscicollLookupRequest, GrscicollLookupResponse> getKvStore(
      String apiUrl) {
    if (kvStore == null) {
      synchronized (LOCK) {
        if (kvStore == null) {
          ClientConfiguration clientConfiguration =
              ClientConfiguration.builder()
                  .withBaseApiUrl(apiUrl)
                  .withFileCacheMaxSizeMb(100L) // 100MB
                  .build();
          kvStore = GrscicollLookupKVStoreFactory.simpleGrscicollLookupKVStore(clientConfiguration);
        }
      }
    }
    return kvStore;
  }
}
