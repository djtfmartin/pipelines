package org.gbif.pipelines.kv;

import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.parsers.config.KvConfig;
import org.gbif.rest.client.geocode.GeocodeResponse;

import lombok.SneakyThrows;

public class GeocodeStoreFactory {

  private final KeyValueStore<LatLng, GeocodeResponse> store;
  private static volatile GeocodeStoreFactory instance;
  private static final Object MUTEX = new Object();

  @SneakyThrows
  private GeocodeStoreFactory(KvConfig config) {
    store = GeocodeStore.get(config);
  }

  public static GeocodeStoreFactory getInstance(KvConfig config) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new GeocodeStoreFactory(config);
        }
      }
    }
    return instance;
  }

  public KeyValueStore<LatLng, GeocodeResponse> getStore() {
    return store;
  }

}