package org.gbif.pipelines.interpretation.spark;

import java.io.IOException;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.conf.CachedHBaseKVStoreConfiguration;
import org.gbif.kvs.geocode.GeocodeKVStoreFactory;
import org.gbif.kvs.geocode.GeocodeRequest;
import org.gbif.kvs.hbase.HBaseKVStoreConfiguration;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.geocode.GeocodeResponse;

// A simple geo lookup using HBase
public class GeoLookup {
  private static KeyValueStore<GeocodeRequest, GeocodeResponse> CACHE;

  static {
    ClientConfiguration clientConfig =
        ClientConfiguration.builder().withBaseApiUrl("https://api.gbif-test.org/v1/").build();

    CachedHBaseKVStoreConfiguration.Builder configBuilder =
        CachedHBaseKVStoreConfiguration.builder()
            .withValueColumnQualifier("j") // stores JSON data
            .withHBaseKVStoreConfiguration(
                HBaseKVStoreConfiguration.builder()
                    .withTableName("tim_geo_kv")
                    .withColumnFamily("v") // Column in which qualifiers are stored
                    .withNumOfKeyBuckets(10)
                    .withHBaseZk(
                        "c5n7.gbif-test.org:31706,c5n9.gbif-test.org:31706,c6n8.gbif-test.org:31706")
                    .withHBaseZnode("/znode-93f9cdb5-d146-46da-9f80-e8546468b0fe/hbase")
                    .build())
            .withCacheCapacity(100L)
            .withCacheExpiryTimeInSeconds(120L);

    try {
      CACHE = GeocodeKVStoreFactory.simpleGeocodeKVStore(configBuilder.build(), clientConfig);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static String country(GeocodeRequest request) {
    GeocodeResponse response = CACHE.get(request);
    // return the first - any will do
    return response.getLocations().isEmpty()
        ? "UNKNOWN"
        : response.getLocations().get(0).getIsoCountryCode2Digit();
  }
}
