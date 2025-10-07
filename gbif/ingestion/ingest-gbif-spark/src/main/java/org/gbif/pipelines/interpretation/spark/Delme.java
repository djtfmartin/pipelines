package org.gbif.pipelines.interpretation.spark;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.cache.KeyValueCache;
import org.gbif.kvs.geocode.GeocodeRequest;
import org.gbif.rest.client.geocode.GeocodeResponse;

public class Delme {
  public static void main(String[] args) {
    GeocodeRequest r1 = GeocodeRequest.create(-44.345, -122.55789, 3.0);
    GeocodeRequest r2 = GeocodeRequest.create(-44.345, -122.55789, 3.0);
    System.out.println("equality check: " + r1.equals(r2));

    DummyKeyValueStore dummy = new DummyKeyValueStore();
    KeyValueStore<GeocodeRequest, GeocodeResponse> wrapped =
        KeyValueCache.cache(dummy, 10, GeocodeRequest.class, GeocodeResponse.class, Long.MAX_VALUE);

    ExecutorService executor = Executors.newFixedThreadPool(10);

    for (int t = 0; t < 100; t++) {
      executor.submit(
          () -> {
            for (int i = 0; i < 100000; i++) {
              wrapped.get(r1);
              wrapped.get(r2);
            }
          });
    }

    // Gracefully shut down and wait for tasks to complete
    executor.shutdown();
    while (!executor.isTerminated()) {}

    System.out.println("backend calls: " + DummyKeyValueStore.counter.get());
  }

  static class DummyKeyValueStore implements KeyValueStore<GeocodeRequest, GeocodeResponse> {
    static AtomicLong counter = new AtomicLong();

    @Override
    public GeocodeResponse get(GeocodeRequest key) {
      System.out.println("backend has been called");
      counter.addAndGet(1);
      return new GeocodeResponse(
          Arrays.asList(GeocodeResponse.Location.builder().name("bingo").build()));
    }

    @Override
    public void close() {}
  }
}
