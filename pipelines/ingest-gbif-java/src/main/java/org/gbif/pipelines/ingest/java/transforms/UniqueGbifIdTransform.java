package org.gbif.pipelines.ingest.java.transforms;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import org.gbif.pipelines.core.utils.HashUtils;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transforms.core.BasicTransform;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;

/**
 * Splits collection into two:
 * 1 - normal collection with regular GBIF ids
 * 2 - contains invalid records with GBIF ids, as duplicates or missed GBIF ids
 */
@Getter
@Builder
public class UniqueGbifIdTransform {

  private final Map<String, BasicRecord> brMap = new ConcurrentHashMap<>();
  private final Map<String, BasicRecord> brInvalidMap = new ConcurrentHashMap<>();

  @NonNull
  private BasicTransform basicTransform;

  @NonNull
  private Map<String, ExtendedRecord> erMap;

  @Builder.Default
  private ExecutorService executor = Executors.newWorkStealingPool();

  @Builder.Default
  private boolean useSyncMode = true;

  @Builder.Default
  private boolean skipTransform = false;

  public UniqueGbifIdTransform run() {
    return useSyncMode ?  runSync() : runAsync();
  }

  @SneakyThrows
  private UniqueGbifIdTransform runAsync() {
    // Filter GBIF id duplicates
    Consumer<ExtendedRecord> interpretBrFn = filterByGbifId(brMap, brInvalidMap);

    // Run async
    CompletableFuture[] brFutures = erMap.values()
        .stream()
        .map(v -> CompletableFuture.runAsync(() -> interpretBrFn.accept(v), executor))
        .toArray(CompletableFuture[]::new);
    CompletableFuture.allOf(brFutures).get();

    return this;
  }

  @SneakyThrows
  private UniqueGbifIdTransform runSync() {
    erMap.values().forEach(filterByGbifId(brMap, brInvalidMap));

    return this;
  }

  // Filter GBIF id duplicates
  private Consumer<ExtendedRecord> filterByGbifId(Map<String, BasicRecord> map, Map<String, BasicRecord> invalidMap) {
    return er ->
        basicTransform.processElement(er)
            .ifPresent(br -> {
              if (skipTransform) {
                map.put(br.getId(), br);
              } else if (br.getGbifId() != null) {
                BasicRecord record = map.get(br.getGbifId().toString());
                if (record != null) {
                  int compare = HashUtils.getSha1(br.getId()).compareTo(HashUtils.getSha1(record.getId()));
                  if (compare < 0) {
                    map.put(br.getGbifId().toString(), br);
                    invalidMap.put(record.getId(), record);
                  } else {
                    invalidMap.put(br.getId(), br);
                  }
                } else {
                  map.put(br.getGbifId().toString(), br);
                }
              } else {
                invalidMap.put(br.getId(), br);
              }
            });
  }

}
