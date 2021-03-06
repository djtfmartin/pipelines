package org.gbif.pipelines.transforms.core;

import java.nio.file.Paths;
import java.time.Instant;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpreters.core.MetadataInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.parsers.config.WsConfig;
import org.gbif.pipelines.parsers.config.WsConfigFactory;
import org.gbif.pipelines.parsers.ws.client.metadata.MetadataServiceClient;
import org.gbif.pipelines.transforms.SerializableConsumer;
import org.gbif.pipelines.transforms.Transform;
import org.gbif.pipelines.transforms.common.CheckTransforms;

import org.apache.beam.sdk.values.PCollection;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.METADATA_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.METADATA;
import static org.gbif.pipelines.transforms.common.CheckTransforms.checkRecordType;

/**
 * Beam level transformations for the GBIF metadata, reads an avro, writes an avro, maps from value to keyValue and
 * transforms form {@link ExtendedRecord} to {@link MetadataRecord}.
 * <p>
 * ParDo runs sequence of interpretations for {@link MetadataRecord} using {@link ExtendedRecord}
 * as a source and {@link MetadataInterpreter} as interpretation steps
 * <p>
 * wsConfig to create a WsConfig object, please use {@link WsConfigFactory}
 */
public class MetadataTransform extends Transform<String, MetadataRecord> {

  private final Integer attempt;
  private final WsConfig wsConfig;
  private final String endpointType;
  private MetadataServiceClient client;

  private MetadataTransform(WsConfig wsConfig, String endpointType, Integer attempt) {
    super(MetadataRecord.class, METADATA, MetadataTransform.class.getName(), METADATA_RECORDS_COUNT);
    this.wsConfig = wsConfig;
    this.endpointType = endpointType;
    this.attempt = attempt;
  }

  public static MetadataTransform create() {
    return new MetadataTransform(null, null, null);
  }

  public static MetadataTransform create(WsConfig wsConfig, String endpointType, Integer attempt) {
    return new MetadataTransform(wsConfig, endpointType, attempt);
  }

  public static MetadataTransform create(String propertiesPath, String endpointType, Integer attempt, boolean skipRegistryCalls) {
    WsConfig wsConfig = null;
    if (!skipRegistryCalls) {
      wsConfig = WsConfigFactory.create(Paths.get(propertiesPath), WsConfigFactory.METADATA_PREFIX);
    }
    return new MetadataTransform(wsConfig, endpointType, attempt);
  }

  public static MetadataTransform create(Properties properties, String endpointType, Integer attempt, boolean skipRegistryCalls) {
    WsConfig wsConfig = null;
    if (!skipRegistryCalls) {
      wsConfig = WsConfigFactory.create(properties, WsConfigFactory.METADATA_PREFIX);
    }
    return new MetadataTransform(wsConfig, endpointType, attempt);
  }

  public MetadataTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  public MetadataTransform init() {
    setup();
    return this;
  }

  @Setup
  public void setup() {
    if (wsConfig != null) {
      client = MetadataServiceClient.create(wsConfig);
    }
  }

  @Override
  public Optional<MetadataRecord> convert(String source) {
    return Interpretation.from(source)
        .to(id -> MetadataRecord.newBuilder().setId(id).setCreated(Instant.now().toEpochMilli()).build())
        .via(MetadataInterpreter.interpret(client))
        .via(MetadataInterpreter.interpretCrawlId(attempt))
        .via(MetadataInterpreter.interpretEndpointType(endpointType))
        .get();
  }

  /**
   * Checks if list contains {@link MetadataTransform#getRecordType()}, else returns empty
   * {@link PCollection<MetadataRecord>}
   */
  public CheckTransforms<MetadataRecord> checkMetadata(Set<String> types) {
    return CheckTransforms.create(MetadataRecord.class, checkRecordType(types, getRecordType()));
  }

  /**
   * Checks if list contains metadata type only
   */
  public boolean metadataOnly(Set<String> types) {
    return types.size() == 1 && types.contains(getRecordType().name());
  }
}
