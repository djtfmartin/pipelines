package org.gbif.pipelines.interpretation.spark;

import static org.gbif.pipelines.interpretation.ConfigUtil.loadConfig;
import static org.gbif.pipelines.interpretation.spark.SparkUtil.getFileSystem;
import static org.gbif.pipelines.interpretation.spark.SparkUtil.getSparkSession;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.io.avro.*;

public class EventInheritance {
  public static void main(String[] args) throws Exception {

    PipelinesConfig config =
        loadConfig(
            "/Users/djtfmartin/dev/my-forks/pipelines/gbif/ingestion/ingest-gbif-spark/pipelines.yaml");
    String datasetId = "ecebee66-f913-4105-acb6-738430d0edc9";
    int attempt = 1;

    String outputPath = String.format("%s/%s/%d", config.getOutputPath(), datasetId, attempt);

    SparkSession spark =
        getSparkSession("local[*]", "My app", config, Interpretation::configSparkSession);
    FileSystem fileSystem = getFileSystem(spark, config);

    // load simple event
    Dataset<Event> events =
        spark.read().parquet(outputPath + "/simple-event").as(Encoders.bean(Event.class));

    events.createOrReplaceTempView("simple_event");

    Dataset<Row> joinedToParents =
        spark.sql(
            """
            SELECT
                   se.eventCore as eventCore,
                   se.taxon as taxa,
                   se.location as location,
                   se.temporal as temporal,
                   pe.eventCore as eventCoreInfo,
                   pe.taxon as taxaInfo,
                   pe.location as locationInfo,
                   pe.temporal as temporalInfo
            FROM simple_event se
            LEFT OUTER JOIN simple_event pe
            ON array_contains(se.lineage, pe.id)
        """);

    ObjectMapper mapper = new ObjectMapper();

    Dataset<EventInheritedFields> eventsWithInheritedInfo =
        joinedToParents.map(
            (MapFunction<Row, EventInheritedFields>)
                row -> {

                  // main info
                  EventCoreRecord eventCore =
                      mapper.readValue((String) row.getAs("eventCore"), EventCoreRecord.class);
                  MultiTaxonRecord taxon =
                      mapper.readValue((String) row.getAs("taxa"), MultiTaxonRecord.class);
                  LocationRecord location =
                      mapper.readValue((String) row.getAs("location"), LocationRecord.class);
                  TemporalRecord temporal =
                      mapper.readValue((String) row.getAs("temporal"), TemporalRecord.class);

                  // parent info
                  List<EventCoreRecord> eventCoreFromParents =
                      getJoinValues(row, "eventCoreInfo", mapper, EventCoreRecord.class);
                  List<LocationRecord> locationsFromParents =
                      getJoinValues(row, "locationInfo", mapper, LocationRecord.class);
                  List<TemporalRecord> temporalFromParents =
                      getJoinValues(row, "temporalInfo", mapper, TemporalRecord.class);

                  EventCoreInheritedFields eventInheritedFields =
                      inheritEventFrom(eventCore, eventCoreFromParents);
                  LocationInheritedRecord locationInheritedRecord =
                      inheritLocationFrom(location, locationsFromParents);
                  TemporalInheritedRecord temporalInheritedRecord =
                      inheritTemporalFrom(temporal, temporalFromParents);

                  return EventInheritedFields.builder()
                      .id(eventCore.getId())
                      .eventInherited(mapper.writeValueAsString(eventInheritedFields))
                      .locationInherited(mapper.writeValueAsString(locationInheritedRecord))
                      .temporalInherited(mapper.writeValueAsString(temporalInheritedRecord))
                      .build();
                },
            Encoders.bean(EventInheritedFields.class));

    eventsWithInheritedInfo
        .write()
        .mode("overwrite")
        .parquet(outputPath + "/event-inherited-fields");

    eventsWithInheritedInfo.show(false);

    fileSystem.close();
    spark.stop();
    spark.close();
    System.exit(0);
  }

  private static EventCoreInheritedFields inheritEventFrom(
      EventCoreRecord child, List<EventCoreRecord> parents) {

    var builder = EventCoreInheritedFields.builder();
    builder.id(child.getId());
    builder.parentEventID(child.getParentEventID());
    builder.locationID(child.getLocationID());
    builder.eventTypes(
        child.getParentsLineage().stream()
            .map(Parent::getEventType)
            .filter(Objects::nonNull)
            .collect(Collectors.toList()));

    for (EventCoreRecord parent : parents) {
      if (builder.locationID == null && parent.getLocationID() != null) {
        builder.locationID(parent.getLocationID());
      }
    }

    return builder.build();
  }

  private static LocationInheritedRecord inheritLocationFrom(
      LocationRecord child, List<LocationRecord> parents) {

    var builder = LocationInheritedRecord.builder();
    builder.id(child.getId());
    builder.parentId(child.getParentId());
    builder.countryCode(child.getCountryCode());
    builder.stateProvince(child.getStateProvince());
    builder.hasCoordinate(child.getHasCoordinate());
    builder.decimalLatitude(child.getDecimalLatitude());
    builder.decimalLongitude(child.getDecimalLongitude());

    for (LocationRecord parent : parents) {
      if (builder.countryCode == null && parent.getCountryCode() != null) {
        builder.countryCode(parent.getCountryCode());
      }
      if (builder.stateProvince == null && parent.getStateProvince() != null) {
        builder.stateProvince(parent.getStateProvince());
      }
      if (builder.hasCoordinate == null && parent.getHasCoordinate() != null) {
        builder.hasCoordinate(parent.getHasCoordinate());
      }
      if (builder.decimalLatitude == null && parent.getDecimalLatitude() != null) {
        builder.decimalLatitude(parent.getDecimalLatitude());
      }
      if (builder.decimalLongitude == null && parent.getDecimalLongitude() != null) {
        builder.decimalLongitude(parent.getDecimalLongitude());
      }
    }

    return builder.build();
  }

  private static TemporalInheritedRecord inheritTemporalFrom(
      TemporalRecord child, List<TemporalRecord> parents) {

    var builder = TemporalInheritedRecord.builder();
    builder.id(child.getId());
    builder.parentId(child.getParentId());
    builder.year(child.getYear());
    builder.month(child.getMonth());
    builder.day(child.getDay());
    for (TemporalRecord parent : parents) {
      if (builder.year == null && parent.getYear() != null) {
        builder.year(parent.getYear());
      }
      if (builder.month == null && parent.getMonth() != null) {
        builder.month(parent.getMonth());
      }
      if (builder.day == null && parent.getDay() != null) {
        builder.day(parent.getDay());
      }
    }

    return builder.build();
  }

  private static <T> List<T> getJoinValues(
      Row row, String field, ObjectMapper mapper, Class<T> clazz) throws JsonProcessingException {
    String fieldValue = row.getAs(field);
    List<T> results;

    if (fieldValue == null) {
      results = Collections.emptyList();
    } else {
      fieldValue = fieldValue.trim();

      // If it starts with "{", it's a single object → wrap into array
      if (fieldValue.startsWith("{")) {
        fieldValue = "[" + fieldValue + "]";
      }

      JavaType listType = mapper.getTypeFactory().constructCollectionType(List.class, clazz);

      results = mapper.readValue(fieldValue, listType);
    }
    return results;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class EventCoreInheritedFields {
    private String id;
    private String parentEventID;
    private String locationID;
    private List<String> eventTypes;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class LocationInheritedRecord {
    private String id;
    private String parentId;
    private String countryCode;
    private String stateProvince;
    private Boolean hasCoordinate;
    private Double decimalLatitude;
    private Double decimalLongitude;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class TemporalInheritedRecord {
    private String id;
    private String parentId;
    private Integer year;
    private Integer month;
    private Integer day;
  }
}
