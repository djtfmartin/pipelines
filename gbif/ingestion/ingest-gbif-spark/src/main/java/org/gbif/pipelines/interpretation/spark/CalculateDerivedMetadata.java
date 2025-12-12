package org.gbif.pipelines.interpretation.spark;

import static org.gbif.pipelines.interpretation.ConfigUtil.loadConfig;
import static org.gbif.pipelines.interpretation.spark.Directories.SIMPLE_EVENT;
import static org.gbif.pipelines.interpretation.spark.Directories.SIMPLE_OCCURRENCE;
import static org.gbif.pipelines.interpretation.spark.SparkUtil.getFileSystem;
import static org.gbif.pipelines.interpretation.spark.SparkUtil.getSparkSession;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.*;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.parsers.location.parser.ConvexHullParser;
import org.gbif.pipelines.core.parsers.temporal.StringToDateFunctions;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.json.DerivedMetadataRecord;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.io.WKTWriter;
import scala.Tuple2;
import scala.Tuple3;

@Slf4j
public class CalculateDerivedMetadata implements Serializable {

  static final ObjectMapper MAPPER = new ObjectMapper();

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

    runCalculateDerivedMetadata(spark, fileSystem, outputPath);

    fileSystem.close();
    spark.stop();
    spark.close();
    System.exit(0);
  }

  private static Dataset<DerivedMetadataRecord> runCalculateDerivedMetadata(
      SparkSession spark, FileSystem fileSystem, String outputPath) throws IOException {

    // loads events
    Dataset<Event> events =
        spark.read().parquet(outputPath + "/" + SIMPLE_EVENT).as(Encoders.bean(Event.class));

    // join to child events to get all coordinates associated with parent event
    Dataset<EventCoordinate> eventIdToCoordinates = gatherCoordinatesFromChildEvents(spark, events);
    log.info("eventIdToCoordinates {}", eventIdToCoordinates.count());

    // join child events ?
    Dataset<EventCoordinate> coreIdEventCoordinates = getEventCoordinates(events);
    log.info("coreIdEventCoordinates {}", coreIdEventCoordinates.count());

    // does this dataset have occurrences ?
    Dataset<Occurrence> occurrence = loadOccurrences(spark, fileSystem, outputPath);
    log.info("occurrences {}", occurrence.count());

    // get unique occurrence locations - coredId -> "lat||long"
    Dataset<EventCoordinate> coreIdOccurrenceCoordinates = getCoreIdCoordinates(occurrence);
    log.info("coreIdOccurrenceCoordinates {}", coreIdOccurrenceCoordinates.count());

    Dataset<Tuple2<String, EventDate>> eventIdToEventDate =
        gatherEventDatesFromChildEvents(spark, events);
    log.info("eventIdToCoordinates {}", eventIdToCoordinates.count());

    // get unique occurrence temporal - coreId -> eventDate
    Dataset<Tuple2<String, EventDate>> coredIdOccurrenceEventDates =
        getCoreIdEventDates(occurrence);
    log.info("coredIdOccurrenceEventDates {}", coredIdOccurrenceEventDates.count());

    // Calculate Convex Hull
    KeyValueGroupedDataset<String, EventCoordinate> groupedById =
        coreIdOccurrenceCoordinates
            .union(eventIdToCoordinates)
            .union(coreIdEventCoordinates)
            .distinct()
            .groupByKey(
                (MapFunction<EventCoordinate, String>) EventCoordinate::getEventId,
                Encoders.STRING());

    Dataset<Tuple2<String, String>> eventIdConvexHull =
        groupedById.mapGroups(
            (MapGroupsFunction<String, EventCoordinate, Tuple2<String, String>>)
                (eventId, coordsIter) -> {
                  List<Coordinate> coordList = new ArrayList<>();
                  coordsIter.forEachRemaining(
                      ec -> coordList.add(new Coordinate(ec.getLongitude(), ec.getLatitude())));
                  String convexHullWkt =
                      new WKTWriter()
                          .write(ConvexHullParser.fromCoordinates(coordList).getConvexHull());
                  return new Tuple2<>(eventId, convexHullWkt);
                },
            Encoders.tuple(Encoders.STRING(), Encoders.STRING()));

    // convex hulls
    eventIdConvexHull.write().mode(SaveMode.Overwrite).parquet(outputPath + "/derived/convex_hull");

    KeyValueGroupedDataset<String, Tuple2<String, EventDate>> groupedByIdDates =
        coredIdOccurrenceEventDates
            .union(eventIdToEventDate)
            .distinct()
            .groupByKey(
                (MapFunction<Tuple2<String, EventDate>, String>) Tuple2::_1, Encoders.STRING());

    Dataset<Tuple2<String, EventDate>> temporalCoverages =
        groupedByIdDates.mapGroups(
            (MapGroupsFunction<String, Tuple2<String, EventDate>, Tuple2<String, EventDate>>)
                (eventId, eventIter) -> {
                  TemporalAccum accum = new TemporalAccum();
                  eventIter.forEachRemaining(
                      eventDate -> {
                        accum.setMinDate(eventDate._2().getGte());
                        accum.setMaxDate(eventDate._2().getLte());
                      });
                  return new Tuple2(eventId, accum.toEventDate().get());
                },
            Encoders.tuple(Encoders.STRING(), Encoders.bean(EventDate.class)));

    temporalCoverages
        .write()
        .mode(SaveMode.Overwrite)
        .parquet(outputPath + "/derived/temporal_coverage");

    log.info("Derived metadata calculation is not yet implemented.");

    return null;
  }

  private static Dataset<Tuple2<String, EventDate>> gatherEventDatesFromChildEvents(
      SparkSession spark, Dataset<Event> events) {
    events.createOrReplaceTempView("simple_event");
    return spark
        .sql(
            """
                                                SELECT
                                                       parent_event.id as eventId,
                                                       child_event.temporal
                                                FROM simple_event parent_event
                                                LEFT OUTER JOIN simple_event child_event
                                                ON array_contains(child_event.lineage, parent_event.id)
                                            """)
        .filter(
            (FilterFunction<Row>)
                row -> {
                  String temporalJson = row.getAs("temporal");
                  TemporalRecord temporalRecord =
                      MAPPER.readValue(temporalJson, TemporalRecord.class);
                  return temporalRecord != null && temporalRecord.getEventDate() != null;
                })
        .map(
            (MapFunction<Row, Tuple2<String, EventDate>>)
                row -> {
                  String eventId = row.getAs("eventId");
                  String temporalJson = row.getAs("temporal");
                  TemporalRecord temporalRecord =
                      MAPPER.readValue(temporalJson, TemporalRecord.class);
                  return new Tuple2(eventId, temporalRecord.getEventDate());
                },
            Encoders.tuple(Encoders.STRING(), Encoders.bean(EventDate.class)));
  }

  private static Dataset<EventCoordinate> gatherCoordinatesFromChildEvents(
      SparkSession spark, Dataset<Event> events) {
    events.createOrReplaceTempView("simple_event");
    return spark
        .sql(
            """
                                    SELECT
                                           parent_event.id as eventId,
                                           child_event.location
                                    FROM simple_event parent_event
                                    LEFT OUTER JOIN simple_event child_event
                                    ON array_contains(child_event.lineage, parent_event.id)
                                """)
        .filter(
            (FilterFunction<Row>)
                row -> {
                  String locationJson = row.getAs("location");
                  LocationRecord locationRecord =
                      MAPPER.readValue(locationJson, LocationRecord.class);
                  return locationRecord.getHasCoordinate()
                      && locationRecord.getDecimalLatitude() != null
                      && locationRecord.getDecimalLongitude() != null
                      && locationRecord.getDecimalLatitude() >= -90.0
                      && locationRecord.getDecimalLatitude() <= 90.0
                      && locationRecord.getDecimalLongitude() >= -180.0
                      && locationRecord.getDecimalLongitude() <= 180.0;
                })
        .map(
            (MapFunction<Row, EventCoordinate>)
                row -> {
                  String eventId = row.getAs("eventId");
                  String locationJson = row.getAs("location");
                  LocationRecord locationRecord =
                      MAPPER.readValue(locationJson, LocationRecord.class);
                  return new EventCoordinate(
                      eventId,
                      locationRecord.getDecimalLongitude(),
                      locationRecord.getDecimalLatitude());
                },
            Encoders.bean(EventCoordinate.class));
  }

  private static Dataset<Occurrence> loadOccurrences(
      SparkSession spark, FileSystem fs, String outputPath) throws IOException {
    if (fs.exists(new Path(outputPath + "/" + SIMPLE_OCCURRENCE))
        && fs.exists(new Path(outputPath + "/" + SIMPLE_OCCURRENCE + "/_SUCCESS"))) {
      return spark
          .read()
          .parquet(outputPath + "/" + SIMPLE_OCCURRENCE)
          .as(Encoders.bean(Occurrence.class));
    } else {
      return spark.emptyDataset(Encoders.bean(Occurrence.class));
    }
  }

  private static Dataset<EventCoordinate> getEventCoordinates(Dataset<Event> events) {
    return events
        .map(
            (MapFunction<Event, Tuple3<String, Double, Double>>)
                event -> {
                  LocationRecord lir = MAPPER.readValue(event.getLocation(), LocationRecord.class);
                  return new Tuple3<>(
                      event.getId(), lir.getDecimalLongitude(), lir.getDecimalLatitude());
                },
            Encoders.tuple(Encoders.STRING(), Encoders.DOUBLE(), Encoders.DOUBLE()))
        .filter(
            (FilterFunction<Tuple3<String, Double, Double>>)
                t -> {
                  return (t._2() != null) && (t._3() != null);
                })
        .map(
            (MapFunction<Tuple3<String, Double, Double>, EventCoordinate>)
                t -> {
                  return new EventCoordinate(t._1(), t._2(), t._3());
                },
            Encoders.bean(EventCoordinate.class));
  }

  private static Dataset<Tuple2<String, EventDate>> getCoreIdEventDates(
      Dataset<Occurrence> occurrence) {
    return occurrence
        .map(
            (MapFunction<Occurrence, Tuple2<String, EventDate>>)
                occ -> {
                  TemporalRecord lr = MAPPER.readValue(occ.getTemporal(), TemporalRecord.class);
                  String coreId = occ.getCoreId();
                  return new Tuple2<>(coreId, lr.getEventDate());
                },
            Encoders.tuple(Encoders.STRING(), Encoders.bean(EventDate.class)))
        .distinct();
  }

  private static Dataset<EventCoordinate> getCoreIdCoordinates(Dataset<Occurrence> occurrence) {
    return occurrence
        .map(
            (MapFunction<Occurrence, Tuple3<String, Double, Double>>)
                occ -> {
                  LocationRecord lir = MAPPER.readValue(occ.getLocation(), LocationRecord.class);
                  String coreId = occ.getCoreId();
                  return new Tuple3<>(coreId, lir.getDecimalLongitude(), lir.getDecimalLatitude());
                },
            Encoders.tuple(Encoders.STRING(), Encoders.DOUBLE(), Encoders.DOUBLE()))
        .filter(
            (FilterFunction<Tuple3<String, Double, Double>>)
                t -> {
                  return (t._2() != null) && (t._3() != null);
                })
        .map(
            (MapFunction<Tuple3<String, Double, Double>, EventCoordinate>)
                t -> {
                  return new EventCoordinate(t._1(), t._2(), t._3());
                },
            Encoders.bean(EventCoordinate.class))
        .distinct();
  }

  @Data
  public static class TemporalAccum implements Serializable {

    private String minDate;
    private String maxDate;

    public TemporalAccum acc(EventDate eventDate) {
      Optional.ofNullable(eventDate.getGte()).ifPresent(this::setMinDate);
      Optional.ofNullable(eventDate.getLte()).ifPresent(this::setMaxDate);
      return this;
    }

    private void setMinDate(String date) {
      if (Objects.isNull(minDate)) {
        minDate = date;
      } else {
        minDate =
            StringToDateFunctions.getStringToEarliestEpochSeconds(false)
                        .apply(date)
                        .compareTo(
                            StringToDateFunctions.getStringToEarliestEpochSeconds(false)
                                .apply(minDate))
                    < 0
                ? date
                : minDate;
      }
    }

    private void setMaxDate(String date) {
      if (Objects.isNull(maxDate)) {
        maxDate = date;
      } else {
        maxDate =
            StringToDateFunctions.getStringToLatestEpochSeconds(false)
                        .apply(date)
                        .compareTo(
                            StringToDateFunctions.getStringToLatestEpochSeconds(false)
                                .apply(maxDate))
                    > 0
                ? date
                : maxDate;
      }
    }

    public Optional<EventDate> toEventDate() {
      return Objects.isNull(minDate) && Objects.isNull(maxDate)
          ? Optional.empty()
          : Optional.of(getEventDate());
    }

    private EventDate getEventDate() {
      EventDate.Builder evenDate = EventDate.newBuilder();
      Optional.ofNullable(minDate).ifPresent(evenDate::setGte);
      Optional.ofNullable(maxDate).ifPresent(evenDate::setLte);
      return evenDate.build();
    }
  }
}
