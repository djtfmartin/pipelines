package org.gbif.pipelines.interpretation.spark;

import static org.gbif.pipelines.interpretation.ConfigUtil.loadConfig;
import static org.gbif.pipelines.interpretation.spark.Directories.SIMPLE_EVENT;
import static org.gbif.pipelines.interpretation.spark.Directories.SIMPLE_OCCURRENCE;
import static org.gbif.pipelines.interpretation.spark.SparkUtil.getFileSystem;
import static org.gbif.pipelines.interpretation.spark.SparkUtil.getSparkSession;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.json.DerivedMetadataRecord;
import org.locationtech.jts.geom.Coordinate;
import scala.Tuple2;
import scala.Tuple3;

@Slf4j
public class CalculateDerivedMetadata {

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

    events.createOrReplaceTempView("simple_event");

    // join to child events
    Dataset<Tuple2<String, Coordinate>> joinedToParents =
        spark
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
                      return locationRecord.getHasCoordinate();
                    })
            .map(
                (MapFunction<Row, Tuple2<String, Coordinate>>)
                    row -> {
                      String eventId = row.getAs("eventId");
                      String locationJson = row.getAs("location");
                      LocationRecord locationRecord =
                          MAPPER.readValue(locationJson, LocationRecord.class);

                      return new Tuple2<>(
                          eventId,
                          new Coordinate(
                              locationRecord.getDecimalLongitude(),
                              locationRecord.getDecimalLatitude()));
                    },
                Encoders.tuple(Encoders.STRING(), Encoders.bean(Coordinate.class)));

    log.info("joinedToParents {}", joinedToParents.count());

    // join child events ?
    Dataset<Tuple2<String, Coordinate>> coreIdEventCoordinates = getEventCoordinates(events);
    log.info("coreIdEventCoordinates {}", coreIdEventCoordinates.count());

    // does this dataset have occurrences ?
    Dataset<Occurrence> occurrence = loadOccurrences(spark, fileSystem, outputPath);

    // get unique occurrence locations - coredId -> "lat||long"
    Dataset<Tuple2<String, Coordinate>> coreIdOccurrenceCoordinates =
        getCoreIdCoordinates(occurrence);
    log.info("coreIdOccurrenceCoordinates {}", coreIdOccurrenceCoordinates.count());

    // join to events to get eventId -> "lat||long"

    // get unique occurrence temporal - coreId -> eventDate
    Dataset<Tuple2<String, EventDate>> coredIdOccurrenceEventDates =
        getCoreIdEventDates(occurrence);
    log.info("coredIdOccurrenceEventDates {}", coredIdOccurrenceEventDates.count());

    log.info("Derived metadata calculation is not yet implemented.");

    return null;
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

  private static Dataset<Tuple2<String, Coordinate>> getEventCoordinates(Dataset<Event> events) {
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
            (MapFunction<Tuple3<String, Double, Double>, Tuple2<String, Coordinate>>)
                t -> {
                  return new Tuple2<>(t._1(), new Coordinate(t._2(), t._3()));
                },
            Encoders.tuple(Encoders.STRING(), Encoders.bean(Coordinate.class)));
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

  private static Dataset<Tuple2<String, Coordinate>> getCoreIdCoordinates(
      Dataset<Occurrence> occurrence) {
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
            (MapFunction<Tuple3<String, Double, Double>, Tuple2<String, Coordinate>>)
                t -> {
                  return new Tuple2<>(t._1(), new Coordinate(t._2(), t._3()));
                },
            Encoders.tuple(Encoders.STRING(), Encoders.bean(Coordinate.class)))
        .distinct();
  }
}
