/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.pipelines.interpretation.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.interpretation.transform.LocationTransform;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import scala.Tuple2;

@Slf4j
public class LocationInterpretation {

  static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /** Extract locations interpreted using the geocode service for the source records. */
  public static Dataset<Tuple2<String, String>> locationTransform(
      PipelinesConfig config,
      SparkSession spark,
      Dataset<ExtendedRecord> source,
      MetadataRecord mdr,
      String outputPath) {

    LocationTransform locationTransform = LocationTransform.builder().config(config).build();

    spark.sparkContext().setJobGroup("location", "Interpreting source records", true);
    Dataset<Tuple2<String, String>> interpretedLocations =
        source.map(
            (MapFunction<ExtendedRecord, Tuple2<String, String>>)
                record -> {
                  Optional<LocationRecord> location = locationTransform.convert(record, mdr);
                  if (location.isPresent()) {
                    return Tuple2.apply(
                        location.get().getId(),
                        OBJECT_MAPPER.writeValueAsString(location.get().getId()));
                  } else {
                    return null;
                  }
                },
            Encoders.tuple(Encoders.STRING(), Encoders.STRING()));

    return interpretedLocations;
  }
}
