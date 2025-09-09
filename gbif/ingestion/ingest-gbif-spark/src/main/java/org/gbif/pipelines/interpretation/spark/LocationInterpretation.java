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

import static org.gbif.dwc.terms.DwcTerm.GROUP_LOCATION;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.interpretation.transform.LocationTransform;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;

public class LocationInterpretation {

  /** Transforms the source records into the location records using the geocode service. */
  public static Dataset<LocationRecord> locationTransform(
      PipelinesConfig config,
      SparkSession spark,
      Dataset<ExtendedRecord> source,
      MetadataRecord mdr,
      int numPartitions) {

    LocationTransform locationTransform =
        LocationTransform.builder().geocodeApiUrl(config.getGeocode().getApi().getWsUrl()).build();

    // extract the location
    spark.sparkContext().setJobGroup("location", "Extract the location", true);
    Dataset<RecordWithLocation> recordWithLocation =
        source.map(
            (MapFunction<ExtendedRecord, RecordWithLocation>)
                or -> {
                  Location location = Location.buildFrom(or);
                  return RecordWithLocation.builder()
                      .id(or.getId())
                      .hash(location.hash())
                      .location(location)
                      .build();
                },
            Encoders.bean(RecordWithLocation.class));
    recordWithLocation.createOrReplaceTempView("record_with_location");

    // distinct the locations to lookup
    spark.sparkContext().setJobGroup("location", "Distinct the locations to lookup", true);
    Dataset<Location> distinctLocations =
        spark
            .sql("SELECT DISTINCT location.* FROM record_with_location")
            .repartition(numPartitions)
            .as(Encoders.bean(Location.class));

    // lookup the distinct locations, and create a dictionary of the results
    spark.sparkContext().setJobGroup("location", "Lookup the distinct locations", true);
    Dataset<KeyedLocationRecord> keyedLocation =
        distinctLocations.map(
            (MapFunction<Location, KeyedLocationRecord>)
                location -> {

                  // HACK - the function takes ExtendedRecord, but we have a Location
                  ExtendedRecord er =
                      ExtendedRecord.newBuilder()
                          .setId("UNUSED_BUT_NECESSARY")
                          .setCoreTerms(location.toCoreTermsMap())
                          .build();

                  // look them up
                  Optional<LocationRecord> converted = locationTransform.convert(er, mdr);

                  if (converted.isPresent()) {
                    return KeyedLocationRecord.builder()
                        .key(location.hash())
                        .locationRecord(converted.get())
                        .build();
                  } else {
                    return KeyedLocationRecord.builder()
                        .key(location.hash())
                        .build(); // TODO: null handling?
                  }
                },
            Encoders.bean(KeyedLocationRecord.class));
    keyedLocation.createOrReplaceTempView("key_location");

    // join the dictionary back to the source records
    spark.sparkContext().setJobGroup("location", "Join back to the source records", true);
    Dataset<RecordWithRecords> expanded =
        spark
            .sql(
                "SELECT r.id, l.locationRecord"
                    + " FROM record_with_location r "
                    + " LEFT JOIN key_location l ON r.hash = l.key")
            .as(Encoders.bean(RecordWithRecords.class));

    spark.sparkContext().setJobGroup("location", "Output Location Records", true);
    return expanded.map(
        (MapFunction<RecordWithRecords, LocationRecord>)
            r -> {
              LocationRecord locationRecord =
                  r.getLocationRecord() == null
                      ? LocationRecord.newBuilder().build()
                      : r.getLocationRecord();

              locationRecord.setId(r.getId());
              return locationRecord;
            },
        Encoders.bean(LocationRecord.class));
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class RecordWithLocation {
    private String id;
    private String hash;
    private Location location;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class KeyedLocationRecord {
    private String key;
    private LocationRecord locationRecord;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class RecordWithRecords {
    private String id;
    private LocationRecord locationRecord;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Location {
    private String id;
    private String locationID;
    private String higherGeographyID;
    private String higherGeography;
    private String continent;
    private String waterBody;
    private String islandGroup;
    private String island;
    private String country;
    private String countryCode;
    private String stateProvince;
    private String county;
    private String municipality;
    private String locality;
    private String verbatimLocality;
    private String minimumElevationInMeters;
    private String maximumElevationInMeters;
    private String verbatimElevation;
    private String verticalDatum;
    private String minimumDepthInMeters;
    private String maximumDepthInMeters;
    private String verbatimDepth;
    private String minimumDistanceAboveSurfaceInMeters;
    private String maximumDistanceAboveSurfaceInMeters;
    private String locationAccordingTo;
    private String locationRemarks;
    private String decimalLatitude;
    private String decimalLongitude;
    private String geodeticDatum;
    private String coordinateUncertaintyInMeters;
    private String coordinatePrecision;
    private String pointRadiusSpatialFit;
    private String verbatimCoordinates;
    private String verbatimLatitude;
    private String verbatimLongitude;
    private String verbatimCoordinateSystem;
    private String verbatimSRS;
    private String footprintWKT;
    private String footprintSRS;
    private String footprintSpatialFit;
    private String georeferencedBy;
    private String georeferencedDate;
    private String georeferenceProtocol;
    private String georeferenceSources;
    private String georeferenceRemarks;

    static Location buildFrom(ExtendedRecord er) {
      LocationBuilder builder = Location.builder();

      Arrays.stream(DwcTerm.values())
          .filter(t -> GROUP_LOCATION.equals(t.getGroup()) && !t.isClass())
          .forEach(
              term -> {
                String fieldName = term.simpleName(); // e.g., "country"
                String value =
                    er.getCoreTerms()
                        .get(term.qualifiedName()); // or however the ER provides values

                if (value != null) {
                  try {
                    Method setter = builder.getClass().getMethod(fieldName, String.class);
                    setter.invoke(builder, value);
                  } catch (NoSuchMethodException e) {
                    System.err.println("No setter for: " + fieldName);
                  } catch (Exception e) {
                    e.printStackTrace();
                  }
                }
              });

      return builder.build();
    }

    String hash() {
      return String.join(
          "|",
          id,
          locationID,
          higherGeographyID,
          higherGeography,
          continent,
          waterBody,
          islandGroup,
          island,
          country,
          countryCode,
          stateProvince,
          county,
          municipality,
          locality,
          verbatimLocality,
          minimumElevationInMeters,
          maximumElevationInMeters,
          verbatimElevation,
          verticalDatum,
          minimumDepthInMeters,
          maximumDepthInMeters,
          verbatimDepth,
          minimumDistanceAboveSurfaceInMeters,
          maximumDistanceAboveSurfaceInMeters,
          maximumDistanceAboveSurfaceInMeters,
          locationAccordingTo,
          locationRemarks,
          decimalLatitude,
          decimalLongitude,
          geodeticDatum,
          coordinateUncertaintyInMeters,
          coordinatePrecision,
          pointRadiusSpatialFit,
          verbatimCoordinates,
          verbatimLatitude,
          verbatimLongitude,
          verbatimCoordinateSystem,
          verbatimSRS,
          footprintWKT,
          footprintSRS,
          footprintSpatialFit,
          georeferencedBy,
          georeferencedDate,
          georeferenceProtocol,
          georeferenceSources,
          georeferenceRemarks);
    }

    public Map<String, String> toCoreTermsMap() {
      Map<String, String> coreTerms = new HashMap<>();
      putIfNotNull(coreTerms, DwcTerm.higherGeographyID, getHigherGeographyID());
      putIfNotNull(coreTerms, DwcTerm.higherGeography, getHigherGeography());
      putIfNotNull(coreTerms, DwcTerm.continent, getContinent());
      putIfNotNull(coreTerms, DwcTerm.waterBody, getWaterBody());
      putIfNotNull(coreTerms, DwcTerm.islandGroup, getIslandGroup());
      putIfNotNull(coreTerms, DwcTerm.island, getIsland());
      putIfNotNull(coreTerms, DwcTerm.country, getCountry());
      putIfNotNull(coreTerms, DwcTerm.countryCode, getCountryCode());
      putIfNotNull(coreTerms, DwcTerm.stateProvince, getStateProvince());
      putIfNotNull(coreTerms, DwcTerm.county, getCounty());
      putIfNotNull(coreTerms, DwcTerm.municipality, getMunicipality());
      putIfNotNull(coreTerms, DwcTerm.locality, getLocality());
      putIfNotNull(coreTerms, DwcTerm.verbatimLocality, getVerbatimLocality());
      putIfNotNull(coreTerms, DwcTerm.minimumElevationInMeters, getMinimumElevationInMeters());
      putIfNotNull(coreTerms, DwcTerm.maximumElevationInMeters, getMaximumElevationInMeters());
      putIfNotNull(coreTerms, DwcTerm.verbatimElevation, getVerbatimElevation());
      putIfNotNull(coreTerms, DwcTerm.verticalDatum, getVerticalDatum());
      putIfNotNull(coreTerms, DwcTerm.minimumDepthInMeters, getMinimumDepthInMeters());
      putIfNotNull(coreTerms, DwcTerm.maximumDepthInMeters, getMaximumDepthInMeters());
      putIfNotNull(coreTerms, DwcTerm.verbatimDepth, getVerbatimDepth());
      putIfNotNull(
          coreTerms,
          DwcTerm.minimumDistanceAboveSurfaceInMeters,
          getMinimumDistanceAboveSurfaceInMeters());
      putIfNotNull(
          coreTerms,
          DwcTerm.maximumDistanceAboveSurfaceInMeters,
          getMaximumDistanceAboveSurfaceInMeters());
      putIfNotNull(coreTerms, DwcTerm.locationAccordingTo, getLocationAccordingTo());
      putIfNotNull(coreTerms, DwcTerm.locationRemarks, getLocationRemarks());
      putIfNotNull(coreTerms, DwcTerm.decimalLatitude, getDecimalLatitude());
      putIfNotNull(coreTerms, DwcTerm.decimalLongitude, getDecimalLongitude());
      putIfNotNull(coreTerms, DwcTerm.geodeticDatum, getGeodeticDatum());
      putIfNotNull(
          coreTerms, DwcTerm.coordinateUncertaintyInMeters, getCoordinateUncertaintyInMeters());
      putIfNotNull(coreTerms, DwcTerm.coordinatePrecision, getCoordinatePrecision());
      putIfNotNull(coreTerms, DwcTerm.pointRadiusSpatialFit, getPointRadiusSpatialFit());
      putIfNotNull(coreTerms, DwcTerm.verbatimCoordinates, getVerbatimCoordinates());
      putIfNotNull(coreTerms, DwcTerm.verbatimLatitude, getVerbatimLatitude());
      putIfNotNull(coreTerms, DwcTerm.verbatimLongitude, getVerbatimLongitude());
      putIfNotNull(coreTerms, DwcTerm.verbatimCoordinateSystem, getVerbatimCoordinateSystem());
      putIfNotNull(coreTerms, DwcTerm.verbatimSRS, getVerbatimSRS());
      putIfNotNull(coreTerms, DwcTerm.footprintWKT, getFootprintWKT());
      putIfNotNull(coreTerms, DwcTerm.footprintSRS, getFootprintSRS());
      putIfNotNull(coreTerms, DwcTerm.footprintSpatialFit, getFootprintSpatialFit());
      putIfNotNull(coreTerms, DwcTerm.georeferencedBy, getGeoreferencedBy());
      putIfNotNull(coreTerms, DwcTerm.georeferencedDate, getGeoreferencedDate());
      putIfNotNull(coreTerms, DwcTerm.georeferenceProtocol, getGeoreferenceProtocol());
      putIfNotNull(coreTerms, DwcTerm.georeferenceSources, getGeoreferenceSources());
      putIfNotNull(coreTerms, DwcTerm.georeferenceRemarks, getGeoreferenceRemarks());

      return coreTerms;
    }

    private void putIfNotNull(Map<String, String> map, DwcTerm term, String value) {
      if (value != null) {
        map.put(term.qualifiedName(), value);
      }
    }
  }
}
