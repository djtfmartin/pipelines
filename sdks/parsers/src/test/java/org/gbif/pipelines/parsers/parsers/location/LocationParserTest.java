package org.gbif.pipelines.parsers.parsers.location;

import java.util.Arrays;
import java.util.Collections;

import org.gbif.api.vocabulary.Country;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.parsers.parsers.common.ParsedField;
import org.gbif.pipelines.parsers.utils.ExtendedRecordBuilder;
import org.gbif.rest.client.geocode.GeocodeResponse;
import org.gbif.rest.client.geocode.Location;

import org.junit.Assert;
import org.junit.Test;

import static org.gbif.api.vocabulary.OccurrenceIssue.COORDINATE_ROUNDED;
import static org.gbif.api.vocabulary.OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES;
import static org.gbif.api.vocabulary.OccurrenceIssue.COUNTRY_INVALID;
import static org.gbif.api.vocabulary.OccurrenceIssue.GEODETIC_DATUM_ASSUMED_WGS84;

public class LocationParserTest {

  private static final String TEST_ID = "1";

  private static final Double LATITUDE_CANADA = 60.4;
  private static final Double LONGITUDE_CANADA = -131.3;

  private static final KeyValueTestStore TEST_STORE = new KeyValueTestStore();

  static {
    TEST_STORE.put(new LatLng(60.4d, -131.3d), toGeocodeResponse(Country.CANADA));
    TEST_STORE.put(new LatLng(30.2d, 100.2344349d), toGeocodeResponse(Country.CHINA));
    TEST_STORE.put(new LatLng(30.2d, 100.234435d), toGeocodeResponse(Country.CHINA));
    TEST_STORE.put(new LatLng(71.7d, -42.6d), toGeocodeResponse(Country.GREENLAND));
    TEST_STORE.put(new LatLng(-17.65, -149.46), toGeocodeResponse(Country.FRENCH_POLYNESIA));
    TEST_STORE.put(new LatLng(27.15, -13.20), toGeocodeResponse(Country.MOROCCO));
  }

  private static GeocodeResponse toGeocodeResponse(Country country) {
    Location location = new Location();
    location.setIsoCountryCode2Digit(country.getIso2LetterCode());
    return new GeocodeResponse(Collections.singletonList(location));
  }

  private KeyValueTestStore getkvStore() {
    return TEST_STORE;
  }

  @Test
  public void parseByCountryTest() {

    // State
    ExtendedRecord extendedRecord =
        ExtendedRecordBuilder.create().id(TEST_ID).country("Spain").build();

    // When
    ParsedField<ParsedLocation> result = LocationParser.parse(extendedRecord, getkvStore());

    // Should
    Assert.assertEquals(Country.SPAIN, result.getResult().getCountry());
  }

  @Test
  public void parseByCountryCodeTest() {

    // State
    ExtendedRecord extendedRecord =
        ExtendedRecordBuilder.create().id(TEST_ID).countryCode("ES").build();

    // When
    ParsedField<ParsedLocation> result = LocationParser.parse(extendedRecord, getkvStore());

    // Should
    Assert.assertEquals(Country.SPAIN, result.getResult().getCountry());
  }

  @Test
  public void parseByCountryAndAcountryCodeTest() {

    // State
    ExtendedRecord extendedRecord =
        ExtendedRecordBuilder.create().id(TEST_ID).country("Spain").countryCode("ES").build();

    // When
    ParsedField<ParsedLocation> result = LocationParser.parse(extendedRecord, getkvStore());

    // Should
    Assert.assertEquals(Country.SPAIN, result.getResult().getCountry());
  }

  @Test
  public void invalidCountryIssueTest() {

    // State
    ExtendedRecord extendedRecord =
        ExtendedRecordBuilder.create().id(TEST_ID).country("foo").build();

    // When
    ParsedField<ParsedLocation> result = LocationParser.parse(extendedRecord, getkvStore());

    // Should
    Assert.assertFalse(result.isSuccessful());
    Assert.assertNull(result.getResult().getCountry());
    Assert.assertTrue(result.getIssues().contains(COUNTRY_INVALID.name()));
  }

  @Test
  public void invalidCountryCodeIssue() {

    // State
    ExtendedRecord extendedRecord =
        ExtendedRecordBuilder.create().id(TEST_ID).countryCode("foo").build();

    // When
    ParsedField<ParsedLocation> result = LocationParser.parse(extendedRecord, getkvStore());

    // Should
    Assert.assertFalse(result.isSuccessful());
    Assert.assertNull(result.getResult().getCountry());
    Assert.assertTrue(result.getIssues().contains(COUNTRY_INVALID.name()));
  }

  @Test
  public void coordsWithDerivedCountryTest() {

    // State
    ExtendedRecord extendedRecord =
        ExtendedRecordBuilder.create()
            .id(TEST_ID)
            .decimalLatitude("30.2")
            .decimalLongitude("100.2344349")
            .build();

    // When
    ParsedField<ParsedLocation> result = LocationParser.parse(extendedRecord, getkvStore());

    // Should
    Assert.assertFalse(result.isSuccessful());
    Assert.assertEquals(Country.CHINA, result.getResult().getCountry());
    Assert.assertEquals(30.2d, result.getResult().getLatLng().getLatitude(), 0);
    Assert.assertEquals(100.234435d, result.getResult().getLatLng().getLongitude(), 0);
    Assert.assertTrue(
        result
            .getIssues()
            .containsAll(
                Arrays.asList(
                    COORDINATE_ROUNDED.name(),
                    COUNTRY_DERIVED_FROM_COORDINATES.name(),
                    GEODETIC_DATUM_ASSUMED_WGS84.name())));
  }

  @Test
  public void verbatimLtnLngWithDerivedCountryTest() {

    // State
    ExtendedRecord extendedRecord =
        ExtendedRecordBuilder.create()
            .id(TEST_ID)
            .verbatimLatitude("30.2")
            .verbatimLongitude("100.2344349")
            .build();

    // When
    ParsedField<ParsedLocation> result = LocationParser.parse(extendedRecord, getkvStore());

    // Should
    Assert.assertFalse(result.isSuccessful());
    Assert.assertEquals(Country.CHINA, result.getResult().getCountry());
    Assert.assertEquals(30.2d, result.getResult().getLatLng().getLatitude(), 0);
    Assert.assertEquals(100.234435d, result.getResult().getLatLng().getLongitude(), 0);
    Assert.assertTrue(
        result
            .getIssues()
            .containsAll(
                Arrays.asList(
                    COORDINATE_ROUNDED.name(),
                    COUNTRY_DERIVED_FROM_COORDINATES.name(),
                    GEODETIC_DATUM_ASSUMED_WGS84.name())));
  }

  @Test
  public void verbatimCoordsWithDerivedCountryTest() {

    // State
    ExtendedRecord extendedRecord =
        ExtendedRecordBuilder.create().id(TEST_ID).verbatimCoords("30.2, 100.2344349").build();

    // When
    ParsedField<ParsedLocation> result = LocationParser.parse(extendedRecord, getkvStore());

    // Should
    Assert.assertFalse(result.isSuccessful());
    Assert.assertEquals(Country.CHINA, result.getResult().getCountry());
    Assert.assertEquals(30.2d, result.getResult().getLatLng().getLatitude(), 0);
    Assert.assertEquals(100.234435d, result.getResult().getLatLng().getLongitude(), 0);
    Assert.assertTrue(
        result
            .getIssues()
            .containsAll(
                Arrays.asList(
                    COORDINATE_ROUNDED.name(),
                    COUNTRY_DERIVED_FROM_COORDINATES.name(),
                    GEODETIC_DATUM_ASSUMED_WGS84.name())));
  }

  @Test
  public void coordsAndCountryWhenParsedThenReturnCoordsAndCountry() {

    // State
    ExtendedRecord extendedRecord =
        ExtendedRecordBuilder.create()
            .id(TEST_ID)
            .country(Country.CANADA.getTitle())
            .countryCode(Country.CANADA.getIso2LetterCode())
            .decimalLatitude(String.valueOf(LATITUDE_CANADA))
            .decimalLongitude(String.valueOf(LONGITUDE_CANADA))
            .build();

    // When
    ParsedField<ParsedLocation> result = LocationParser.parse(extendedRecord, getkvStore());

    // Should
    Assert.assertTrue(result.isSuccessful());
    Assert.assertEquals(Country.CANADA, result.getResult().getCountry());
    Assert.assertEquals(LATITUDE_CANADA, result.getResult().getLatLng().getLatitude(), 0);
    Assert.assertEquals(LONGITUDE_CANADA, result.getResult().getLatLng().getLongitude(), 0);
    Assert.assertEquals(1, result.getIssues().size());
    Assert.assertTrue(result.getIssues().contains(GEODETIC_DATUM_ASSUMED_WGS84.name()));
  }

  @Test(expected = NullPointerException.class)
  public void nullArgs() {
    // When
    LocationParser.parse(null, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidArgs() {
    // When
    LocationParser.parse(new ExtendedRecord(), null);
  }
}
