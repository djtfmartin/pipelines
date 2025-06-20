package org.gbif.pipelines.core.parsers.location.parser;

import static org.gbif.api.vocabulary.OccurrenceIssue.*;

import java.util.*;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.GeocodeRequest;
import org.gbif.pipelines.core.interpreters.model.ExtendedRecord;
import org.gbif.pipelines.core.interpreters.model.LocationRecord;
import org.gbif.pipelines.core.parsers.VocabularyParser;
import org.gbif.pipelines.core.parsers.common.ParsedField;
import org.gbif.rest.client.geocode.GeocodeResponse;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ContinentParser {

  public static ParsedField<Continent> parseContinent(
      ExtendedRecord er,
      LocationRecord lr,
      KeyValueStore<GeocodeRequest, GeocodeResponse> kvStore) {
    Objects.requireNonNull(er, "ExtendedRecord is required");
    Objects.requireNonNull(lr, "LocationRecord is required");
    Objects.requireNonNull(kvStore, "GeocodeService kvStore is required");

    Set<String> issues = new TreeSet<>();

    // Parse continent string
    ParsedField<Continent> parsedContinent =
        parseContinent(er, VocabularyParser.continentParser(), CONTINENT_INVALID.name());
    Continent continent = getContinentResult(parsedContinent, issues).orElse(null);

    // Add an issue if the continent doesn't contain the country
    if (lr.getCountryCode() != null && continent != null) {
      Country country = Country.fromIsoCode(lr.getCountryCode());

      if (!CountryContinentMaps.continentsForCountry(country).contains(continent)) {
        issues.add(CONTINENT_COUNTRY_MISMATCH.name());
      }
    }

    // Take parsed coordinate value
    GeocodeRequest latLng =
        GeocodeRequest.create(
            lr.getDecimalLatitude(),
            lr.getDecimalLongitude(),
            lr.getCoordinateUncertaintyInMeters());

    // Use these to retrieve the Continent.
    if (latLng.getLat() != null && latLng.getLng() != null) {
      Optional<List<Continent>> continentsKv = getContinentFromCoordinates(latLng, kvStore);

      if (continentsKv.isPresent()) {
        // We are on land somewhere
        List<Continent> continents = continentsKv.get();

        if (continent == null) {
          // no continent supplied, so assign it
          issues.add(CONTINENT_DERIVED_FROM_COORDINATES.name());
          return ParsedField.success(continents.get(0), issues);
        } else if (continents.contains(continent)) {
          // continent found
          return ParsedField.success(continent, issues);
        } else {
          // Wrong continent found.
          issues.add(CONTINENT_COORDINATE_MISMATCH.name());
          return ParsedField.success(continent, issues);
        }
      } else {
        // We are in the ocean
        if (continent == null) {
          // Perfect.
          return ParsedField.success(null, issues);
        } else {
          // Continent supplied but in the ocean.
          issues.add(CONTINENT_COORDINATE_MISMATCH.name());
          return ParsedField.success(null, issues);
        }
      }
    } else if (lr.getCountryCode() != null) {
      // No coordinates, but we can check e.g. TURKİYE is either ASIA or EUROPE.
      Country country = Country.fromIsoCode(lr.getCountryCode());

      if (continent == null) {
        if (CountryContinentMaps.continentsForCountry(country).size() == 1) {
          // Set the continent if it's unambiguous, e.g. Switzerland
          issues.add(CONTINENT_DERIVED_FROM_COUNTRY.name());
          return ParsedField.success(
              CountryContinentMaps.continentsForCountry(country).get(0), issues);
        } else {
          // Transcontinental country.
          return parsedContinent;
        }
      } else {
        if (CountryContinentMaps.continentsForCountry(country).contains(continent)) {
          return parsedContinent;
        } else {
          issues.add(CONTINENT_COUNTRY_MISMATCH.name());
          return ParsedField.success(continent, issues);
        }
      }
    } else {
      return parsedContinent;
    }
  }

  private static Optional<List<Continent>> getContinentFromCoordinates(
      GeocodeRequest latLng, KeyValueStore<GeocodeRequest, GeocodeResponse> geocodeKvStore) {
    if (latLng.isValid()) {
      GeocodeResponse geocodeResponse = geocodeKvStore.get(latLng);
      if (geocodeResponse != null && !geocodeResponse.getLocations().isEmpty()) {
        List<Continent> continents =
            geocodeResponse.getLocations().stream()
                .filter(l -> "Continent".equals(l.getType()))
                .sorted(Comparator.comparingDouble(GeocodeResponse.Location::getDistance))
                .map(GeocodeResponse.Location::getId)
                .map(Continent::fromString)
                .collect(Collectors.toList());
        if (!continents.isEmpty()) {
          return Optional.of(continents);
        }
      }
    }
    return Optional.empty();
  }

  private static ParsedField<Continent> parseContinent(
      ExtendedRecord er, VocabularyParser<Continent> parser, String issue) {
    Optional<ParseResult<Continent>> parseResultOpt = parser.map(er, parseRes -> parseRes);

    if (!parseResultOpt.isPresent()) {
      // case when the continent is null in the extended record. We return an issue not to break the
      // whole interpretation
      return ParsedField.fail();
    }

    ParseResult<Continent> parseResult = parseResultOpt.get();
    ParsedField.ParsedFieldBuilder<Continent> builder = ParsedField.builder();
    if (parseResult.isSuccessful()) {
      builder.successful(true);
      builder.result(parseResult.getPayload());
    } else {
      builder.issues(Collections.singleton(issue));
    }
    return builder.build();
  }

  private static Optional<Continent> getContinentResult(
      ParsedField<Continent> field, Set<String> issues) {
    if (!field.isSuccessful()) {
      issues.addAll(field.getIssues());
    }

    return Optional.ofNullable(field.getResult());
  }
}
