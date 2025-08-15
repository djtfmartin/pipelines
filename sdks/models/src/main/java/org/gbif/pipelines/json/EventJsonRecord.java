package org.gbif.pipelines.json;

import java.util.List;
import lombok.Builder;
import lombok.Data;

@Data
@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
public class EventJsonRecord {

  // Internal
  private String id;
  private String created;
  private List<Parent> parentsLineage;
  private String license;

  // Record-level
  private VocabularyConcept eventType;
  private String eventName;
  private String references;
  private String institutionCode;
  private List<String> datasetID;
  private List<String> datasetName;
  private String modified;

  // Event
  private List<String> samplingProtocol;
  private String samplingProtocolJoined;
  private String eventID;
  private String parentEventID;
  private String sampleSizeUnit;
  private Double sampleSizeValue;
  private Integer year;
  private Integer month;
  private Integer day;
  private EventDate eventDate;
  private String eventDateSingle;
  private Integer startDayOfYear;
  private Integer endDayOfYear;
  private String locationID;

  // Location
  private String locality;
  private String continent;
  private String waterBody;
  private String countryCode;
  private String country;
  private String stateProvince;
  private Double minimumElevationInMeters;
  private Double maximumElevationInMeters;
  private String verbatimElevation;
  private Double minimumDepthInMeters;
  private Double maximumDepthInMeters;
  private String verbatimDepth;
  private Double minimumDistanceAboveSurfaceInMeters;
  private Double maximumDistanceAboveSurfaceInMeters;
  private Double coordinateUncertaintyInMeters;
  private Double coordinatePrecision;
  private Coordinates coordinates;
  private String scoordinates;
  private Double decimalLatitude;
  private Double decimalLongitude;
  private GadmFeatures gadm;
  private Boolean hasCoordinate;
  private Boolean repatriated;
  private Boolean hasGeospatialIssue;
  private String footprintWKT;
  private Double distanceFromCentroidInMeters;
  private String publishingCountry;

  // Multimedia
  private List<String> mediaTypes;
  private List<String> mediaLicenses;
  private List<Multimedia> multimediaItems;

  // MeasurementOrFact
  private List<String> measurementOrFactTypes;
  private List<String> measurementOrFactMethods;
  private Integer measurementOrFactCount;

  // Extra
  private List<String> issues;
  private List<String> notIssues;
  private List<String> extensions;
  private VerbatimRecord verbatim;

  // Aggregations
  private Integer occurrenceCount;

  // De-normalised event info
  private String surveyID;
  private List<String> eventHierarchy;
  private List<String> eventTypeHierarchy;
  private String eventHierarchyJoined;
  private String eventTypeHierarchyJoined;
  private Integer eventHierarchyLevels;
  private List<MeasurementOrFactJsonRecord> measurementOrFacts;

  // Extensions
  private SeedbankRecord seedbankRecord;
}
