package org.gbif.pipelines.core.interpreters.model;

import java.util.List;

public interface LocationRecord extends Record {

    String getId();

    void setId(String id);

    String getCoreId();

    void setCoreId(String coreId);

    String getParentId();

    void setParentId(String parentId);

    Long getCreated();

    void setCreated(Long created);

    String getContinent();

    void setContinent(String continent);

    String getWaterBody();

    void setWaterBody(String waterBody);

    String getCountry();

    void setCountry(String country);

    String getCountryCode();

    void setCountryCode(String countryCode);

    String getPublishingCountry();

    void setPublishingCountry(String publishingCountry);

    String getGbifRegion();

    void setGbifRegion(String gbifRegion);

    String getPublishedByGbifRegion();

    void setPublishedByGbifRegion(String publishedByGbifRegion);

    String getStateProvince();

    void setStateProvince(String stateProvince);

    Double getMinimumElevationInMeters();

    void setMinimumElevationInMeters(Double minimumElevationInMeters);

    Double getMaximumElevationInMeters();

    void setMaximumElevationInMeters(Double maximumElevationInMeters);

    Double getElevation();

    void setElevation(Double elevation);

    Double getElevationAccuracy();

    void setElevationAccuracy(Double elevationAccuracy);

    Double getMinimumDepthInMeters();

    void setMinimumDepthInMeters(Double minimumDepthInMeters);

    Double getMaximumDepthInMeters();

    void setMaximumDepthInMeters(Double maximumDepthInMeters);

    Double getDepth();

    void setDepth(Double depth);

    Double getDepthAccuracy();

    void setDepthAccuracy(Double depthAccuracy);

    Double getMinimumDistanceAboveSurfaceInMeters();

    void setMinimumDistanceAboveSurfaceInMeters(Double minimumDistanceAboveSurfaceInMeters);

    Double getMaximumDistanceAboveSurfaceInMeters();

    void setMaximumDistanceAboveSurfaceInMeters(Double maximumDistanceAboveSurfaceInMeters);

    Double getDecimalLatitude();

    void setDecimalLatitude(Double decimalLatitude);

    Double getDecimalLongitude();

    void setDecimalLongitude(Double decimalLongitude);

    Double getCoordinateUncertaintyInMeters();

    void setCoordinateUncertaintyInMeters(Double coordinateUncertaintyInMeters);

    Double getCoordinatePrecision();

    void setCoordinatePrecision(Double coordinatePrecision);

    Boolean getHasCoordinate();

    void setHasCoordinate(Boolean hasCoordinate);

    Boolean getRepatriated();

    void setRepatriated(Boolean repatriated);

    Boolean getHasGeospatialIssue();

    void setHasGeospatialIssue(Boolean hasGeospatialIssue);

    String getLocality();

    void setLocality(String locality);

    String getGeoreferencedDate(); // Logical type for timestamp, often mapped to String or java.time.Instant

    void setGeoreferencedDate(String georeferencedDate);

    GadmFeatures getGadm();

    void setGadm(GadmFeatures gadm);

    String getFootprintWKT();

    void setFootprintWKT(String footprintWKT);

    String getBiome();

    void setBiome(String biome);

    Double getDistanceFromCentroidInMeters();

    void setDistanceFromCentroidInMeters(Double distanceFromCentroidInMeters);

    List<String> getHigherGeography();

    void setHigherGeography(List<String> higherGeography);

    List<String> getGeoreferencedBy();

    void setGeoreferencedBy(List<String> georeferencedBy);
}
