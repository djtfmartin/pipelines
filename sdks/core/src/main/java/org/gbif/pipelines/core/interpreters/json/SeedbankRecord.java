package org.gbif.pipelines.core.interpreters.json;

import lombok.Builder;
import lombok.Data;

/** Represents a record from a seed bank. */
@Data
@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
public class SeedbankRecord {
  private String id;
  private String accessionNumber;
  private Double adjustedGerminationPercentage;
  private String cultivated;
  private Double darkHours;
  private Long dateCollected;
  private Long dateInStorage;
  private Double dayTemperatureInCelsius;
  private String formInStorage;
  private Double germinationRateInDays;
  private Double lightHours;
  private String mediaSubstrate;
  private Double nightTemperatureInCelsius;
  private Double numberEmpty;
  private Double numberFull;
  private Double numberGerminated;
  private Double numberPlantsSampled;
  private Double numberTested;
  private String plantForm;
  private String pretreatment;
  private String primaryCollector;
  private String primaryStorageSeedBank;
  private Double purityPercentage;
  private String populationCode;
  private Double storageRelativeHumidityPercentage;
  private Double quantityCount;
  private Double quantityInGrams;
  private Double seedPerGram;
  private Double storageTemperatureInCelsius;
  private Long testDateStarted;
  private Double testLengthInDays;
  private Double thousandSeedWeight;
  private Double viabilityPercentage;
  private Long numberNotViable;
}
