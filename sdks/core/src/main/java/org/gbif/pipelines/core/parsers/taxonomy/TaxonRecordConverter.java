package org.gbif.pipelines.core.parsers.taxonomy;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.api.model.Constants;
import org.gbif.pipelines.model.Diagnostics;
import org.gbif.pipelines.model.RankedName;
import org.gbif.pipelines.model.RankedNameWithAuthorship;
import org.gbif.pipelines.model.TaxonRecord;
import org.gbif.rest.client.species.NameUsageMatchResponse;

/** Adapts a {@link NameUsageMatchResponse} into a {@link TaxonRecord} */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TaxonRecordConverter {

  public static final String IUCN_REDLIST_GBIF_KEY = Constants.IUCN_DATASET_KEY.toString();

  /**
   * I modify the parameter instead of creating a new one and returning it because the lambda
   * parameters are final used in Interpreter.
   */
  public static void convert(
      NameUsageMatchResponse nameUsageMatch,
      TaxonRecord taxonRecord,
      Supplier<RankedNameWithAuthorship> rankedNameWithAuthorshipSupplier,
      Supplier<RankedName> rankedNameSupplier) {
    Objects.requireNonNull(nameUsageMatch);
    convertInternal(
        nameUsageMatch, taxonRecord, rankedNameWithAuthorshipSupplier, rankedNameSupplier);
  }

  private static TaxonRecord convertInternal(
      NameUsageMatchResponse source,
      TaxonRecord taxonRecord,
      Supplier<RankedNameWithAuthorship> rankedNameWithAuthorshipSupplier,
      Supplier<RankedName> rankedNameSupplier) {

    List<RankedName> classifications =
        source.getClassification().stream()
            .map(t -> convertRankedName(t, rankedNameSupplier))
            .collect(Collectors.toList());

    taxonRecord.setClassification(classifications);
    taxonRecord.setSynonym(source.isSynonym());
    taxonRecord.setUsage(convertUsage(source.getUsage(), rankedNameWithAuthorshipSupplier));

    // Usage is set as the accepted usage if the accepted usage is null
    taxonRecord.setAcceptedUsage(
        Optional.ofNullable(
                convertUsage(source.getAcceptedUsage(), rankedNameWithAuthorshipSupplier))
            .orElse(taxonRecord.getUsage()));

    taxonRecord.setDiagnostics(convertDiagnostics(source.getDiagnostics()));

    // IUCN Red List Category
    Optional.ofNullable(source.getAdditionalStatus()).orElseGet(List::of).stream()
        .filter(status -> status.getDatasetKey().equals(IUCN_REDLIST_GBIF_KEY))
        .findFirst()
        .map(status -> status.getStatusCode())
        .ifPresent(taxonRecord::setIucnRedListCategoryCode);

    return taxonRecord;
  }

  private static RankedNameWithAuthorship convertUsage(
      NameUsageMatchResponse.Usage rankedNameApi, Supplier<RankedNameWithAuthorship> supplier) {
    if (rankedNameApi == null) {
      return null;
    }

    RankedNameWithAuthorship rankedNameWithAuthorship = supplier.get();
    rankedNameWithAuthorship.setKey(rankedNameApi.getKey());
    rankedNameWithAuthorship.setName(rankedNameApi.getName());
    rankedNameWithAuthorship.setRank(rankedNameApi.getRank());
    rankedNameWithAuthorship.setAuthorship(rankedNameApi.getAuthorship());
    rankedNameWithAuthorship.setCode(rankedNameApi.getCode());
    rankedNameWithAuthorship.setInfragenericEpithet(rankedNameApi.getInfragenericEpithet());
    rankedNameWithAuthorship.setInfraspecificEpithet(rankedNameApi.getInfraspecificEpithet());
    rankedNameWithAuthorship.setSpecificEpithet(rankedNameApi.getSpecificEpithet());
    rankedNameWithAuthorship.setFormattedName(rankedNameApi.getFormattedName());
    rankedNameWithAuthorship.setStatus(rankedNameApi.getStatus());

    return rankedNameWithAuthorship;
  }

  private static RankedName convertRankedName(
      NameUsageMatchResponse.RankedName rankedNameApi, Supplier<RankedName> supplier) {
    if (rankedNameApi == null) {
      return null;
    }

    RankedName rankedName = supplier.get();
    rankedName.setKey(rankedNameApi.getKey());
    rankedName.setName(rankedNameApi.getName());
    rankedName.setRank(rankedNameApi.getRank());
    return rankedName;
  }

  private static Diagnostics convertDiagnostics(NameUsageMatchResponse.Diagnostics diagnosticsApi) {
    return null;

    //    if (diagnosticsApi == null) {
    //      return null;
    //    }
    //
    //    // alternatives
    //    List<TaxonRecord> alternatives =
    //        diagnosticsApi.getAlternatives().stream()
    //            .map(match -> convertInternal(match, TaxonRecord.newBuilder().build()))
    //            .collect(Collectors.toList());
    //
    //    Diagnostic.Builder builder =
    //        Diagnostic.newBuilder()
    //            //            .setAlternatives(alternatives)
    //            .setConfidence(diagnosticsApi.getConfidence())
    //            .setMatchType(MatchType.valueOf(diagnosticsApi.getMatchType().name()))
    //            .setNote(diagnosticsApi.getNote())
    //            .setLineage(List.of());
    //
    //    return builder.build();
  }
}
