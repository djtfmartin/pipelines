package org.gbif.pipelines.core.interpreters.extension;

import java.util.Collections;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.interpreters.ExtensionInterpretation;
import org.gbif.pipelines.core.interpreters.ExtensionInterpretation.Result;
import org.gbif.pipelines.core.interpreters.ExtensionInterpretation.TargetHandler;
import org.gbif.pipelines.core.interpreters.model.ExtendedRecord;
import org.gbif.pipelines.core.interpreters.model.MeasurementOrFact;
import org.gbif.pipelines.core.interpreters.model.MeasurementOrFactRecord;

/**
 * Interpreter for the MeasurementsOrFacts extension, Interprets form {@link ExtendedRecord} to
 * {@link MeasurementOrFactRecord}.
 *
 * @see <a href="http://rs.gbif.org/extension/dwc/measurements_or_facts.xml</a>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MeasurementOrFactInterpreter {

  private static final Function<Supplier<MeasurementOrFact>, TargetHandler<MeasurementOrFact>>
      HANDLER =
          mft ->
              ExtensionInterpretation.extension(Extension.MEASUREMENT_OR_FACT)
                  .to(mft)
                  .map(DwcTerm.measurementID, MeasurementOrFact::setMeasurementID)
                  .map(DwcTerm.measurementType, MeasurementOrFact::setMeasurementType)
                  .map(DwcTerm.measurementUnit, MeasurementOrFact::setMeasurementUnit)
                  .map(DwcTerm.measurementValue, MeasurementOrFact::setMeasurementValue)
                  .map(DwcTerm.measurementAccuracy, MeasurementOrFact::setMeasurementAccuracy)
                  .map(
                      DwcTerm.measurementDeterminedBy,
                      MeasurementOrFact::setMeasurementDeterminedBy)
                  .map(
                      DwcTerm.measurementDeterminedDate,
                      MeasurementOrFact::setMeasurementDeterminedDate)
                  .map(DwcTerm.measurementMethod, MeasurementOrFact::setMeasurementMethod)
                  .map(DwcTerm.measurementRemarks, MeasurementOrFact::setMeasurementRemarks);

  private static final Function<Supplier<MeasurementOrFact>, TargetHandler<MeasurementOrFact>>
      EXTENDED_HANDLER =
          mft ->
              ExtensionInterpretation.extension(Extension.EXTENDED_MEASUREMENT_OR_FACT)
                  .to(mft)
                  .map(DwcTerm.measurementID, MeasurementOrFact::setMeasurementID)
                  .map(DwcTerm.measurementType, MeasurementOrFact::setMeasurementType)
                  .map(DwcTerm.measurementUnit, MeasurementOrFact::setMeasurementUnit)
                  .map(DwcTerm.measurementValue, MeasurementOrFact::setMeasurementValue)
                  .map(DwcTerm.measurementAccuracy, MeasurementOrFact::setMeasurementAccuracy)
                  .map(
                      DwcTerm.measurementDeterminedBy,
                      MeasurementOrFact::setMeasurementDeterminedBy)
                  .map(
                      DwcTerm.measurementDeterminedDate,
                      MeasurementOrFact::setMeasurementDeterminedDate)
                  .map(DwcTerm.measurementMethod, MeasurementOrFact::setMeasurementMethod)
                  .map(DwcTerm.measurementRemarks, MeasurementOrFact::setMeasurementRemarks);

  /**
   * Interprets measurements or facts of a {@link ExtendedRecord} and populates a {@link
   * MeasurementOrFactRecord} with the interpreted values.
   */
  public static void interpret(
      ExtendedRecord er, MeasurementOrFactRecord mfr, Supplier<MeasurementOrFact> supplier) {
    Objects.requireNonNull(er);
    Objects.requireNonNull(mfr);

    Result<MeasurementOrFact> result = HANDLER.apply(supplier).convert(er);
    Result<MeasurementOrFact> extendedResult = EXTENDED_HANDLER.apply(supplier).convert(er);
    mfr.setMeasurementOrFactItems(
        Collections.unmodifiableList(
            Stream.concat(result.getList().stream(), extendedResult.getList().stream())
                .collect(Collectors.toList())));
    mfr.getIssues().setIssueList(result.getIssuesAsList());
  }
}
