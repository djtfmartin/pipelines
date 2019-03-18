package org.gbif.pipelines.parsers.utils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Issues;

/** Helps to work with org.gbif.pipelines.io.avro models */
public class ModelUtils {

  private ModelUtils() {}

  public static String extractValue(ExtendedRecord er, Term term) {
    return er.getCoreTerms().get(term.qualifiedName());
  }

  public static Optional<String> extractOptValue(ExtendedRecord er, Term term) {
    return Optional.ofNullable(er.getCoreTerms().get(term.qualifiedName()));
  }

  /** Checks if a {@link ExtendedRecord} is null or empty. */
  public static void checkNullOrEmpty(ExtendedRecord extendedRecord) {
    Objects.requireNonNull(extendedRecord, "ExtendedRecord cannot be null");

    if (extendedRecord.getId() == null || extendedRecord.getId().isEmpty()) {
      throw new IllegalArgumentException("ExtendedRecord with id is required");
    }

    if (extendedRecord.getCoreTerms() == null || extendedRecord.getCoreTerms().isEmpty()) {
      throw new IllegalArgumentException("ExtendedRecord with core terms is required");
    }
  }

  public static void addIssue(Issues model, String issue) {
    model.getIssues().getIssueList().add(issue);
  }

  public static void addIssue(Issues model, OccurrenceIssue issue) {
    model.getIssues().getIssueList().add(issue.name());
  }

  public static void addIssue(Issues model, List<String> issues) {
    model.getIssues().getIssueList().addAll(issues);
  }
}
