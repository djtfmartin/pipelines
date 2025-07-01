package org.gbif.pipelines.core.interpreters.model;

import java.util.*;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.Term;

/**
 * Interface for a record that provides methods to extract values using terms, check for values, and
 * handle extensions.
 */
public interface ExtendedRecord extends Record {

  List<String> extractListValue(String separatorRegex, Term term);

  List<String> extractListValue(Term term);

  Map<String, List<Map<String, String>>> getExtensions();

  Map<String, String> getCoreTerms();

  Optional<String> extractLengthAwareOptValue(Term term);

  Optional<String> extractNullAwareOptValue(Map<String, String> termsSource, Term term);

  Optional<String> extractNullAwareOptValue(Term term);

  Optional<String> extractOptValue(Term term);

  String extractNullAwareValue(Term term);

  String extractValue(Term term);

  String getCoreId();

  String getCoreRowType();

  String getId();

  boolean hasExtension(Extension extension);

  boolean hasExtension(String extension);

  boolean hasValue(String value);

  boolean hasValue(Term term);

  boolean hasValueNullAware(Term term);

  void checkEmpty();

  void setCoreRowType(String s);

  void setCoreTerms(Map<String, String> stringStringMap);

  void setExtensions(Map<String, List<Map<String, String>>> collect);
}
