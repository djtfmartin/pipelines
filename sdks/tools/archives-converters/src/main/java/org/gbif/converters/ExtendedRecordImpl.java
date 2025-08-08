package org.gbif.converters;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.core.interpreters.model.ExtendedRecord;
import java.util.*;
import java.util.stream.Collectors;

import static org.gbif.pipelines.core.utils.IdentificationUtils.extractFromIdentificationExtension;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ExtendedRecordImpl implements ExtendedRecord {

    private String id;
    private String coreId;
    private String coreRowType = "http://rs.tdwg.org/dwc/terms/Occurrence";
    private Long created;
    private Map<String, String> coreTerms;
    private Map<String, List<Map<String, String>>> extensions;

    public static final String DEFAULT_SEPARATOR = "\\|";

    public String extractValue(Term term) {
        String value = this.getCoreTerms().get(term.qualifiedName());
        return value != null ? value.trim() : extractFromIdentificationExtension(this, term);
    }

    public static String extractValue(Map<String, String> termsSource, Term term) {
        return termsSource.get(term.qualifiedName());
    }

    /**
     * Extracts a Term value, if such value has a variation of the word "null" it is transformed to
     * null.
     */
    public String extractNullAwareValue(Term term) {
        String value = extractValue(term);
        if (hasValue(value)) {
            return value;
        } else {
            String valueFromIdentificationExtension = extractFromIdentificationExtension(this, term);
            return hasValue(valueFromIdentificationExtension) ? valueFromIdentificationExtension : null;
        }
    }

    public String extractNullAwareValue(Map<String, String> termsSource, Term term) {
        String value = extractValue(termsSource, term);
        return hasValue(value) ? value : null;
    }

    public boolean hasValue(String value) {
        return value != null && !value.isEmpty() && !"null".equalsIgnoreCase(value.trim());
    }

    public Optional<String> extractLengthAwareOptValue(Term term) {
        String value = extractNullAwareValue(term);
        // Indexing limit length
        value = value != null && value.getBytes().length >= 32000 ? null : value;
        return Optional.ofNullable(value);
    }

    public Optional<String> extractNullAwareOptValue(ExtendedRecord er, Term term) {
        return Optional.ofNullable(extractNullAwareValue(term));
    }

    public Optional<String> extractNullAwareOptValue(
            Map<String, String> termsSource, Term term) {
        return Optional.ofNullable(extractNullAwareValue(termsSource, term));
    }

    @Override
    public Optional<String> extractNullAwareOptValue(Term term) {
        return Optional.empty();
    }

    public Optional<String> extractOptValue(Term term) {
        return Optional.ofNullable(extractValue(term));
    }

    public List<String> extractListValue(String separatorRegex, Term term) {
        return extractOptValue(term)
                .filter(x -> !x.isEmpty())
                .map(
                        x ->
                                Arrays.stream(x.split(separatorRegex))
                                        .map(String::trim)
                                        .filter(v -> !v.isEmpty())
                                        .collect(Collectors.toList()))
                .orElse(Collections.emptyList());
    }

    @Override
    public List<String> extractListValue(Term term) {
        return List.of();
    }

    public boolean hasValue(Term term) {
        return extractOptValue(term).isPresent();
    }

    public boolean hasValueNullAware(Term term) {
        return extractNullAwareOptValue(term).isPresent();
    }

    @Override
    public void checkEmpty() {

        if (getId() == null || getId().isEmpty()) {
            throw new IllegalArgumentException("ExtendedRecord with id is required");
        }

        if (getCoreTerms() == null || getCoreTerms().isEmpty()) {
            throw new IllegalArgumentException("ExtendedRecord with core terms is required");
        }
    }

    public boolean hasExtension(Extension extension) {
        return hasExtension(extension.getRowType());
    }

    public boolean hasExtension(String extension) {
        return Optional.ofNullable(this.getExtensions().get(extension))
                .filter(l -> !l.isEmpty())
                .isPresent();
    }
}
