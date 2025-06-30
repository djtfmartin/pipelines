package org.gbif.pipelines.core.interpreters.model;

import org.gbif.api.vocabulary.OccurrenceIssue;

public interface GrscicollRecord extends Record {

    /**
     * @return the institution match, or null if not present
     */
    Match getInstitutionMatch();

    /**
     * @param match the institution match to set
     */
    void setInstitutionMatch(Match match);

    /**
     * @return the collection match, or null if not present
     */
    Match getCollectionMatch();

    /**
     * @param match the collection match to set
     */
    void setCollectionMatch(Match match);
}
