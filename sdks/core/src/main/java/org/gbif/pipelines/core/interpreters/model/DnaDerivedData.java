package org.gbif.pipelines.core.interpreters.model;

public interface DnaDerivedData {
    /**
     * @return the DNA sequence ID, or null if not present
     */
    String getDnaSequenceID();

    /**
     * @param dnaSequenceID the DNA sequence ID to set
     */
    void setDnaSequenceID(String dnaSequenceID);
}
