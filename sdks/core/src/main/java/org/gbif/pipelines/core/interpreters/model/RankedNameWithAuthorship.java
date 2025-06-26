package org.gbif.pipelines.core.interpreters.model;

public interface RankedNameWithAuthorship {

    String getKey();
    void setKey(String key);

    String getName();
    void setName(String name);

    String getCanonicalName();
    void setCanonicalName(String canonicalName);

    String getRank();
    void setRank(String rank);

    String getAuthorship();
    void setAuthorship(String authorship);

    String getCode();
    void setCode(String code);

    String getStatus();
    void setStatus(String status);

    String getInfragenericEpithet();
    void setInfragenericEpithet(String infragenericEpithet);

    String getSpecificEpithet();
    void setSpecificEpithet(String specificEpithet);

    String getInfraspecificEpithet();
    void setInfraspecificEpithet(String infraspecificEpithet);

    String getFormattedName();
    void setFormattedName(String formattedName);
}
