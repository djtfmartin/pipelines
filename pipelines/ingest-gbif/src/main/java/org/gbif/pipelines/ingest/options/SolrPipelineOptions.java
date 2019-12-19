package org.gbif.pipelines.ingest.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/** Main pipeline options necessary for Elasticsearch index properties */
public interface SolrPipelineOptions extends PipelineOptions, InterpretationPipelineOptions {

    @Description("SOLR collection to index into")
    @Default.String("biocache")
    String solrCollection();
    void setSolrCollection(String collection);

    @Description("List of Zookeeper hosts.")
    String getZkHost();
    void setZkHost(String zkHosts);
}