#!/usr/bin/env bash


java -XX:+UseG1GC -Xms1G -Xmx4G -jar /Users/mar759/dev/gbif/pipelines/pipelines/ingest-gbif-standalone/target/ingest-gbif-standalone-2.3.1-SNAPSHOT-shaded.jar \
 --pipelineStep=INTERPRETED_TO_ALA_SOLR_INDEX \
 --datasetId=dr1411 \
 --attempt=1 \
 --runner=SparkRunner \
 --inputPath=/data/pipelines-data \
 --targetPath=/data/pipelines-data \
 --metaFileName=indexing-metrics.txt \
 --properties=pipelines.properties \
 --solrCollection=biocache \
 --includeSampling=false \
 --includeGbifTaxonomy=false \
 --zkHost=localhost:9983