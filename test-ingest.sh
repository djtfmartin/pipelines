#!/usr/bin/env bash

java -cp pipelines/ingest-gbif-standalone/target/ingest-gbif-standalone-2.3.1-SNAPSHOT-shaded.jar \
 org.gbif.pipelines.standalone.DwcaPipeline \
  --pipelineStep=DWCA_TO_VERBATIM \
  --datasetId=dr1411 \
  --attempt=1 \
  --runner=SparkRunner \
  --targetPath=/data/pipelines-data \
  --inputPath=/data/biocache-load/dr1411
