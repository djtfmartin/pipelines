#!/usr/bin/env bash

#java -cp pipelines/ingest-gbif-standalone/target/ingest-gbif-standalone-2.2.32-SNAPSHOT-shaded.jar org.gbif.pipelines.ingest.pipelines.DwcaToVerbatimPipeline --datasetId=9f747cff-839f-4485-83a1-f10317a92a82 --attempt=1 --runner=SparkRunner --targetPath=/data/biocache-load/dr1411/ --inputPath=/data/biocache-load/dr1411/

java -cp pipelines/ingest-gbif-standalone/target/ingest-gbif-standalone-2.2.32-SNAPSHOT-shaded.jar \
 org.gbif.pipelines.standalone.DwcaPipeline \
  --pipelineStep=DWCA_TO_VERBATIM \
  --datasetId=9f747cff-839f-4485-83a1-f10317a92a82 \
  --attempt=1 \
  --runner=SparkRunner \
  --targetPath=/data/biocache-load/dr1411/ \
  --inputPath=/data/biocache-load/dr1411/
