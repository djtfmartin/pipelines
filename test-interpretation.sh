#!/usr/bin/env bash

java -XX:+UseG1GC -Xms1G -Xmx4G -jar pipelines/ingest-gbif-standalone/target/ingest-gbif-standalone-2.3.1-SNAPSHOT-shaded.jar \
--pipelineStep=VERBATIM_TO_ALA_INTERPRETED \
--datasetId=dr1411 \
--attempt=1 \
--interpretationTypes=ALL \
--runner=SparkRunner \
--targetPath=/data/pipelines-data \
--inputPath=/data/pipelines-data/dr1411/1/verbatim.avro \
--metaFileName=interpretation-metrics.txt \
--properties=pipelines.properties \
--useExtendedRecordId=true \
--skipRegisrtyCalls=true
