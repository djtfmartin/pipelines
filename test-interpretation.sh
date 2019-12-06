#!/usr/bin/env bash

#java -cp pipelines/ingest-gbif-standalone/target/ingest-gbif-standalone-2.2.32-SNAPSHOT-shaded.jar org.gbif.pipelines.ingest.pipelines.DwcaToInterpretedPipeline --datasetId=9f747cff-839f-4485-83a1-f10317a92a82 --attempt=1 --runner=SparkRunner --targetPath=/data/biocache-load/dr1411/ --inputPath=/data/biocache-load/dr1411/ --properties=/Users/mar759/dev/gbif/pipelines/gbif.properties

#java -cp pipelines/ingest-gbif-standalone/target/ingest-gbif-standalone-2.2.32-SNAPSHOT-shaded.jar org.gbif.pipelines.standalone.DwcaPipeline --pipelineStep=DWCA_TO_INTERPRETED --datasetId=9f747cff-839f-4485-83a1-f10317a92a82 --attempt=1 --runner=SparkRunner --targetPath=/data/biocache-load/dr1411/ --inputPath=/data/biocache-load/dr1411/ --properties=/Users/mar759/dev/pipelines/gbif.properties

java -XX:+UseG1GC -Xms1G -Xmx4G -jar pipelines/ingest-gbif-standalone/target/ingest-gbif-standalone-2.3.0-SNAPSHOT-shaded.jar \
--pipelineStep=VERBATIM_TO_ALA_INTERPRETED \
--datasetId=9f747cff-839f-4485-83a1-f10317a92a82 \
--attempt=1 \
--interpretationTypes=ALL \
--runner=SparkRunner \
--targetPath=/Users/mar759/dev/app-data/biocache-load/dr1411/ \
--inputPath=/Users/mar759/dev/app-data/biocache-load/dr1411/9f747cff-839f-4485-83a1-f10317a92a82/1/verbatim.avro \
--metaFileName=interpretation-metrics.txt \
--properties=/Users/mar759/dev/gbif/pipelines/pipelines.properties \
--useExtendedRecordId=true \
--skipRegisrtyCalls=true
