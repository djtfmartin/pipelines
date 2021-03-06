cd solr

echo 'Zipping configset'
rm config.zip
zip config.zip *

echo 'Deleting existing collection'
curl -X GET "http://localhost:8983/solr/admin/collections?action=DELETE&name=biocache"

echo 'Deleting existing configset'
curl -X GET "http://localhost:8983/solr/admin/configs?action=DELETE&name=biocache&omitHeader=true"

echo 'Creating  configset'
curl -X POST --header "Content-Type:application/octet-stream" --data-binary @config.zip "http://localhost:8983/solr/admin/configs?action=UPLOAD&name=biocache"

echo 'Creating  collection'
curl -X GET "http://localhost:8983/solr/admin/collections?action=CREATE&name=biocache&numShards=1&replicationFactor=1&collection.configName=biocache"
cd ..

echo 'Done'



