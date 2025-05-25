#!/bin/bash
set -e

echo "Waiting for NiFi to fully start..."
sleep 120  # Adjust this based on your machine/network

chmod -R 777 ./data# generate_token_and_start.sh

TOKEN=$(docker exec elasticsearch bash -c "bin/elasticsearch-service-tokens create elastic/kibana kibana-token" | grep -oP '(?<=token\":\")[^"]*')
echo "ELASTICSEARCH_SERVICEACCOUNTTOKEN=$TOKEN" > .env