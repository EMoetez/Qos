#!/bin/bash
set -e

echo "Waiting for NiFi to fully start..."
sleep 120  # Adjust this based on your machine/network

echo "Importing flow into NiFi..."

curl -k -X POST "https://localhost:8443/nifi-api/process-groups/root/process-group-references" \
  -H "Content-Type: application/json" \
  -d "@/opt/bootstrap/nifi-flow.json"

echo "Flow imported successfully."

# Start NiFi after bootstrap
exec /opt/nifi/nifi-current/bin/nifi.sh run
