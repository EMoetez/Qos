#!/bin/bash
set -e # Exit on any error

PROJECT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )" # Get project root
ENV_FILE="${PROJECT_DIR}/.env" # Single .env file for Docker Compose
DATA_DIR="${PROJECT_DIR}/data"

# --- 1. Ensure Data Directories Exist and Set Permissions (Host-side) ---
echo "SETUP: Ensuring data directories exist and setting permissions..."
mkdir -p "${DATA_DIR}/staging" \
         "${DATA_DIR}/landing" \
         "${DATA_DIR}/processed" \
         "${DATA_DIR}/archive" \
         "${DATA_DIR}/error"

# Apply broad permissions for development.
# WARNING: 777 is insecure for production.
# Consider more specific chown/chmod if UIDs of container processes are known.
sudo chmod -R 777 "${DATA_DIR}" 
echo "SETUP: Permissions set for ${DATA_DIR}"


# --- 2. Start Elasticsearch and Wait for it to be Healthy ---
echo "STARTUP: Bringing up Elasticsearch..."
docker-compose up -d elasticsearch

echo "STARTUP: Waiting for Elasticsearch to be healthy..."
max_retries=30
count=0
# Assumes elasticsearch service in docker-compose.yml has a working healthcheck
until [[ "$(docker inspect -f '{{.State.Health.Status}}' elasticsearch 2>/dev/null)" == "healthy" ]]; do
  count=$((count+1))
  if [[ $count -gt $max_retries ]]; then
    echo "ERROR: Elasticsearch did not become healthy in time. Exiting."
    docker-compose logs elasticsearch
    exit 1
  fi
  echo "Waiting for Elasticsearch (attempt $count/$max_retries)... sleeping 5s"
  sleep 5
done
echo "STARTUP: Elasticsearch is healthy."


# --- 3. Generate Kibana Service Token ---
echo "SETUP: Generating Kibana service token..."
# Generate a unique name for the token each time to avoid "already exists" error
TOKEN_NAME="kibana-sa-token-$(date +%s)" 
# Attempt to get just the token string. Adjust parsing if ES output format changes.
# The `tail -n 1` assumes the token is the last line of relevant output.
KIBANA_TOKEN_LINE=$(docker exec elasticsearch bin/elasticsearch-service-tokens create elastic/kibana "${TOKEN_NAME}" | grep "TOKEN: ")

if [[ -z "$KIBANA_TOKEN_LINE" ]]; then
    echo "ERROR: Failed to generate Kibana token. Output from elasticsearch-service-tokens was:"
    # Run again without grep to see full output for debugging
    docker exec elasticsearch bin/elasticsearch-service-tokens create elastic/kibana "debug-${TOKEN_NAME}"
    exit 1
fi

KIBANA_SA_TOKEN=$(echo "$KIBANA_TOKEN_LINE" | awk '{print $2}')

if [ -z "$KIBANA_SA_TOKEN" ]; then
  echo "ERROR: Failed to parse Kibana token from output: $KIBANA_TOKEN_LINE"
  exit 1
fi
echo "SETUP: Kibana service token generated."


# --- 4. Write Token to .env file (Docker Compose will pick this up) ---
echo "SETUP: Writing KIBANA_SA_TOKEN to ${ENV_FILE}"
# Ensure .env file exists, create if not, then update/add the token
# This preserves other variables that might be in .env
if grep -q "KIBANA_SA_TOKEN=" "${ENV_FILE}" 2>/dev/null; then
  # Variable exists, update it
  sed -i.bak "s|^KIBANA_SA_TOKEN=.*|KIBANA_SA_TOKEN=${KIBANA_SA_TOKEN}|" "${ENV_FILE}" && rm "${ENV_FILE}.bak"
else
  # Variable doesn't exist, append it
  echo "KIBANA_SA_TOKEN=${KIBANA_SA_TOKEN}" >> "${ENV_FILE}"
fi
echo "SETUP: .env file updated."


# --- 5. Bring up the rest of the services ---
echo "STARTUP: Bringing up all services (including Kibana which will use the token)..."
docker-compose up -d 
#--build # --build if you have services with Dockerfiles that might change

echo "---------------------------------------------------------------------"
echo "Stack deployment initiated! It might take a few minutes for all services to be fully operational."
echo "Kibana will use the token: ${KIBANA_SA_TOKEN}"
echo "Check service status with: docker-compose ps"
echo "View logs with: docker-compose logs -f <service_name> or docker-compose logs -f"
echo "Access Kibana at http://localhost:5601 (once ready)"
echo "Access Spark Master UI at http://localhost:8081"
echo "---------------------------------------------------------------------"