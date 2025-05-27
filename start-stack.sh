#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status.

# --- Configuration ---
PROJECT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ENV_FILE="${PROJECT_DIR}/.env"
DATA_DIR="${PROJECT_DIR}/data"
ELASTICSEARCH_SERVICE_NAME="elasticsearch" # As defined in docker-compose.yml
ELASTIC_USER="elastic" # From your docker-compose.yml
ELASTIC_PASS="elasticelastic123" # From your docker-compose.yml
# The name you give to the token instance within Elasticsearch.
# The script will try to delete and recreate this named token.
KIBANA_TOKEN_NAME_IN_ES="kibana-runtime-token" 

# --- 1. Ensure Data Directories Exist and Set Host Permissions (Optional but good for dev) ---
echo "SETUP: Ensuring data directories exist and setting permissions..."
mkdir -p "${DATA_DIR}/staging" \
         "${DATA_DIR}/landing" \
         "${DATA_DIR}/processed" \
         "${DATA_DIR}/archive" \
         "${DATA_DIR}/error"

# Apply broad permissions for development.
# WARNING: 777 is insecure for production. You might be prompted for sudo.
if sudo chmod -R 777 "${DATA_DIR}"; then
    echo "SETUP: Permissions set for ${DATA_DIR}"
else
    echo "SETUP: Warning - Failed to set permissions for ${DATA_DIR}. Manual check might be needed."
fi
echo "--------------------------------------------------"

# Download the specific version
sudo curl -L "https://github.com/docker/compose/releases/download/v2.20.2/docker-compose-linux-x86_64" -o /usr/local/bin/docker-compose

# Make it executable
sudo chmod +x /usr/local/bin/docker-compose

sudo ln -s /usr/local/bin/docker-compose /usr/bin/docker-composedocker-compose

# --- 2. Start Elasticsearch and Wait for it to be Healthy ---
echo "STARTUP: Bringing up ${ELASTICSEARCH_SERVICE_NAME} service..."
docker-compose up -d "${ELASTICSEARCH_SERVICE_NAME}"

echo "STARTUP: Waiting for ${ELASTICSEARCH_SERVICE_NAME} to be healthy..."
max_retries=30 # ~2.5 minutes
count=0
# We will use curl to check health.
until curl -s -k -u "${ELASTIC_USER}:${ELASTIC_PASS}" "http://localhost:9200/_cluster/health?wait_for_status=yellow&timeout=5s" > /dev/null; do
  count=$((count+1))
  if [[ $count -gt $max_retries ]]; then
    echo "ERROR: ${ELASTICSEARCH_SERVICE_NAME} did not become healthy in time. Exiting."
    docker-compose logs "${ELASTICSEARCH_SERVICE_NAME}"
    exit 1
  fi
  # Check if the container is still running
  if ! docker-compose ps "${ELASTICSEARCH_SERVICE_NAME}" | grep -q "Up"; then
    echo "ERROR: ${ELASTICSEARCH_SERVICE_NAME} container is not running. Exiting."
    docker-compose logs "${ELASTICSEARCH_SERVICE_NAME}"
    exit 1
  fi
  echo "Waiting for ${ELASTICSEARCH_SERVICE_NAME} (attempt $count/$max_retries)... sleeping 5s"
  sleep 5
done
echo "STARTUP: ${ELASTICSEARCH_SERVICE_NAME} is healthy."
echo "--------------------------------------------------"


# --- 3. Generate/Re-generate Kibana Service Token ---
echo "TOKEN_GEN: Attempting to generate/re-generate Kibana service token '${KIBANA_TOKEN_NAME_IN_ES}'..."

# First, try to delete the token if it already exists, to ensure we can create it fresh.
# Suppress errors if it doesn't exist using '|| true'.
echo "TOKEN_GEN: Attempting to delete existing token (if any) to ensure clean creation..."
docker exec "${ELASTICSEARCH_SERVICE_NAME}" \
  bin/elasticsearch-service-tokens delete elastic/kibana "${KIBANA_TOKEN_NAME_IN_ES}" > /dev/null 2>&1 || true
echo "TOKEN_GEN: Old token deletion attempt complete (errors ignored if token didn't exist)."

# Now create the new token
echo "TOKEN_GEN: Creating new service token '${KIBANA_TOKEN_NAME_IN_ES}' for service account 'elastic/kibana'..."
RAW_TOKEN_OUTPUT=$(docker exec "${ELASTICSEARCH_SERVICE_NAME}" \
  bin/elasticsearch-service-tokens create elastic/kibana "${KIBANA_TOKEN_NAME_IN_ES}")

# Parse the RAW_TOKEN_OUTPUT to extract the actual token value based on your observed format
# Your format: SERVICE_TOKEN elastic/kibana/kibana-token-test = AAEAAW...
KIBANA_SA_TOKEN=$(echo "${RAW_TOKEN_OUTPUT}" | grep "SERVICE_TOKEN .* =" | awk -F '= ' '{print $2}' | tr -d '[:space:]') 
# Added tr -d '[:space:]' to remove any potential leading/trailing whitespace from awk's output

if [ -z "$KIBANA_SA_TOKEN" ]; then
  echo "ERROR: Failed to generate or parse Kibana token!"
  echo "Raw output from Elasticsearch token creation was:"
  echo "-------------------- RAW OUTPUT START --------------------"
  echo "${RAW_TOKEN_OUTPUT}"
  echo "--------------------  RAW OUTPUT END  --------------------"
  # Try creating one with a different name for full debug output if the first failed
  echo "Attempting one-off token creation for full debug output:"
  docker exec "${ELASTICSEARCH_SERVICE_NAME}" \
    bin/elasticsearch-service-tokens create elastic/kibana "debug-token-$(date +%s)"
  exit 1
else
  echo "TOKEN_GEN: Successfully generated Kibana Token."
  # For debugging the parsed token:
  # echo "Parsed Token Value: [${KIBANA_SA_TOKEN}]"
fi
echo "--------------------------------------------------"


# --- 4. Write Token to .env file (Docker Compose will pick this up) ---
echo "ENV_SETUP: Writing KIBANA_SA_TOKEN to ${ENV_FILE}..."
# This sed command will replace the line if it exists, or add it if it doesn't.
# Using a temporary file for sed -i compatibility across macOS/Linux (BSD vs GNU sed).
if grep -q "^KIBANA_SA_TOKEN=" "${ENV_FILE}" 2>/dev/null; then
  # Variable exists, update it
  sed "s|^KIBANA_SA_TOKEN=.*|KIBANA_SA_TOKEN=${KIBANA_SA_TOKEN}|" "${ENV_FILE}" > "${ENV_FILE}.tmp" && mv "${ENV_FILE}.tmp" "${ENV_FILE}"
  echo "ENV_SETUP: Updated KIBANA_SA_TOKEN in ${ENV_FILE}"
else
  # Variable doesn't exist, append it
  echo "KIBANA_SA_TOKEN=${KIBANA_SA_TOKEN}" >> "${ENV_FILE}"
  echo "ENV_SETUP: Added KIBANA_SA_TOKEN to ${ENV_FILE}"
fi
echo "--------------------------------------------------"


# --- 5. Bring up the rest of the services ---
echo "STARTUP: Bringing up all other services (Kibana will use the token from .env)..."
# Use --remove-orphans to clean up any containers from services no longer in the compose file.
# Use --force-recreate for services like Kibana to ensure they pick up new env vars if they were already created.
# --build will rebuild images if their Dockerfiles or contexts changed.
docker-compose up -d --build --force-recreate --remove-orphans

echo "---------------------------------------------------------------------"
echo "Stack deployment initiated! It might take a few minutes for all services to be fully operational."
echo "KIBANA_SA_TOKEN set to: ${KIBANA_SA_TOKEN}"
echo "Check service status with: docker-compose ps"
echo "View logs with: docker-compose logs -f <service_name> or docker-compose logs -f"
echo "---------------------------------------------------------------------"
echo "Access Points:"
echo "  Kibana UI:        http://localhost:5601"
echo "  Elasticsearch:    http://localhost:9200 (User: ${ELASTIC_USER})"
echo "  NiFi UI:          https://localhost:8443/nifi"
echo "  Spark Master UI:  http://localhost:8081"
echo "---------------------------------------------------------------------"
