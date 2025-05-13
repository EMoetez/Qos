#!/bin/bash

set -e

###############################################################################
# 🔧 Full NiFi + Spark Pipeline Setup Script
# Supports: Java 21, NiFi 2.3.0 (HTTPS on 8443), Spark 3.5.5, flow.json.gz
# Author: You
###############################################################################

# === CONFIGURATION ===
NIFI_VERSION="2.3.0"
SPARK_VERSION="3.5.5"
INSTALL_DIR="/opt"
NIFI_DIR="$INSTALL_DIR/nifi-$NIFI_VERSION"
SPARK_DIR="$INSTALL_DIR/spark"
DATA_ROOT="/data-ingestion"
USERNAME="admin"
PASSWORD="adminpassword"
SPARK_SERVICE_SCRIPT="/opt/spark_service/spark_service.py"
SPARK_SERVICE_LOG="/tmp/spark_service.log"

echo "📦 Updating and installing dependencies..."
sudo apt update
sudo apt install -y openjdk-21-jdk curl wget unzip jq python3 python3-pip

echo "✅ Java 21 installed. Setting JAVA_HOME..."
echo "export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64" >> ~/.bashrc
echo "export PATH=\$PATH:\$JAVA_HOME/bin" >> ~/.bashrc
source ~/.bashrc

# === Step 1: Install Apache NiFi ===
echo "⬇️ Downloading and installing Apache NiFi $NIFI_VERSION..."
wget -q https://downloads.apache.org/nifi/${NIFI_VERSION}/nifi-${NIFI_VERSION}-bin.zip
unzip -q nifi-${NIFI_VERSION}-bin.zip
sudo mv nifi-${NIFI_VERSION} $NIFI_DIR
sudo chown -R $USER:$USER $NIFI_DIR

echo "📂 Copying your custom NiFi flow.json.gz..."
cp flow.json.gz $NIFI_DIR/conf/flow.json.gz   # I need to change the directory where the flow is copied from

# === Step 2: Install Apache Spark ===
echo "⬇️ Downloading and installing Apache Spark $SPARK_VERSION..."
wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz
tar xf spark-${SPARK_VERSION}-bin-hadoop3.tgz
sudo mv spark-${SPARK_VERSION}-bin-hadoop3 $SPARK_DIR
echo "export SPARK_HOME=$SPARK_DIR" >> ~/.bashrc
echo "export PATH=\$PATH:\$SPARK_HOME/bin" >> ~/.bashrc
source ~/.bashrc

# === Step 3: Create Data Ingestion Folder Structure ===
echo "📁 Creating data-ingestion folder structure..."
mkdir -p $DATA_ROOT/staging $DATA_ROOT/processed $DATA_ROOT/archive
sudo chown -R $USER:$USER $DATA_ROOT

# === Step 4: Copy Spark Service Script ===
echo "📜 Copying Spark service script..."
mkdir -p /opt/spark_service
cp spark_service.py $SPARK_SERVICE_SCRIPT
chmod +x $SPARK_SERVICE_SCRIPT

# === Step 5: Start NiFi ===
echo "🚀 Starting NiFi..."
$NIFI_DIR/bin/nifi.sh start

echo "⏳ Waiting 30 seconds for NiFi to initialize (HTTPS mode)..."
sleep 30

# === Step 6: Authenticate to NiFi and Get Access Token ===
echo "🔐 Authenticating to NiFi over HTTPS (port 8443)..."
TOKEN=$(curl -s -k -X POST \
  -d "username=${USERNAME}&password=${PASSWORD}" \
  https://localhost:8443/nifi-api/access/token)

if [ -z "$TOKEN" ]; then
  echo "❌ Failed to get NiFi access token. Check username/password or NiFi startup logs."
  exit 1
fi
echo "✅ NiFi token acquired."

# === Step 7: Get Root Process Group ID ===
echo "🔁 Fetching root process group ID..."
ROOT_PG_ID=$(curl -s -k -H "Authorization: Bearer $TOKEN" \
  https://localhost:8443/nifi-api/flow/process-groups/root | jq -r '.processGroupFlow.id')

if [ -z "$ROOT_PG_ID" ] || [ "$ROOT_PG_ID" = "null" ]; then
  echo "❌ Could not retrieve root PG ID. Is NiFi fully initialized?"
  exit 1
fi
echo "✅ Root Process Group ID: $ROOT_PG_ID"

# === Step 8: Start All Processors ===
echo "🚀 Starting all processors..."
curl -s -X PUT -k -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"id\": \"$ROOT_PG_ID\", \"state\": \"RUNNING\"}" \
  https://localhost:8443/nifi-api/flow/process-groups/$ROOT_PG_ID
echo "✅ All processors started."

# === Step 9: Start Spark Service in Background ===
echo "🔥 Starting Spark service used by NiFi..."
pkill -f "$SPARK_SERVICE_SCRIPT" || true
nohup python3 "$SPARK_SERVICE_SCRIPT" > "$SPARK_SERVICE_LOG" 2>&1 &

echo "✅ Spark service started and listening for NiFi requests."

# === Done ===
echo "🎉 Pipeline is fully up and running!"
echo "🌐 Visit NiFi UI at: https://localhost:8443/nifi"
echo "📂 Data ingestion folder: $DATA_ROOT/staging"

