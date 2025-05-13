#!/bin/bash

set -e

# Install dependencies
sudo apt update
sudo apt install -y openjdk-21-jdk curl wget unzip

# Set JAVA_HOME
echo "export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64" >> ~/.bashrc
source ~/.bashrc

# Download and install NiFi 2.3
wget https://downloads.apache.org/nifi/2.3.0/nifi-2.3.0-bin.zip
unzip nifi-2.3.0-bin.zip
sudo mv nifi-2.3.0 /opt/nifi
sudo chown -R $USER:$USER /opt/nifi

# Start NiFi
/opt/nifi/bin/nifi.sh start

