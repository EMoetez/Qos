# Use the official Apache NiFi image as a base
# IMPORTANT: Use the SAME NiFi version that your flow.json.gz was generated from!
# For NiFi 2.x, the image naming might be different, e.g., apache/nifi:2.0.0-M1
# Check Docker Hub for the exact tag for "NiFi 2.3" (which sounds like a milestone release of NiFi 2.0)
ARG NIFI_VERSION=2.3.0 # Example: Replace with your exact NiFi version (e.g., 2.0.0-M1)
FROM apache/nifi:${NIFI_VERSION}

# NiFi runs as user 'nifi' (UID 1000) by default.
# The conf directory will be owned by root initially in the base image.
# The entrypoint script in the official image handles permissions for files it creates,
# but for files we copy, we might need to ensure permissions if NiFi needs to write to them
# (though flow.json.gz is mostly read on startup and then NiFi manages it).

# Copy your flow definition into the conf directory
# The target path inside the container is typically /opt/nifi/nifi-current/conf/
COPY flow.json.gz /opt/nifi/nifi-current/conf/flow.json.gz

# Ensure the nifi user can read/write it.
# The NiFi entrypoint script might also handle chowning the conf dir,
# but being explicit can be safer.
USER root
RUN chown nifi:nifi /opt/nifi/nifi-current/conf/flow.json.gz && \
    chmod 660 /opt/nifi/nifi-current/conf/flow.json.gz
USER nifi 

# The default CMD and ENTRYPOINT from the base image will be used.
# NiFi will start and load the provided flow.json.gz.
CMD ["bin/nifi.sh", "run"]