# On the host (adjust if using Docker Desktop):
#After finishing the setup of the pipeline.
docker cp nifi:/opt/nifi/nifi-current/conf/flow.json.gz ./nifi-config/flow.json.gz





# This command returns the bearer token needed for authenticating with nifi 
curl -k -X POST https://localhost:8443/nifi-api/access/token \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'username=your_username&password=your_password'

# this command starts a processor, change only the processor id and the bearer token 
curl -k -X PUT https://localhost:8443/nifi-api/processors/4547e9c7-0196-1000-aa8f-1a0d32d94944/run-status \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <your-token-if-needed>' \
  -d '{
        "revision": {
          "clientId": "4a76dcff-93f8-4e71-a7c8-eb335eb0b84b",
          "version": 5
        },
        "state": "RUNNING"
      }'

clientId="99b403c7-d6e6-4477-ad8b-84fecf136fa8"
version=1



#Get token for Kibana
# This command returns the bearer token needed for authenticating with Kibana
docker exec -it elasticsearch bash -c "bin/elasticsearch-service-tokens create elastic/kibana kibana-token"
# export the token to use it in the next command
export KIBANA_TOKEN=$(docker exec -it elasticsearch bash -c "bin/elasticsearch-service-tokens create elastic/kibana kibana-token" | grep -oP '(?<=token":")[^"]*')
AAEAAWVsYXN0aWMva2liYW5hL2tpYmFuYS10b2tlbjpzVjdpeEg4NVNTdUl2MWt2NTNTb0VR
AAEAAWVsYXN0aWMva2liYW5hL2tpYmFuYS10b2tlbjpZcl9HYlRRLVM0Q2RRc0ZQeW13TFdR