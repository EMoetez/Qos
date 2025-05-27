import time
import requests
import os

KIBANA_URL = "http://kibana:5601"
NDJSON_FILE = "kibana-objects.ndjson"

# Get credentials from environment variables
ELASTIC_USERNAME = os.getenv("ELASTIC_USERNAME", "elastic")
ELASTIC_PASSWORD = os.getenv("ELASTIC_PASSWORD", "elasticelastic123")

def wait_for_kibana():
    max_retries = 60  # Wait up to 5 minutes
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # Try to access Kibana status endpoint
            r = requests.get(
                f"{KIBANA_URL}/api/status",
                auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD),
                timeout=10
            )
            if r.status_code == 200:
                print("Kibana is ready.")
                return True
        except Exception as e:
            print(f"Connection error: {e}")
        
        print(f"Waiting for Kibana... (attempt {retry_count + 1}/{max_retries})")
        time.sleep(5)
        retry_count += 1
    
    print("Kibana did not become ready within the expected time.")
    return False

def import_saved_objects():
    try:
        if not os.path.exists(NDJSON_FILE):
            print(f"Error: {NDJSON_FILE} file not found.")
            return False
            
        with open(NDJSON_FILE, 'rb') as f:
            headers = {
                "kbn-xsrf": "true" #"Content-Type": "application/json"
            }
            
            # Use basic auth for Kibana API
            res = requests.post(
                f"{KIBANA_URL}/api/saved_objects/_import?overwrite=true",
                headers=headers,
                files={"file": f},
                auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD)
            )
            
            if res.status_code == 200:
                print("Import successful!")
                print("Import response:", res.json())
                return True
            else:
                print(f"Import failed with status code: {res.status_code}")
                print("Response:", res.text)
                return False
                
    except Exception as e:
        print(f"Error importing saved objects: {e}")
        return False

if __name__ == "__main__":
    if wait_for_kibana():
        import_saved_objects()
    else:
        print("Failed to connect to Kibana. Exiting.")
