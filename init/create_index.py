import requests
import time

ELASTIC_URL = "http://elasticsearch:9200"
INDEX_NAME = "network-data"

# Define mapping
mapping = {
    "mappings": {
        "properties": {
            "location": {"type": "geo_point"},
            "Date": {"type": "date"}
        }
    }
}

def wait_for_elasticsearch():
    while True:
        try:
            r = requests.get(ELASTIC_URL)
            if r.status_code == 200:
                print("Elasticsearch is up!")
                break
        except Exception:
            pass
        print("Waiting for Elasticsearch...")
        time.sleep(5)

def create_index_if_not_exists():
    res = requests.head(f"{ELASTIC_URL}/{INDEX_NAME}")
    if res.status_code == 404:
        print(f"Index '{INDEX_NAME}' does not exist. Creating...")
        r = requests.put(f"{ELASTIC_URL}/{INDEX_NAME}", json=mapping)
        print("Response:", r.json())
    else:
        print(f"Index '{INDEX_NAME}' already exists. Skipping creation.")

if __name__ == "__main__":
    wait_for_elasticsearch()
    create_index_if_not_exists()
