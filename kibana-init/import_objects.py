import time
import requests

KIBANA_URL = "http://kibana:5601"
NDJSON_FILE = "kibana-objects.ndjson"

def wait_for_kibana():
    while True:
        try:
            r = requests.get(f"{KIBANA_URL}/api/status")
            if r.status_code == 200:
                print("Kibana is ready.")
                break
        except:
            pass
        print("Waiting for Kibana...")
        time.sleep(5)

def import_saved_objects():
    with open(NDJSON_FILE, 'rb') as f:
        headers = {"kbn-xsrf": "true"}
        res = requests.post(
            f"{KIBANA_URL}/api/saved_objects/_import?overwrite=true",
            headers=headers,
            files={"file": f}
        )
        print("Import response:", res.status_code, res.text)

if __name__ == "__main__":
    wait_for_kibana()
    import_saved_objects()
