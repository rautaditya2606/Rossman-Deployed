import json
import base64
import time
import requests
import atexit
import os
from dotenv import load_dotenv

load_dotenv()

TOPIC_NAME = os.getenv('KAFKA_TOPIC', 'rossman')
REST_PROXY_URL = os.getenv('KAFKA_REST_PROXY_URL', 'https://kafka-23493bfd-aditya-fbdc.k.aivencloud.com:22768')
AUTH = (os.getenv('KAFKA_USER'), os.getenv('KAFKA_PASS'))
GROUP_ID = "rossman_rest_group"
INSTANCE_ID = "rossman_consumer_1"

CONSUMER_HEADERS = {
    "Content-Type": "application/vnd.kafka.v2+json",
    "Accept": "application/vnd.kafka.v2+json",
}
FETCH_HEADERS = {
    "Accept": "application/vnd.kafka.binary.v2+json",
}

BASE = f"{REST_PROXY_URL}/consumers/{GROUP_ID}/instances/{INSTANCE_ID}"


def delete_consumer():
    try:
        requests.delete(BASE, headers=CONSUMER_HEADERS, auth=AUTH, timeout=5)
        print("Consumer instance deleted.")
    except Exception:
        pass

atexit.register(delete_consumer)

# 1. Create consumer instance (ignore 409 if it already exists)
resp = requests.post(
    f"{REST_PROXY_URL}/consumers/{GROUP_ID}",
    headers=CONSUMER_HEADERS,
    json={"name": INSTANCE_ID, "format": "binary", "auto.offset.reset": "earliest"},
    auth=AUTH,
)
if resp.status_code not in (200, 409):
    resp.raise_for_status()
print("Consumer instance ready.")

# 2. Subscribe to topic
resp = requests.post(
    f"{BASE}/subscription",
    headers=CONSUMER_HEADERS,
    json={"topics": [TOPIC_NAME]},
    auth=AUTH,
)
resp.raise_for_status()
print(f"Subscribed to '{TOPIC_NAME}'. Polling (Ctrl+C to stop)...\n")

# 3. Poll loop
try:
    while True:
        resp = requests.get(
            f"{BASE}/records",
            headers=FETCH_HEADERS,
            params={"timeout": 5000, "max_bytes": 1048576},
            auth=AUTH,
        )
        resp.raise_for_status()
        records = resp.json()
        if not records:
            print("No new messages...")
        for rec in records:
            value = json.loads(base64.b64decode(rec["value"]).decode())
            print(f"[partition={rec['partition']} offset={rec['offset']}] {value}")
        time.sleep(1)
except KeyboardInterrupt:
    print("\nStopping.")
