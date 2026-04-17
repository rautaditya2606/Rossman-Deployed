import time
import json
import base64
import requests
import os
from dotenv import load_dotenv

load_dotenv()

TOPIC_NAME = os.getenv('KAFKA_TOPIC', 'rossman')
REST_PROXY_URL = os.getenv('KAFKA_REST_PROXY_URL', 'https://kafka-23493bfd-aditya-fbdc.k.aivencloud.com:22768')
AUTH = (os.getenv('KAFKA_USER'), os.getenv('KAFKA_PASS'))

HEADERS = {
    "Content-Type": "application/vnd.kafka.binary.v2+json",
    "Accept": "application/vnd.kafka.v2+json",
}

def send_message(payload: dict) -> bool:
    encoded = base64.b64encode(json.dumps(payload).encode()).decode()
    body = {"records": [{"value": encoded}]}
    resp = requests.post(
        f"{REST_PROXY_URL}/topics/{TOPIC_NAME}",
        headers=HEADERS,
        json=body,
        auth=AUTH,
    )
    resp.raise_for_status()
    return resp.json().get("offsets", [{}])[0].get("error_code") is None

for i in range(10):
    msg = {"message": f"Hello from REST Proxy {i + 1}", "timestamp": time.ctime()}
    ok = send_message(msg)
    print(f"[{'OK' if ok else 'FAIL'}] Sent: {msg}")
    time.sleep(1)

print("Done.")
