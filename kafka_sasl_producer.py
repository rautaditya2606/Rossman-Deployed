import time
import json
import ssl
import os
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

TOPIC_NAME = os.getenv('KAFKA_TOPIC', 'rossman')
BOOTSTRAP_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVER', 'kafka-23493bfd-aditya-fbdc.k.aivencloud.com:22769')
SASL_USER = os.getenv('KAFKA_USER')
SASL_PASS = os.getenv('KAFKA_PASS')

print(f"Initializing Kafka Producer (SASL_SSL) to {BOOTSTRAP_SERVER}...")

context = ssl.create_default_context()
context.check_hostname = False
context.verify_mode = ssl.CERT_NONE

try:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVER,
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username=SASL_USER,
        sasl_plain_password=SASL_PASS,
        ssl_context=context,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print("Producer initialized successfully.")

    for i in range(5):
        data = {
            "message": f"Hello from Rossman MLOps {i + 1}",
            "timestamp": time.ctime(),
            "source": "sasl_test"
        }
        producer.send(TOPIC_NAME, value=data)
        print(f"Sent: {data}")
        time.sleep(1)

    producer.flush()
    producer.close()
    print("Test complete.")

except Exception as e:
    print(f"Error: {e}")
