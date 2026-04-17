import time
import json
import ssl
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

TOPIC_NAME = os.getenv('KAFKA_TOPIC', 'rossman')
BOOTSTRAP_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVER', 'kafka-23493bfd-aditya-fbdc.k.aivencloud.com:22769')
SASL_USER = os.getenv('KAFKA_USER')
SASL_PASS = os.getenv('KAFKA_PASS')

print(f"Initializing Kafka Consumer (SASL_SSL) to {BOOTSTRAP_SERVER}...")

context = ssl.create_default_context()
context.check_hostname = False
context.verify_mode = ssl.CERT_NONE

try:
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVER,
        client_id="ROSSMAN_CLIENT",
        group_id="ROSSMAN_GROUP",
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username=SASL_USER,
        sasl_plain_password=SASL_PASS,
        ssl_context=context,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    print("Consumer initialized. Waiting for messages (Ctrl+C to stop)...")

    while True:
        messages = consumer.poll(timeout_ms=5000)
        if not messages:
            print("No new messages found in the last 5 seconds...")
            continue
        for tp, msgs in messages.items():
            for message in msgs:
                print(f"Received from {message.value.get('data_source', 'unknown')}: {message.value}")

except KeyboardInterrupt:
    print("\nStopping consumer...")
except Exception as e:
    print(f"Error in Kafka Consumer: {e}")
finally:
    if 'consumer' in locals():
        consumer.close()
