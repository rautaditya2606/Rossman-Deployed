import pandas as pd
import numpy as np
import os
import json
import ssl
import base64
import requests
from datetime import datetime
from kafka import KafkaProducer

month2str = {
    1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
    7: 'Jul', 8: 'Aug', 9: 'Sept', 10: 'Oct', 11: 'Nov', 12: 'Dec'
}

# ---------------------------------------------------------------------------
# Kafka producer — initialized once, reused every call.
# _kafka_producer = None  → not yet attempted
# _kafka_producer = False → failed, don't retry (use REST proxy instead)
# ---------------------------------------------------------------------------
_kafka_producer = None

def get_kafka_producer():
    global _kafka_producer
    if _kafka_producer is not None:
        return _kafka_producer or None   # False → None

    try:
        ca_file, cert_file, key_file = "ca.pem", "service.cert", "service.key"
        if os.path.exists(ca_file) and os.path.exists(cert_file) and os.path.exists(key_file):
            print("Kafka: using SSL Client Certificate auth.")
            _kafka_producer = KafkaProducer(
                bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVER_SSL', 'kafka-23493bfd-aditya-fbdc.k.aivencloud.com:22766'),
                security_protocol="SSL",
                ssl_cafile=ca_file,
                ssl_certfile=cert_file,
                ssl_keyfile=key_file,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            return _kafka_producer

        bootstrap_server = os.getenv('KAFKA_BOOTSTRAP_SERVER')
        sasl_user = os.getenv('KAFKA_USER')
        sasl_pass = os.getenv('KAFKA_PASS')
        if all([bootstrap_server, sasl_user, sasl_pass]):
            print("Kafka: trying SASL_SSL auth...")
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            _kafka_producer = KafkaProducer(
                bootstrap_servers=bootstrap_server,
                security_protocol="SASL_SSL",
                sasl_mechanism="PLAIN",
                sasl_plain_username=sasl_user,
                sasl_plain_password=sasl_pass,
                ssl_context=context,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Kafka: SASL_SSL connected.")
            return _kafka_producer

        return None
    except Exception as e:
        print(f"Kafka direct connection failed ({e}), will use REST proxy.")
        _kafka_producer = False   # sentinel — skip retries, fall back to REST
        return None


def _send_via_rest_proxy(topic: str, data: dict) -> bool:
    """Send a message via Aiven Kafka REST Proxy (HTTPS — works from any cloud)."""
    rest_url = os.getenv('KAFKA_REST_PROXY_URL', 'https://kafka-23493bfd-aditya-fbdc.k.aivencloud.com:22768')
    user = os.getenv('KAFKA_USER')
    password = os.getenv('KAFKA_PASS')
    if not user or not password:
        return False
    try:
        encoded = base64.b64encode(json.dumps(data).encode()).decode()
        resp = requests.post(
            f"{rest_url}/topics/{topic}",
            headers={
                "Content-Type": "application/vnd.kafka.binary.v2+json",
                "Accept": "application/vnd.kafka.v2+json",
            },
            json={"records": [{"value": encoded}]},
            auth=(user, password),
            timeout=10,
        )
        resp.raise_for_status()
        return True
    except Exception as e:
        print(f"Kafka REST proxy error: {e}")
        return False


def log_prediction(user_input, prediction, data_source='user'):
    """Stream input features and prediction to Kafka (direct or REST proxy)."""
    log_data = {
        "timestamp": datetime.now().isoformat(),
        "store_id": int(user_input['Store']),
        "date": user_input['Date'],
        "promo": int(user_input['Promo']),
        "state_holiday": user_input['StateHoliday'],
        "school_holiday": int(user_input['SchoolHoliday']),
        "prediction": float(prediction),
        "data_source": data_source,
    }
    topic = os.getenv('KAFKA_TOPIC', 'rossman')

    producer = get_kafka_producer()
    if producer:
        try:
            producer.send(topic, value=log_data).get(timeout=10)
            return {"kafka": True}
        except Exception as e:
            print(f"Kafka send error: {e}")

    # Fallback: REST proxy
    success = _send_via_rest_proxy(topic, log_data)
    return {"kafka": success}


def decode_input(user_input, store_static_dict):
    store_id = int(user_input['Store'])
    store_metadata = store_static_dict[store_id]

    date = pd.to_datetime(user_input['Date'])
    year, month, day = date.year, date.month, date.day
    week_of_year = date.isocalendar()[1]
    day_of_week = date.dayofweek + 1

    comp_year = store_metadata.get('CompetitionOpenSinceYear') or year
    comp_month = store_metadata.get('CompetitionOpenSinceMonth') or month
    competition_open = max(0, 12 * (year - comp_year) + (month - comp_month))

    if store_metadata['Promo2'] == 1 and pd.notnull(store_metadata['Promo2SinceYear']) and pd.notnull(store_metadata['Promo2SinceWeek']):
        promo2_open = 12 * (year - store_metadata['Promo2SinceYear']) + ((week_of_year - store_metadata['Promo2SinceWeek']) * 7 / 30.5)
        promo2_open = max(0, promo2_open)
    else:
        promo2_open = 0

    if store_metadata['PromoInterval'] and store_metadata['Promo2'] == 1:
        promo_months = store_metadata['PromoInterval'].split(',')
        is_promo2_month = int(month2str[month] in promo_months)
    else:
        is_promo2_month = 0

    decoded_input = {
        'Store': store_id,
        'DayOfWeek': day_of_week,
        'Promo': int(user_input['Promo']),
        'StateHoliday': user_input['StateHoliday'],
        'SchoolHoliday': int(user_input['SchoolHoliday']),
        'StoreType': store_metadata['StoreType'],
        'Assortment': store_metadata['Assortment'],
        'CompetitionDistance': store_metadata['CompetitionDistance'],
        'CompetitionOpen': competition_open,
        'Day': day,
        'Month': month,
        'Year': year,
        'WeekOfYear': week_of_year,
        'Promo2': store_metadata['Promo2'],
        'Promo2Open': promo2_open,
        'IsPromo2Month': is_promo2_month
    }

    return pd.DataFrame([decoded_input])
