import pandas as pd
import numpy as np
import psycopg2
import os
import json
import ssl
from datetime import datetime
from kafka import KafkaProducer

month2str = {
    1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
    7: 'Jul', 8: 'Aug', 9: 'Sept', 10: 'Oct', 11: 'Nov', 12: 'Dec'
}

def get_kafka_producer():
    """Initialize a Kafka producer with either SASL_SSL or SSL (Client Cert) authentication."""
    try:
        # 1. Try SSL Client Certificate first if files exist
        ca_file = "ca.pem"
        cert_file = "service.cert"
        key_file = "service.key"
        
        if os.path.exists(ca_file) and os.path.exists(cert_file) and os.path.exists(key_file):
            print("Using Kafka SSL Client Certificate authentication...")
            bootstrap_server = os.getenv('KAFKA_BOOTSTRAP_SERVER_SSL', 'kafka-23493bfd-aditya-fbdc.k.aivencloud.com:22766')
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_server,
                security_protocol="SSL",
                ssl_cafile=ca_file,
                ssl_certfile=cert_file,
                ssl_keyfile=key_file,
                api_version=(0, 10, 2),
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            return producer

        # 2. Fallback to SASL_SSL if environment variables are set
        bootstrap_server = os.getenv('KAFKA_BOOTSTRAP_SERVER')
        sasl_user = os.getenv('KAFKA_USER')
        sasl_pass = os.getenv('KAFKA_PASS')
        
        if all([bootstrap_server, sasl_user, sasl_pass]):
            print("Using Kafka SASL_SSL authentication...")
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE

            producer = KafkaProducer(
                bootstrap_servers=bootstrap_server,
                security_protocol="SASL_SSL",
                sasl_mechanism="PLAIN",
                sasl_plain_username=sasl_user,
                sasl_plain_password=sasl_pass,
                ssl_context=context,
                api_version=(0, 10, 2),
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            return producer
            
        print("No Kafka authentication configuration found.")
        return None
    except Exception as e:
        print(f"Error creating Kafka producer: {e}")
        return None

def get_db_connection():
    """Establish a connection to the PostgreSQL database."""
    db_url = os.getenv('DATABASE_URL')
    try:
        if db_url:
            # Use fixed URL if provided (standard for Render/Heroku)
            conn = psycopg2.connect(db_url)
        else:
            # Fallback to individual parameters (local development)
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                database=os.getenv('DB_NAME', 'rossman_db'),
                user=os.getenv('DB_USER', 'postgres'),
                password=os.getenv('DB_PASS', 'password')
            )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def log_prediction(user_input, prediction, data_source='user'):
    """Log the input features and the resulting prediction to PostgreSQL and Kafka."""
    postgres_success = False
    kafka_success = False

    # 1. Log to PostgreSQL
    conn = get_db_connection()
    if conn:
        table_name = os.getenv('TABLE_NAME', 'rossman_deployed')
        try:
            with conn.cursor() as cur:
                insert_query = f"""
                    INSERT INTO {table_name} (
                        store_id, date, promo, state_holiday, school_holiday, prediction, data_source
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                cur.execute(insert_query, (
                    int(user_input['Store']),
                    user_input['Date'],
                    int(user_input['Promo']),
                    user_input['StateHoliday'],
                    int(user_input['SchoolHoliday']),
                    float(prediction),
                    data_source
                ))
                conn.commit()
                postgres_success = True
        except Exception as e:
            print(f"Error logging to PostgreSQL: {e}")
        finally:
            conn.close()

    # 2. Log to Kafka
    producer = get_kafka_producer()
    if producer:
        try:
            topic_name = os.getenv('KAFKA_TOPIC', 'rossman_logs')
            log_data = {
                "timestamp": datetime.now().isoformat(),
                "store_id": int(user_input['Store']),
                "date": user_input['Date'],
                "promo": int(user_input['Promo']),
                "state_holiday": user_input['StateHoliday'],
                "school_holiday": int(user_input['SchoolHoliday']),
                "prediction": float(prediction),
                "data_source": data_source
            }
            future = producer.send(topic_name, value=log_data)
            future.get(timeout=10) # Block to ensure it's sent
            producer.flush()
            kafka_success = True
        except Exception as e:
            print(f"Error logging to Kafka: {e}")
        finally:
            producer.close()
    
    return {"postgres": postgres_success, "kafka": kafka_success}

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
