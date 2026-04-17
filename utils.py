import pandas as pd
import numpy as np
import psycopg2
import os
from datetime import datetime

month2str = {
    1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
    7: 'Jul', 8: 'Aug', 9: 'Sept', 10: 'Oct', 11: 'Nov', 12: 'Dec'
}

def get_db_connection():
    """Establish a connection to the PostgreSQL database."""
    db_url = os.getenv('DATABASE_URL')
    try:
        if db_url:
            # Use fixed URL if provided (standard for Render/Heroku)
            conn = psycopg2.connect(db_url, sslmode='require')
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
    """Log the input features and the resulting prediction to PostgreSQL."""
    conn = get_db_connection()
    if conn is None:
        return
    
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
    except Exception as e:
        print(f"Error logging to database: {e}")
    finally:
        conn.close()

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
