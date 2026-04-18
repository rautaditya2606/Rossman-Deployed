import random
import time
import requests
import pandas as pd
from datetime import datetime, timedelta

valid_stores = list(range(1, 1116))  # overridden by app.py via set_valid_stores()

def set_valid_stores(stores):
    global valid_stores
    valid_stores = stores

def generate_random_input():
    """Generates a random input following Rossmann data constraints."""
    store = random.choice(valid_stores)
    
    # Generate random date within the last 30 days or next 30 days
    days_offset = random.randint(-30, 30)
    random_date = (datetime.now() + timedelta(days=days_offset)).strftime('%Y-%m-%d')
    
    promo = random.choices([0, 1], weights=[0.6, 0.4])[0]
    state_holiday = random.choices(['0', 'a', 'b', 'c'], weights=[0.9, 0.05, 0.03, 0.02])[0]
    school_holiday = random.choices([0, 1], weights=[0.8, 0.2])[0]
    
    return {
        "Store": str(store),
        "Date": random_date,
        "Promo": str(promo),
        "StateHoliday": state_holiday,
        "SchoolHoliday": str(school_holiday)
    }

def run_simulation(iterations=50, delay=0.5):
    """Sends random data to the API to simulate traffic."""
    url = "http://localhost:8080/api/predictor"
    
    print(f"Starting simulation: {iterations} requests...")
    
    success_count = 0
    for i in range(iterations):
        data = generate_random_input()
        try:
            # We call the local API directly so the data follows the exact app pipeline
            # Note: The app must be running for this to work
            response = requests.post(url, json=data)
            if response.status_code == 200:
                success_count += 1
                print(f"[{i+1}/{iterations}] Store {data['Store']} - Prediction: {response.json()['prediction']:.2f}")
            else:
                print(f"[{i+1}/{iterations}] Error: {response.text}")
        except Exception as e:
            print(f"[{i+1}/{iterations}] Connection failed: {e}")
        
        time.sleep(delay)
    
    print(f"\nSimulation complete! Successfully logged {success_count} synthetic records.")

if __name__ == "__main__":
    # First, we need to make sure the table has the right schema
    import os
    from init_db import init_db
    print("Ensuring database schema is up to date...")
    init_db()
    
    # Ask if user wants to run simulation
    print("\nThis script will generate random store data and send it to your Flask app.")
    print("Make sure your Flask app is running on http://localhost:8080")
    
    run_simulation(iterations=20)
