from flask import Flask, render_template, request, jsonify
from flask_apscheduler import APScheduler
import joblib
from utils import decode_input, log_prediction
from generate_synthetic_data import generate_random_input
from dotenv import load_dotenv
import os

load_dotenv()

app = Flask(__name__)
scheduler = APScheduler()

# Avoid running scheduler multiple times (once for gunicorn worker)
if not scheduler.running:
    # Initialize and start scheduler for Production (Gunicorn) or local direct script
    if not app.debug or os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
        scheduler.init_app(app)
        scheduler.start()

model = joblib.load('model/xgb_pipeline.joblib')
scaler = joblib.load('model/scaler.joblib')
encoder = joblib.load('model/encoder.joblib')
store_static_dict = joblib.load('model/store_static_dict.joblib')

@scheduler.task('interval', id='do_job_1', minutes=1)
def scheduled_prediction_job():
    """Background task to generate synthetic data and log it."""
    with app.app_context():
        try:
            # 1. Generate random input
            data = generate_random_input()
            
            # 2. Process data exactly like real requests
            decoded_df = decode_input(data, store_static_dict)
            decoded_df[numeric_cols] = scaler.transform(decoded_df[numeric_cols])
            decoded_df[encoded_cols] = encoder.transform(decoded_df[categorical_cols])
            x_decoded = decoded_df[numeric_cols + encoded_cols]
            
            # 3. Predict & Log
            prediction = float(model.predict(x_decoded)[0])
            log_prediction(data, prediction, data_source='synthetic')
            
            print(f"DEBUG: Auto-logged synthetic prediction for Store {data['Store']}")
        except Exception as e:
            print(f"DEBUG: Failed to run background job: {e}")

# These must match the order during training
numeric_cols = ['Store','Promo', 'SchoolHoliday', 'CompetitionDistance', 'CompetitionOpen', 'Promo2', 'Promo2Open', 'IsPromo2Month', 'Day','Month', 'Year', 'WeekOfYear']
categorical_cols = ['DayOfWeek', 'StateHoliday', 'StoreType', 'Assortment']
encoded_cols = encoder.get_feature_names_out().tolist()

@app.route('/', methods=['GET', 'POST'])
def index():
    prediction = None
    if request.method == 'POST':
        user_input = {
            'Store': request.form['Store'],
            'Date': request.form['Date'],
            'Promo': request.form['Promo'],
            'StateHoliday': request.form['StateHoliday'],
            'SchoolHoliday': request.form['SchoolHoliday']
        }

        decoded_df = decode_input(user_input, store_static_dict)
        decoded_df[numeric_cols] = scaler.transform(decoded_df[numeric_cols])
        decoded_df[encoded_cols] = encoder.transform(decoded_df[categorical_cols])
        x_decoded = decoded_df[numeric_cols + encoded_cols]
        prediction = model.predict(x_decoded)[0]
        
        # Log to PostgreSQL
        log_prediction(user_input, prediction)

    return render_template('index.html', prediction=prediction)


@app.route('/api/predictor', methods=['POST'])
def api_predictor():
    try:
        data = request.get_json(force=True)
        decoded_df = decode_input(data, store_static_dict)
        decoded_df[numeric_cols] = scaler.transform(decoded_df[numeric_cols])
        decoded_df[encoded_cols] = encoder.transform(decoded_df[categorical_cols])
        x_decoded = decoded_df[numeric_cols + encoded_cols]
        prediction = float(model.predict(x_decoded)[0])
        
        # Log to PostgreSQL
        log_prediction(data, prediction)
        
        return jsonify({'prediction': prediction})
    except Exception as e:
        return jsonify({'error': str(e)}), 400

    


if __name__ == '__main__':
    # Scheduler is already initialized and started in top-level app context
    # for production/reloader scenarios above if needed.
    
    app.run(debug=True, port=8080, host='0.0.0.0', use_reloader=False)
    app.config['TEMPLATES_AUTO_RELOAD'] = True
