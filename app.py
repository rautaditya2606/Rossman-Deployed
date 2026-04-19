from flask import Flask, render_template, request, jsonify
from flask_apscheduler import APScheduler
from utils import decode_input, log_prediction, build_feature_vector
from generate_synthetic_data import generate_random_input, set_valid_stores
from dotenv import load_dotenv
import onnxruntime as ort
import joblib
import json
import os

load_dotenv()

app = Flask(__name__)
scheduler = APScheduler()

if not scheduler.running:
    if not app.debug or os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
        scheduler.init_app(app)
        scheduler.start()

with open('model/scaler_params.json') as f:
    scaler_params = json.load(f)
with open('model/encoder_categories.json') as f:
    encoder_categories = json.load(f)

ort_session = ort.InferenceSession('model/xgb_model.onnx',
                                   providers=['CPUExecutionProvider'])
_ort_input_name = ort_session.get_inputs()[0].name
print("Loaded ONNX model.")

store_static_dict = joblib.load('model/store_static_dict.joblib')
set_valid_stores(list(store_static_dict.keys()))

# Must match the order during training
numeric_cols     = ['Store', 'Promo', 'SchoolHoliday', 'CompetitionDistance', 'CompetitionOpen',
                    'Promo2', 'Promo2Open', 'IsPromo2Month', 'Day', 'Month', 'Year', 'WeekOfYear']
categorical_cols = ['DayOfWeek', 'StateHoliday', 'StoreType', 'Assortment']


def predict(decoded: dict) -> float:
    x, _ = build_feature_vector(decoded, numeric_cols, categorical_cols,
                                scaler_params, encoder_categories)
    result = ort_session.run(None, {_ort_input_name: x})[0]
    return max(0.0, float(result.flat[0]))


@scheduler.task('interval', id='do_job_1', seconds=1, max_instances=1)
def scheduled_prediction_job():
    with app.app_context():
        try:
            data = generate_random_input()
            decoded = decode_input(data, store_static_dict)
            prediction = predict(decoded)
            log_prediction(data, prediction, data_source='synthetic')
            print(f"Streamed synthetic prediction → Store {data['Store']} | pred={prediction:.2f}")
        except Exception as e:
            print(f"Scheduler error: {e}")


@app.route('/', methods=['GET', 'POST'])
def index():
    prediction = None
    log_status = None
    if request.method == 'POST':
        user_input = {
            'Store':         request.form['Store'],
            'Date':          request.form['Date'],
            'Promo':         request.form['Promo'],
            'StateHoliday':  request.form['StateHoliday'],
            'SchoolHoliday': request.form['SchoolHoliday'],
        }
        decoded = decode_input(user_input, store_static_dict)
        prediction = predict(decoded)
        log_status = log_prediction(user_input, prediction)

    return render_template('index.html', prediction=prediction, log_status=log_status)


@app.route('/api/predictor', methods=['POST'])
def api_predictor():
    try:
        data = request.get_json(force=True)
        decoded = decode_input(data, store_static_dict)
        prediction = predict(decoded)
        log_prediction(data, prediction)
        return jsonify({'prediction': prediction})
    except Exception as e:
        return jsonify({'error': str(e)}), 400



@app.route('/stats')
def stats():
    try:
        import requests as req
        rest_url = os.getenv('KAFKA_REST_PROXY_URL', 'https://kafka-23493bfd-aditya-fbdc.k.aivencloud.com:22768')
        user     = os.getenv('KAFKA_USER', '')
        password = os.getenv('KAFKA_PASS', '')
        topic    = os.getenv('KAFKA_TOPIC', 'rossman')

        parts_resp = req.get(
            f"{rest_url}/topics/{topic}/partitions",
            headers={"Accept": "application/vnd.kafka.v2+json"},
            auth=(user, password),
            timeout=8,
        )
        parts_resp.raise_for_status()
        partition_ids = [p['partition'] for p in parts_resp.json()]

        total = 0
        offsets = []
        for pid in partition_ids:
            r = req.get(
                f"{rest_url}/topics/{topic}/partitions/{pid}/offsets",
                headers={"Accept": "application/vnd.kafka.v2+json"},
                auth=(user, password),
                timeout=8,
            )
            r.raise_for_status()
            o = r.json()
            total += o.get('end_offset', 0)
            offsets.append({"partition": pid, **o})

        return jsonify({"topic": topic, "total_messages_logged": total, "partitions": offsets})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, port=8080, host='0.0.0.0', use_reloader=False)
    app.config['TEMPLATES_AUTO_RELOAD'] = True
