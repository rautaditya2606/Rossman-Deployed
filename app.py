from flask import Flask, render_template, request, jsonify
import joblib
from utils import decode_input

app = Flask(__name__)

model = joblib.load('model/xgb_pipeline.joblib')
scaler = joblib.load('model/scaler.joblib')
encoder = joblib.load('model/encoder.joblib')
store_static_dict = joblib.load('model/store_static_dict.joblib')

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
        return jsonify({'prediction': prediction})
    except Exception as e:
        return jsonify({'error': str(e)}), 400

    


if __name__ == '__main__':
    app.run(debug=True, port=8080, host='0.0.0.0')
    app.config['TEMPLATES_AUTO_RELOAD'] = True
