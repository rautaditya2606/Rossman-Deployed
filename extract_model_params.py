"""
Run this once locally to convert sklearn joblib files to lightweight numpy/json/xgb format.
Output files go into model/ and are committed to git.
After running, pandas + scikit-learn + scipy are no longer needed at inference time.

Usage:
    python extract_model_params.py
"""
import joblib
import numpy as np
import json

print("Loading joblib files...")
scaler  = joblib.load('model/scaler.joblib')
encoder = joblib.load('model/encoder.joblib')
model   = joblib.load('model/xgb_pipeline.joblib')

# --- scaler: save params (supports MinMaxScaler and StandardScaler) ---
scaler_params = {'type': type(scaler).__name__}
if hasattr(scaler, 'mean_'):   # StandardScaler
    scaler_params['mean_']  = scaler.mean_.tolist()
    scaler_params['scale_'] = scaler.scale_.tolist()
else:                          # MinMaxScaler
    scaler_params['min_']   = scaler.min_.tolist()
    scaler_params['scale_'] = scaler.scale_.tolist()
with open('model/scaler_params.json', 'w') as f:
    json.dump(scaler_params, f)
print(f"Scaler saved  — type={scaler_params['type']}, {len(scaler_params['scale_'])} features")

# --- encoder: save categories per column ---
cats = {
    col: arr.tolist()
    for col, arr in zip(encoder.feature_names_in_, encoder.categories_)
}
with open('model/encoder_categories.json', 'w') as f:
    json.dump(cats, f)
print(f"Encoder saved — columns: {list(cats.keys())}")

# --- xgboost booster: save in native binary format ---
# Works whether model is a raw XGBRegressor or a sklearn Pipeline
booster = None
if hasattr(model, 'get_booster'):
    booster = model.get_booster()
elif hasattr(model, 'steps'):
    for _, step in model.steps:
        if hasattr(step, 'get_booster'):
            booster = step.get_booster()
            break
if booster is None:
    raise RuntimeError("Could not extract XGBoost booster from model. Check model structure.")

booster.save_model('model/xgb_model.ubj')
print("XGBoost booster saved → model/xgb_model.ubj")

print("\nDone. Commit the new model/ files and redeploy.")
print("You can now remove pandas, scikit-learn, scipy, joblib from requirements.txt")
