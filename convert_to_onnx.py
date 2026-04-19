"""
Run once locally to convert xgb_model.ubj → xgb_model.onnx.
Commit the .onnx file to git; Render uses onnxruntime (no xgboost needed).

Usage:
    pip install onnxmltools onnxconverter-common
    python convert_to_onnx.py
"""
import xgboost as xgb
from onnxmltools import convert_xgboost
from onnxconverter_common.data_types import FloatTensorType

# Must match training: 12 numeric + 7+4+4+3 OHE = 30 total features
N_FEATURES = 30

booster = xgb.Booster()
booster.load_model('model/xgb_model.ubj')

# onnxmltools requires feature names in 'f%d' format
n = len(booster.feature_names) if booster.feature_names else N_FEATURES
booster.feature_names = [f'f{i}' for i in range(n)]

initial_types = [('float_input', FloatTensorType([None, n]))]
onnx_model = convert_xgboost(booster, initial_types=initial_types)

with open('model/xgb_model.onnx', 'wb') as f:
    f.write(onnx_model.SerializeToString())

print("Saved model/xgb_model.onnx")
print("Commit it, then redeploy — no xgboost needed on Render.")
