import argparse
from flask import Flask, Blueprint, jsonify, request
from flask_cors import CORS
from waitress import serve

# 定義 CMD 參數
parser = argparse.ArgumentParser()
parser.add_argument("--mode", type = str, help = "development or production mode")
args = parser.parse_args()

# 定義應用程式與 API
app = Flask(__name__)
CORS(app)
apiBp = Blueprint("api", __name__)