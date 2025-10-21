from flask import Flask, jsonify, request
import os
from datetime import datetime, timezone

app = Flask(__name__)

@app.get("/")
def index():
    return "Flask Minimal Demo: OK"    

@app.get("/health")
def health():
    return jsonify(status="ok"), 200

@app.get("/api/time")
def time_now():
    now_utc = datetime.now(timezone.utc).isoformat()
    return jsonify(utc=now_utc)    

@app.post("/api/echo")
def echo():
    payload = request.get_json(silent=True) or {}
    return jsonify(received=payload, length=len(str(payload)))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
