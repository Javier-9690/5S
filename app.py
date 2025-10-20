import os
from datetime import datetime, timezone
from flask import Flask, jsonify, request

app = Flask(__name__)

@app.route("/")
def index():
    return (
        "<h1>Flask Minimal Demo</h1>"
        "<p>¡Hola, mundo! 🚀</p>"
        "<p>Endpoints: /health, /api/time, /api/echo</p>"
    )

@app.route("/health")
def health():
    return jsonify(status="ok"), 200

@app.route("/api/time")
def time_now():
    now_utc = datetime.now(timezone.utc).isoformat()
    return jsonify(utc=now_utc)

@app.route("/api/echo", methods=["POST"])
def echo():
    payload = request.get_json(silent=True) or {}
    return jsonify(received=payload, length=len(str(payload)))

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
