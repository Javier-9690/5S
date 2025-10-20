# Flask Minimal Demo


App mínima para pruebas. Endpoints:
- `/` raíz HTML
- `/health` (GET) → `{ "status": "ok" }`
- `/api/time` (GET) → hora en UTC
- `/api/echo` (POST JSON) → devuelve el payload


## Cómo correr local
```bash
python -m venv .venv
source .venv/bin/activate # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
python app.py
# abre http://localhost:5000
