import os
import io
import re
import unicodedata
import pandas as pd
from datetime import datetime, timezone
from flask import (
    Flask, jsonify, request, render_template,
    redirect, url_for, flash, send_file
)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret")  # para mensajes flash

# --- Normalización robusta de nombres de columnas --------------------------------
def normalize(s: str) -> str:
    if s is None:
        return ""
    # quita acentos
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    # minúsculas, quita espacios extra
    s = s.lower().strip()
    s = re.sub(r"\s+", " ", s)
    return s

EXPECTED_COLS = [
    "semana",
    "fecha",
    "id",
    "empresa contratista",
    "descripcion problema",
    "tipo de riesgo (psicosocial)",
    "pabellon",
    "habitacion",
    "ingresar contacto",
    "nombre usuario",
    "responsable (toma requerimiento)",
    "estatus (cerrado o abierto)",
    "notificacion usuario",
    "plan de accion",
    "fecha de cierre",
]
EXPECTED_COLS_N = [normalize(c) for c in EXPECTED_COLS]

# memoria simple para demo
DATAFRAME_CACHE = None

# --- Rutas -----------------------------------------------------------------------
@app.get("/")
def index():
    return render_template("upload.html")

@app.get("/health")
def health():
    return jsonify(status="ok"), 200

@app.post("/upload")
def upload():
    global DATAFRAME_CACHE

    if "file" not in request.files or request.files["file"].filename == "":
        flash("Sube un archivo Excel (.xlsx o .xlsb).")
        return redirect(url_for("index"))

    f = request.files["file"]
    filename = f.filename.lower()

    try:
        if filename.endswith(".xlsx"):
            df = pd.read_excel(f, engine="openpyxl")
        else:
            flash("Formato no soportado. Usa .xlsx"
            return redirect(url_for("index"))
    except Exception as e:
        flash(f"Error leyendo el Excel: {e}")
        return redirect(url_for("index"))

    # validar columnas
    original_cols = list(df.columns)
    norm_map = {col: normalize(str(col)) for col in original_cols}
    have = set(norm_map.values())
    missing = [c for c in EXPECTED_COLS_N if c not in have]

    if missing:
        # para ayudar, muestra cuáles te faltan y cómo se llaman en tu archivo
        flash(
            "Columnas faltantes: "
            + ", ".join(missing)
            + ".\nColumnas encontradas: "
            + ", ".join(original_cols)
        )
        return redirect(url_for("index"))

    # reordenar/renombrar a los nombres “bonitos” de EXPECTED_COLS
    reverse_lookup = {v: k for k, v in norm_map.items()}  # normalizado -> original
    ordered_original = [reverse_lookup[c] for c in EXPECTED_COLS_N]
    df = df[ordered_original]
    df.columns = EXPECTED_COLS  # renombra con acentos y mayúsculas como pediste

    DATAFRAME_CACHE = df  # guarda en memoria
    rows = min(len(df), 200)  # muestra primeras 200 filas en la tabla
    return render_template("data.html", columns=list(df.columns), rows=df.head(rows).values.tolist(), total=len(df))

@app.get("/download.csv")
def download_csv():
    if DATAFRAME_CACHE is None:
        flash("Primero sube un Excel.")
        return redirect(url_for("index"))

    out = io.StringIO()
    DATAFRAME_CACHE.to_csv(out, index=False)
    out.seek(0)
    return send_file(
        io.BytesIO(out.getvalue().encode("utf-8-sig")),
        mimetype="text/csv",
        as_attachment=True,
        download_name="reporte.csv",
    )

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
