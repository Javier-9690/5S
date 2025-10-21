import os
import csv
import io
from datetime import datetime
from flask import (
    Flask, render_template, request, redirect,
    url_for, flash, send_file, jsonify
)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret")

# --- Almacenamiento en memoria (no persistente) ---
REGISTROS = []  # lista de dicts

CAMPOS = [
    "Semana",
    "Fecha",
    "ID",
    "Empresa contratista",
    "Descripción Problema",
    "TIPO DE RIESGO (Psicosocial )",
    "PABELLON",
    "Habitación",
    "Ingresar Contacto",
    "Nombre usuario",
    "Responsable (Toma Requerimiento)",
    "Estatus (Cerrado o Abierto)",
    "Notificación Usuario",
    "Plan de Acción",
    "Fecha de Cierre",
]

ESTATUS_OPCIONES = ["Abierto", "Cerrado"]

@app.get("/health")
def health():
    return jsonify(status="ok"), 200

@app.get("/")
def home():
    # página principal: lista de registros
    return redirect(url_for("listar"))

@app.get("/nuevo")
def nuevo():
    hoy = datetime.now().date().isoformat()
    return render_template(
        "form.html",
        campos=CAMPOS,
        estatus_opts=ESTATUS_OPCIONES,
        hoy=hoy
    )

@app.post("/crear")
def crear():
    # construir el registro a partir del form
    data = {}
    for campo in CAMPOS:
        data[campo] = request.form.get(campo, "").strip()

    # validaciones simples
    obligatorios = ["Semana", "Fecha", "ID", "Estatus (Cerrado o Abierto)"]
    faltantes = [c for c in obligatorios if not data.get(c)]
    if faltantes:
        flash("Faltan campos obligatorios: " + ", ".join(faltantes))
        return redirect(url_for("nuevo"))

    if data["Estatus (Cerrado o Abierto)"] not in ESTATUS_OPCIONES:
        flash("Estatus inválido. Usa: Abierto o Cerrado.")
        return redirect(url_for("nuevo"))

    REGISTROS.append(data)
    flash("Registro agregado correctamente.")
    return redirect(url_for("listar"))

@app.get("/registros")
def listar():
    return render_template("list.html", registros=REGISTROS, campos=CAMPOS, total=len(REGISTROS))

@app.post("/borrar/<int:idx>")
def borrar(idx):
    if 0 <= idx < len(REGISTROS):
        REGISTROS.pop(idx)
        flash("Registro eliminado.")
    else:
        flash("Índice inválido.")
    return redirect(url_for("listar"))

@app.get("/download.csv")
def download_csv():
    if not REGISTROS:
        flash("No hay datos para descargar.")
        return redirect(url_for("listar"))

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CAMPOS)
    writer.writeheader()
    writer.writerows(REGISTROS)
    csv_bytes = io.BytesIO(buf.getvalue().encode("utf-8-sig"))
    return send_file(
        csv_bytes,
        mimetype="text/csv",
        as_attachment=True,
        download_name="registros.csv"
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

