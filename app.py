import os
import csv
import io
import re
from datetime import datetime, date, timedelta
from flask import (
    Flask, render_template, request, redirect,
    url_for, flash, send_file, jsonify
)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret")

# --- Mapa de semanas: {numero: (inicio, fin)} en ISO (YYYY-MM-DD) ---------------
WEEK_MAP = {
    42: ("2025-10-13", "2025-10-19"),
    43: ("2025-10-20", "2025-10-26"),
    44: ("2025-10-27", "2025-11-02"),
    45: ("2025-11-03", "2025-11-09"),
    46: ("2025-11-10", "2025-11-16"),
    47: ("2025-11-17", "2025-11-23"),
    48: ("2025-11-24", "2025-11-30"),
    49: ("2025-12-01", "2025-12-07"),
    50: ("2025-12-08", "2025-12-14"),
    51: ("2025-12-15", "2025-12-21"),
    52: ("2025-12-22", "2025-12-28"),
    53: ("2025-12-29", "2026-01-04"),
    54: ("2026-01-05", "2026-01-11"),
    55: ("2026-01-12", "2026-01-18"),
    56: ("2026-01-19", "2026-01-25"),
    57: ("2026-01-26", "2026-02-01"),
    58: ("2026-02-02", "2026-02-08"),
    59: ("2026-02-09", "2026-02-15"),
    60: ("2026-02-16", "2026-02-22"),
    61: ("2026-02-23", "2026-03-01"),
    62: ("2026-03-02", "2026-03-08"),
    63: ("2026-03-09", "2026-03-15"),
    64: ("2026-03-16", "2026-03-22"),
    65: ("2026-03-23", "2026-03-29"),
    66: ("2026-03-30", "2026-04-05"),
    67: ("2026-04-06", "2026-04-12"),
    68: ("2026-04-13", "2026-04-19"),
    69: ("2026-04-20", "2026-04-26"),
    70: ("2026-04-27", "2026-05-03"),
    71: ("2026-05-04", "2026-05-10"),
    72: ("2026-05-11", "2026-05-17"),
    73: ("2026-05-18", "2026-05-24"),
    74: ("2026-05-25", "2026-05-31"),
    75: ("2026-06-01", "2026-06-07"),
    76: ("2026-06-08", "2026-06-14"),
    77: ("2026-06-15", "2026-06-21"),
    78: ("2026-06-22", "2026-06-28"),
    79: ("2026-06-29", "2026-07-05"),
    80: ("2026-07-06", "2026-07-12"),
    81: ("2026-07-13", "2026-07-19"),
    82: ("2026-07-20", "2026-07-26"),
    83: ("2026-07-27", "2026-08-02"),
    84: ("2026-08-03", "2026-08-09"),
    85: ("2026-08-10", "2026-08-16"),
    86: ("2026-08-17", "2026-08-23"),
    87: ("2026-08-24", "2026-08-30"),
    88: ("2026-08-31", "2026-09-06"),
    89: ("2026-09-07", "2026-09-13"),
    90: ("2026-09-14", "2026-09-20"),
    91: ("2026-09-21", "2026-09-27"),
    92: ("2026-09-28", "2026-10-04"),
    93: ("2026-10-05", "2026-10-11"),
    94: ("2026-10-12", "2026-10-18"),
    95: ("2026-10-19", "2026-10-25"),
    96: ("2026-10-26", "2026-11-01"),
}

def week_dates(week_number: int):
    """Devuelve lista de 7 fechas (date) desde el inicio de la semana."""
    if week_number not in WEEK_MAP:
        return []
    start_str, end_str = WEEK_MAP[week_number]
    d0 = date.fromisoformat(start_str)
    return [d0 + timedelta(days=i) for i in range(7)]

TIME_RE = re.compile(r"^\d{1,2}:\d{2}$")  # mm:ss

# almacenamiento en memoria
CAPTURAS = []  # cada item: dict con 'semana', 'fechas', 'eventos', 'doble', 'encuesta', 'tiempo'

@app.get("/health")
def health():
    return jsonify(status="ok"), 200

@app.get("/")
def root():
    return redirect(url_for("form_semana"))

@app.get("/captura")
def form_semana():
    # semana preseleccionada (opcional ?semana=43)
    semana_sel = request.args.get("semana", type=int)
    dias = week_dates(semana_sel) if semana_sel else []
    return render_template(
        "semana_form.html",
        week_map=WEEK_MAP,
        semana_sel=semana_sel,
        dias=dias
    )

@app.post("/guardar")
def guardar():
    semana = request.form.get("semana", type=int)
    if not semana or semana not in WEEK_MAP:
        flash("Selecciona una semana válida.")
        return redirect(url_for("form_semana", semana=semana or ""))

    dias = week_dates(semana)
    n = 7

    # toma arrays del form
    eventos = [request.form.get(f"eventos_{i}", "").strip() for i in range(n)]
    doble   = [request.form.get(f"doble_{i}", "").strip() for i in range(n)]
    encuesta= [request.form.get(f"encuesta_{i}", "").strip() for i in range(n)]
    tiempo  = [request.form.get(f"tiempo_{i}", "").strip() for i in range(n)]

    # validaciones básicas
    def is_int_or_empty(s): 
        return s == "" or s.isdigit()
    errs = []
    for i in range(n):
        if not is_int_or_empty(eventos[i]): errs.append(f"Eventos día {i+1} debe ser número.")
        if not is_int_or_empty(doble[i]):   errs.append(f"Doble asignación día {i+1} debe ser número.")
        if not is_int_or_empty(encuesta[i]):errs.append(f"Encuesta día {i+1} debe ser número.")
        if tiempo[i] and not TIME_RE.match(tiempo[i]): 
            errs.append(f"Tiempo día {i+1} debe ser mm:ss (ej: 05:30).")
    if errs:
        flash("\n".join(errs))
        return redirect(url_for("form_semana", semana=semana))

    # normaliza a int / mm:ss
    def to_int(s): return int(s) if s else 0
    eventos_i  = [to_int(x) for x in eventos]
    doble_i    = [to_int(x) for x in doble]
    encuesta_i = [to_int(x) for x in encuesta]

    captura = {
        "semana": semana,
        "rango": WEEK_MAP[semana],
        "fechas": [d.isoformat() for d in dias],
        "eventos": eventos_i,
        "doble": doble_i,
        "encuesta": encuesta_i,
        "tiempo": tiempo,  # mantener texto mm:ss
        "creado": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    CAPTURAS.append(captura)
    flash("Datos guardados.")
    return redirect(url_for("listar"))

@app.get("/registros")
def listar():
    return render_template("list.html", capturas=CAPTURAS)

@app.get("/download.csv")
def download_csv():
    if not CAPTURAS:
        flash("No hay datos para descargar.")
        return redirect(url_for("listar"))

    # aplanar para CSV: una fila por día
    rows = []
    for c in CAPTURAS:
        for i, fecha in enumerate(c["fechas"]):
            rows.append({
                "semana": c["semana"],
                "fecha": fecha,
                "eventos_seguridad": c["eventos"][i],
                "doble_asignacion": c["doble"][i],
                "encuesta_satisfaccion": c["encuesta"][i],
                "tiempo_atencion_mmss": c["tiempo"][i],
            })
    buf = io.StringIO()
    fieldnames = ["semana","fecha","eventos_seguridad","doble_asignacion","encuesta_satisfaccion","tiempo_atencion_mmss"]
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return send_file(
        io.BytesIO(buf.getvalue().encode("utf-8-sig")),
        mimetype="text/csv",
        as_attachment=True,
        download_name="capturas.csv"
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))


