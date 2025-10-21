import os
import csv
import io
import re
from statistics import mean
from datetime import datetime, date, timedelta
from flask import (
    Flask, render_template, request, redirect,
    url_for, flash, send_file, jsonify
)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret")

# --- Semanas ---------------------------------------------------------------
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
    if week_number not in WEEK_MAP:
        return []
    start_str, _ = WEEK_MAP[week_number]
    d0 = date.fromisoformat(start_str)
    return [d0 + timedelta(days=i) for i in range(7)]

# mm:ss ESTRICTO
TIME_RE = re.compile(r"^\d{2}:\d{2}$")

def mmss_to_seconds(s: str) -> int:
    if not s:
        return 0
    m, ss = s.split(":")
    return int(m) * 60 + int(ss)

def seconds_to_mmss(x: int) -> str:
    m, s = divmod(max(0, int(x)), 60)
    return f"{m:02d}:{s:02d}"

# --- Memoria (no persistente) ----------------------------------------------
CAPTURAS = []

# --- Rutas ------------------------------------------------------------------
@app.get("/health")
def health():
    return jsonify(status="ok"), 200

@app.get("/")
def root():
    return redirect(url_for("form_semana"))

@app.get("/captura")
def form_semana():
    semana_sel = request.args.get("semana", type=int)
    dias = week_dates(semana_sel) if semana_sel else []
    return render_template("semana_form.html",
                           week_map=WEEK_MAP,
                           semana_sel=semana_sel,
                           dias=dias)

@app.post("/guardar")
def guardar():
    semana = request.form.get("semana", type=int)
    if not semana or semana not in WEEK_MAP:
        flash("Selecciona una semana válida.")
        return redirect(url_for("form_semana", semana=semana or ""))

    dias = week_dates(semana)
    n = 7

    def gi(name): return [request.form.get(f"{name}_{i}", "").strip() for i in range(n)]
    censo      = gi("censo")
    eventos    = gi("eventos")      # Eventos de seguridad
    duplicidad = gi("duplicidad")   # antes "doble"
    encuesta   = gi("encuesta")
    tiempo     = gi("tiempo")       # mm:ss

    def is_int_or_empty(s): return s == "" or s.isdigit()
    errs = []
    for i in range(n):
        if not is_int_or_empty(censo[i]):      errs.append(f"Censo día {i+1} debe ser número.")
        if not is_int_or_empty(eventos[i]):    errs.append(f"Eventos de seguridad día {i+1} debe ser número.")
        if not is_int_or_empty(duplicidad[i]): errs.append(f"Duplicidad día {i+1} debe ser número.")
        if not is_int_or_empty(encuesta[i]):   errs.append(f"Encuesta día {i+1} debe ser número.")
        if tiempo[i] and not TIME_RE.match(tiempo[i]):
            errs.append(f"Tiempo día {i+1} debe ser mm:ss (ej: 03:54).")
    if errs:
        flash("\n".join(errs))
        return redirect(url_for("form_semana", semana=semana))

    to_int = lambda s: int(s) if s else 0
    captura = {
        "semana": semana,
        "rango": WEEK_MAP[semana],
        "fechas": [d.isoformat() for d in dias],
        "censo": [to_int(x) for x in censo],
        "eventos_seguridad": [to_int(x) for x in eventos],
        "duplicidad": [to_int(x) for x in duplicidad],
        "encuesta": [to_int(x) for x in encuesta],
        "tiempo": tiempo,
        "creado": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    CAPTURAS.append(captura)
    flash("Datos guardados.")
    return redirect(url_for("listar"))

@app.post("/borrar/<int:idx>")
def borrar(idx):
    if 0 <= idx < len(CAPTURAS):
        CAPTURAS.pop(idx)
        flash("Registro eliminado.")
    else:
        flash("Índice inválido.")
    return redirect(url_for("listar"))

@app.get("/registros")
def listar():
    registros = []
    for c in CAPTURAS:
        t_secs = [mmss_to_seconds(x) for x in c["tiempo"]]
        reg = c.copy()
        reg["prom_censo"] = round(mean(c["censo"]), 2) if c["censo"] else 0
        reg["prom_eventos"] = round(mean(c["eventos_seguridad"]), 2) if c["eventos_seguridad"] else 0
        reg["prom_duplicidad"] = round(mean(c["duplicidad"]), 2) if c["duplicidad"] else 0
        reg["prom_encuesta"] = round(mean(c["encuesta"]), 2) if c["encuesta"] else 0
        reg["prom_tiempo"] = seconds_to_mmss(int(mean(t_secs))) if t_secs else "00:00"
        registros.append(reg)
    return render_template("list.html", capturas=registros)

@app.get("/download.csv")
def download_csv():
    if not CAPTURAS:
        flash("No hay datos para descargar.")
        return redirect(url_for("listar"))

    rows = []
    for c in CAPTURAS:
        for i, fecha in enumerate(c["fechas"]):
            rows.append({
                "semana": c["semana"],
                "fecha": fecha,
                "censo": c["censo"][i],
                "eventos_de_seguridad": c["eventos_seguridad"][i],
                "duplicidad": c["duplicidad"][i],
                "encuesta_satisfaccion": c["encuesta"][i],
                "tiempo_atencion_mmss": c["tiempo"][i],
            })
    buf = io.StringIO()
    fieldnames = ["semana","fecha","censo","eventos_de_seguridad","duplicidad","encuesta_satisfaccion","tiempo_atencion_mmss"]
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return send_file(
        io.BytesIO(buf.getvalue().encode("utf-8-sig")),
        mimetype="text/csv",
        as_attachment=True,
        download_name="capturas.csv"
    )

# ---------------- Dashboard (AHORA POR DÍA) ---------------------------------
@app.get("/dashboard")
def dashboard():
    if not CAPTURAS:
        return render_template("dashboard.html",
                               have_data=False,
                               cards={},
                               labels=[],
                               series={},
                               table=[])

    # Agregación diaria global (sumas por día; tiempo = promedio por día)
    per_day = {}  # { 'YYYY-MM-DD': {censo, eventos, duplicidad, encuesta, tiempos_segundos[]} }
    for c in CAPTURAS:
        for i, fecha in enumerate(c["fechas"]):
            d = per_day.setdefault(fecha, {"censo":0, "eventos":0, "duplicidad":0, "encuesta":0, "tiempos": []})
            d["censo"]      += c["censo"][i]
            d["eventos"]    += c["eventos_seguridad"][i]
            d["duplicidad"] += c["duplicidad"][i]
            d["encuesta"]   += c["encuesta"][i]
            d["tiempos"].append(mmss_to_seconds(c["tiempo"][i]) if c["tiempo"][i] else 0)

    # ordenar por fecha
    ordered_dates = sorted(per_day.keys())
    s_censo, s_eventos, s_duplicidad, s_encuesta, s_tavg = [], [], [], [], []
    for d in ordered_dates:
        s_censo.append(per_day[d]["censo"])
        s_eventos.append(per_day[d]["eventos"])
        s_duplicidad.append(per_day[d]["duplicidad"])
        s_encuesta.append(per_day[d]["encuesta"])
        avg_s = int(mean(per_day[d]["tiempos"])) if per_day[d]["tiempos"] else 0
        s_tavg.append(avg_s)

    # tarjetas (totales y promedio global de tiempo)
    cards = {
        "censo_total": sum(s_censo),
        "eventos_total": sum(s_eventos),
        "duplicidad_total": sum(s_duplicidad),
        "encuesta_total": sum(s_encuesta),
        "tiempo_prom_global": seconds_to_mmss(int(mean([x for x in s_tavg if x>0])) if any(s_tavg) else 0),
    }

    # tabla por semana (dejamos igual que antes, sin ratios eventos/encuesta x100)
    # agrupamos por semana solo para la tabla resumen
    def mmss_to_s_list(lst): return [mmss_to_seconds(x) for x in lst]
    by_week = {}
    for c in CAPTURAS:
        w = c["semana"]
        if w not in by_week:
            by_week[w] = {"rango": c["rango"], "censo": [], "eventos": [], "duplicidad": [], "encuesta": [], "tiempo_s": []}
        by_week[w]["censo"]      += c["censo"]
        by_week[w]["eventos"]    += c["eventos_seguridad"]
        by_week[w]["duplicidad"] += c["duplicidad"]
        by_week[w]["encuesta"]   += c["encuesta"]
        by_week[w]["tiempo_s"]   += mmss_to_s_list(c["tiempo"])

    table = []
    for w in sorted(by_week.keys()):
        b = by_week[w]
        censo_sum = sum(b["censo"])
        dup_sum   = sum(b["duplicidad"])
        table.append({
            "semana": w,
            "rango": b["rango"],
            "censo": censo_sum,
            "eventos": sum(b["eventos"]),
            "duplicidad": dup_sum,
            "encuesta": sum(b["encuesta"]),
            "t_prom": seconds_to_mmss(int(mean(b["tiempo_s"])) if b["tiempo_s"] else 0),
            "dup_x100": round((dup_sum / censo_sum * 100), 2) if censo_sum else 0.0
        })

    series = {
        "labels_days": ordered_dates,
        "censo": s_censo,
        "eventos": s_eventos,
        "duplicidad": s_duplicidad,
        "encuesta": s_encuesta,
        "tavg_sec": s_tavg,
    }

    return render_template("dashboard.html",
                           have_data=True,
                           cards=cards,
                           labels=ordered_dates,
                           series=series,
                           table=table)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))



