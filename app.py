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

# -------------------- BD: SQLAlchemy --------------------
from sqlalchemy import create_engine, Column, Integer, Date, DateTime, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy.exc import SQLAlchemyError

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret")

# ---------- Conexión a BD ----------
def _normalize_db_url(url: str) -> str:
    # Fuerza driver psycopg2 y SSL si no viene
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    if "sslmode=" not in url:
        url += ("&" if "?" in url else "?") + "sslmode=require"
    return url

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("Falta la variable de entorno DATABASE_URL")

ENGINE = create_engine(_normalize_db_url(DATABASE_URL), pool_pre_ping=True)
SessionLocal = sessionmaker(bind=ENGINE, autocommit=False, autoflush=False)
Base = declarative_base()

# ---------- Modelos ----------
class Capture(Base):
    __tablename__ = "captures"
    id = Column(Integer, primary_key=True)
    semana = Column(Integer, nullable=False)
    rango_start = Column(Date, nullable=False)
    rango_end = Column(Date, nullable=False)
    creado = Column(DateTime, nullable=False, default=datetime.utcnow)

    days = relationship("CaptureDay", back_populates="capture",
                        cascade="all, delete-orphan")

class CaptureDay(Base):
    __tablename__ = "capture_days"
    id = Column(Integer, primary_key=True)
    capture_id = Column(Integer, ForeignKey("captures.id", ondelete="CASCADE"), nullable=False)
    fecha = Column(Date, nullable=False)
    censo = Column(Integer, nullable=False, default=0)
    eventos_seguridad = Column(Integer, nullable=False, default=0)
    duplicidad = Column(Integer, nullable=False, default=0)
    encuesta = Column(Integer, nullable=False, default=0)
    tiempo_sec = Column(Integer, nullable=False, default=0)  # guardamos mm:ss como segundos

    capture = relationship("Capture", back_populates="days")

# Crea tablas si no existen
Base.metadata.create_all(ENGINE)

# -------------------- Semanas / utilitarios --------------------
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

TIME_RE = re.compile(r"^\d{2}:\d{2}$")

def mmss_to_seconds(s: str) -> int:
    if not s:
        return 0
    m, ss = s.split(":")
    return int(m) * 60 + int(ss)

def seconds_to_mmss(x: int) -> str:
    m, s = divmod(max(0, int(x)), 60)
    return f"{m:02d}:{s:02d}"

# -------------------- Rutas --------------------
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
    eventos    = gi("eventos")
    duplicidad = gi("duplicidad")
    encuesta   = gi("encuesta")
    tiempo     = gi("tiempo")

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

    # Persistir en BD
    try:
        db = SessionLocal()
        rango_start = date.fromisoformat(WEEK_MAP[semana][0])
        rango_end   = date.fromisoformat(WEEK_MAP[semana][1])
        cap = Capture(semana=semana, rango_start=rango_start, rango_end=rango_end)
        db.add(cap)
        db.flush()  # obtiene cap.id

        for i, d in enumerate(dias):
            t_sec = mmss_to_seconds(tiempo[i]) if tiempo[i] else 0
            day = CaptureDay(
                capture_id=cap.id,
                fecha=d,
                censo=to_int(censo[i]),
                eventos_seguridad=to_int(eventos[i]),
                duplicidad=to_int(duplicidad[i]),
                encuesta=to_int(encuesta[i]),
                tiempo_sec=t_sec
            )
            db.add(day)

        db.commit()
        flash("Datos guardados.")
    except SQLAlchemyError as e:
        if 'db' in locals(): db.rollback()
        flash(f"Error al guardar en BD: {e}")
    finally:
        if 'db' in locals(): db.close()

    return redirect(url_for("listar"))

@app.post("/borrar/<int:capture_id>")
def borrar(capture_id):
    try:
        db = SessionLocal()
        cap = db.get(Capture, capture_id)
        if not cap:
            flash("Registro no encontrado.")
        else:
            db.delete(cap)  # cascade borra los días
            db.commit()
            flash("Registro eliminado.")
    except SQLAlchemyError as e:
        if 'db' in locals(): db.rollback()
        flash(f"Error al eliminar: {e}")
    finally:
        if 'db' in locals(): db.close()

    return redirect(url_for("listar"))

@app.get("/registros")
def listar():
    pares = []  # [(capture_id, reg), ...]
    try:
    # Leer SIEMPRE desde la BD
        db = SessionLocal()
        caps = db.query(Capture).order_by(Capture.id.desc()).all()
        for cap in caps:
            days = sorted(cap.days, key=lambda d: d.fecha)
            fechas = [d.fecha.isoformat() for d in days]
            censo = [d.censo for d in days]
            eventos = [d.eventos_seguridad for d in days]
            duplicidad = [d.duplicidad for d in days]
            encuesta = [d.encuesta for d in days]
            tiempos = [seconds_to_mmss(d.tiempo_sec) for d in days]
            t_secs = [d.tiempo_sec for d in days] or [0]

            reg = {
                "semana": cap.semana,
                "rango": (cap.rango_start.isoformat(), cap.rango_end.isoformat()),
                "fechas": fechas,
                "censo": censo,
                "eventos_seguridad": eventos,
                "duplicidad": duplicidad,
                "encuesta": encuesta,
                "tiempo": tiempos,
                "creado": cap.creado.isoformat(timespec="seconds") + "Z",
                "prom_censo": round(mean(censo), 2) if censo else 0,
                "prom_eventos": round(mean(eventos), 2) if eventos else 0,
                "prom_duplicidad": round(mean(duplicidad), 2) if duplicidad else 0,
                "prom_encuesta": round(mean(encuesta), 2) if encuesta else 0,
                "prom_tiempo": seconds_to_mmss(int(mean(t_secs))) if t_secs else "00:00",
            }
            pares.append((cap.id, reg))
    except SQLAlchemyError as e:
        flash(f"Error al leer BD: {e}")
    finally:
        if 'db' in locals(): db.close()

    return render_template("list.html", capturas_id_reg=pares)

@app.get("/download.csv")
def download_csv():
    """Exporta SIEMPRE desde la BD (captures + capture_days)."""
    try:
        db = SessionLocal()
        caps = db.query(Capture).order_by(Capture.semana).all()
        if not caps:
            flash("No hay datos para descargar.")
            return redirect(url_for("listar"))

        rows = []
        for cap in caps:
            for d in sorted(cap.days, key=lambda x: x.fecha):
                rows.append({
                    "semana": cap.semana,
                    "fecha": d.fecha.isoformat(),
                    "censo": d.censo,
                    "eventos_de_seguridad": d.eventos_seguridad,
                    "duplicidad": d.duplicidad,
                    "encuesta_satisfaccion": d.encuesta,
                    "tiempo_atencion_mmss": seconds_to_mmss(d.tiempo_sec),
                })

        buf = io.StringIO()
        fieldnames = [
            "semana", "fecha", "censo",
            "eventos_de_seguridad", "duplicidad",
            "encuesta_satisfaccion", "tiempo_atencion_mmss"
        ]
        writer = csv.DictWriter(buf, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

        return send_file(
            io.BytesIO(buf.getvalue().encode("utf-8-sig")),
            mimetype="text/csv",
            as_attachment=True,
            download_name="capturas.csv"
        )
    except SQLAlchemyError as e:
        flash(f"Error al exportar: {e}")
        return redirect(url_for("listar"))
    finally:
        if 'db' in locals(): db.close()

# ---------------- Dashboard (por día) ----------------
@app.get("/dashboard")
def dashboard():
    try:
        db = SessionLocal()
        days = db.query(CaptureDay).join(Capture, CaptureDay.capture_id==Capture.id).all()
        if not days:
            return render_template("dashboard.html",
                                   have_data=False, cards={}, labels=[], series={}, table=[])

        # agregación por día
        per_day = {}
        for d in days:
            key = d.fecha.isoformat()
            g = per_day.setdefault(key, {"censo":0,"eventos":0,"duplicidad":0,"encuesta":0,"tiempos":[]})
            g["censo"] += d.censo
            g["eventos"] += d.eventos_seguridad
            g["duplicidad"] += d.duplicidad
            g["encuesta"] += d.encuesta
            g["tiempos"].append(d.tiempo_sec)

        ordered_dates = sorted(per_day.keys())
        s_censo, s_eventos, s_duplicidad, s_encuesta, s_tavg = [], [], [], [], []
        for k in ordered_dates:
            g = per_day[k]
            s_censo.append(g["censo"])
            s_eventos.append(g["eventos"])
            s_duplicidad.append(g["duplicidad"])
            s_encuesta.append(g["encuesta"])
            s_tavg.append(int(mean(g["tiempos"])) if g["tiempos"] else 0)

        cards = {
            "censo_total": sum(s_censo),
            "eventos_total": sum(s_eventos),
            "duplicidad_total": sum(s_duplicidad),
            "encuesta_total": sum(s_encuesta),
            "tiempo_prom_global": seconds_to_mmss(int(mean([x for x in s_tavg if x>0])) if any(s_tavg) else 0),
        }

        # tabla por semana
        caps = db.query(Capture).order_by(Capture.semana).all()
        table = []
        for cap in caps:
            ds = cap.days
            censo_sum = sum(d.censo for d in ds)
            dup_sum = sum(d.duplicidad for d in ds)
            t_prom_s = int(mean([d.tiempo_sec for d in ds])) if ds else 0
            table.append({
                "semana": cap.semana,
                "rango": (cap.rango_start.isoformat(), cap.rango_end.isoformat()),
                "censo": censo_sum,
                "eventos": sum(d.eventos_seguridad for d in ds),
                "duplicidad": dup_sum,
                "encuesta": sum(d.encuesta for d in ds),
                "t_prom": seconds_to_mmss(t_prom_s),
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
    except SQLAlchemyError as e:
        flash(f"Error en dashboard: {e}")
        return render_template("dashboard.html", have_data=False, cards={}, labels=[], series={}, table=[])
    finally:
        if 'db' in locals(): db.close()

# ---------------- Main ----------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))



