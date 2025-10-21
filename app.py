import os
import re
from datetime import datetime, date, timedelta
from statistics import mean

from flask import (
    Flask, render_template, request, redirect,
    url_for, flash, send_file, jsonify
)

from sqlalchemy import (
    create_engine, Column, Integer, String, Date, DateTime, ForeignKey, func
)
from sqlalchemy.orm import sessionmaker, declarative_base, relationship, scoped_session

# -----------------------------------------------------------------------------
# Flask
# -----------------------------------------------------------------------------
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret")

# -----------------------------------------------------------------------------
# Base de datos: PostgreSQL (DATABASE_URL) o SQLite local
# -----------------------------------------------------------------------------
DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///data.db")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL, echo=False, future=True)
SessionLocal = scoped_session(sessionmaker(bind=engine, autoflush=False, autocommit=False))
Base = declarative_base()

# -----------------------------------------------------------------------------
# Modelos
# -----------------------------------------------------------------------------
class Capture(Base):
    __tablename__ = "captures"
    id = Column(Integer, primary_key=True)
    semana = Column(Integer, nullable=False, index=True)
    rango_ini = Column(String(10), nullable=False)  # YYYY-MM-DD
    rango_fin = Column(String(10), nullable=False)
    creado = Column(DateTime, default=datetime.utcnow)

    entries = relationship("DayEntry", back_populates="capture", cascade="all, delete-orphan")

class DayEntry(Base):
    __tablename__ = "day_entries"
    id = Column(Integer, primary_key=True)
    capture_id = Column(Integer, ForeignKey("captures.id", ondelete="CASCADE"), index=True)
    fecha = Column(Date, nullable=False)

    censo = Column(Integer, default=0)
    eventos_seguridad = Column(Integer, default=0)
    duplicidad = Column(Integer, default=0)
    encuesta = Column(Integer, default=0)
    tiempo_seg = Column(Integer, default=0)  # mm:ss -> segundos

    capture = relationship("Capture", back_populates="entries")

Base.metadata.create_all(engine)

# -----------------------------------------------------------------------------
# Semanas fijas
# -----------------------------------------------------------------------------
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

# Tiempo mm:ss
TIME_RE = re.compile(r"^\d{2}:\d{2}$")
def mmss_to_seconds(s: str) -> int:
    if not s: return 0
    m, sec = s.split(":")
    return int(m)*60 + int(sec)

def seconds_to_mmss(x: int) -> str:
    m, s = divmod(max(0, int(x)), 60)
    return f"{m:02d}:{s:02d}"

# -----------------------------------------------------------------------------
# Rutas
# -----------------------------------------------------------------------------
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
        if tiempo[i] and not TIME_RE.match(tiempo[i]): errs.append(f"Tiempo día {i+1} debe ser mm:ss.")
    if errs:
        flash("\n".join(errs)); return redirect(url_for("form_semana", semana=semana))

    to_int = lambda s: int(s) if s else 0
    start, end = WEEK_MAP[semana]
    db = SessionLocal()
    try:
        cap = Capture(semana=semana, rango_ini=start, rango_fin=end)
        db.add(cap); db.flush()

        for i, d in enumerate(dias):
            e = DayEntry(
                capture_id=cap.id,
                fecha=d,
                censo=to_int(censo[i]),
                eventos_seguridad=to_int(eventos[i]),
                duplicidad=to_int(duplicidad[i]),
                encuesta=to_int(encuesta[i]),
                tiempo_seg=mmss_to_seconds(tiempo[i]) if tiempo[i] else 0,
            )
            db.add(e)

        db.commit()
        flash("Datos guardados.")
    except Exception as ex:
        db.rollback()
        flash(f"Error guardando: {ex}")
    finally:
        db.close()

    return redirect(url_for("listar"))

@app.post("/borrar/<int:capture_id>")
def borrar(capture_id):
    db = SessionLocal()
    try:
        cap = db.get(Capture, capture_id)
        if not cap:
            flash("Registro no encontrado.")
        else:
            db.delete(cap); db.commit()
            flash("Registro eliminado.")
    except Exception as ex:
        db.rollback(); flash(f"Error eliminando: {ex}")
    finally:
        db.close()
    return redirect(url_for("listar"))

@app.get("/registros")
def listar():
    db = SessionLocal()
    registros = []
    try:
        caps = db.query(Capture).order_by(Capture.creado.desc()).all()
        for cap in caps:
            entradas = db.query(DayEntry).filter(DayEntry.capture_id==cap.id).order_by(DayEntry.fecha).all()
            tsecs = [e.tiempo_seg for e in entradas]
            def avg(lst): return round(mean(lst), 2) if lst else 0
            reg = {
                "id": cap.id,
                "semana": cap.semana,
                "rango": (cap.rango_ini, cap.rango_fin),
                "creado": cap.creado.strftime("%Y-%m-%d %H:%M:%S"),
                "fechas": [e.fecha.isoformat() for e in entradas],
                "censo": [e.censo for e in entradas],
                "eventos_seguridad": [e.eventos_seguridad for e in entradas],
                "duplicidad": [e.duplicidad for e in entradas],
                "encuesta": [e.encuesta for e in entradas],
                "tiempo": [seconds_to_mmss(e.tiempo_seg) for e in entradas],
                "prom_censo": avg([e.censo for e in entradas]),
                "prom_eventos": avg([e.eventos_seguridad for e in entradas]),
                "prom_duplicidad": avg([e.duplicidad for e in entradas]),
                "prom_encuesta": avg([e.encuesta for e in entradas]),
                "prom_tiempo": seconds_to_mmss(int(avg(tsecs))) if tsecs else "00:00",
            }
            registros.append(reg)
    finally:
        db.close()
    return render_template("list.html", capturas=registros)

# ---------------- Dashboard: por día (global) y resumen por semana ----------
@app.get("/dashboard")
def dashboard():
    db = SessionLocal()
    try:
        if db.query(Capture).count() == 0:
            return render_template("dashboard.html",
                                   have_data=False, cards={}, labels=[],
                                   series={}, table=[])

        # Agregación diaria global
        q = (
            db.query(
                DayEntry.fecha.label("fecha"),
                func.sum(DayEntry.censo).label("censo"),
                func.sum(DayEntry.eventos_seguridad).label("eventos"),
                func.sum(DayEntry.duplicidad).label("duplicidad"),
                func.sum(DayEntry.encuesta).label("encuesta"),
                func.avg(DayEntry.tiempo_seg).label("tavg")
            ).group_by(DayEntry.fecha).order_by(DayEntry.fecha)
        ).all()

        ordered_dates = [r.fecha.isoformat() for r in q]
        s_censo = [int(r.censo or 0) for r in q]
        s_eventos = [int(r.eventos or 0) for r in q]
        s_dup = [int(r.duplicidad or 0) for r in q]
        s_enc = [int(r.encuesta or 0) for r in q]
        s_tavg = [int(r.tavg or 0) for r in q]

        cards = {
            "censo_total": sum(s_censo),
            "eventos_total": sum(s_eventos),
            "duplicidad_total": sum(s_dup),
            "encuesta_total": sum(s_enc),
            "tiempo_prom_global": seconds_to_mmss(int(mean([x for x in s_tavg if x>0])) if any(s_tavg) else 0),
        }

        # Resumen por semana
        qb = (
            db.query(
                Capture.semana, Capture.rango_ini, Capture.rango_fin,
                func.sum(DayEntry.censo).label("censo"),
                func.sum(DayEntry.eventos_seguridad).label("eventos"),
                func.sum(DayEntry.duplicidad).label("duplicidad"),
                func.sum(DayEntry.encuesta).label("encuesta"),
                func.avg(DayEntry.tiempo_seg).label("tavg")
            )
            .join(DayEntry, DayEntry.capture_id==Capture.id)
            .group_by(Capture.semana, Capture.rango_ini, Capture.rango_fin)
            .order_by(Capture.semana)
        ).all()

        table = []
        for r in qb:
            censo_sum = int(r.censo or 0)
            dup_sum = int(r.duplicidad or 0)
            table.append({
                "semana": int(r.semana),
                "rango": (r.rango_ini, r.rango_fin),
                "censo": censo_sum,
                "eventos": int(r.eventos or 0),
                "duplicidad": dup_sum,
                "encuesta": int(r.encuesta or 0),
                "t_prom": seconds_to_mmss(int(r.tavg or 0)),
                "dup_x100": round((dup_sum / censo_sum * 100), 2) if censo_sum else 0.0,
            })

        series = {
            "labels_days": ordered_dates,
            "censo": s_censo,
            "eventos": s_eventos,
            "duplicidad": s_dup,
            "encuesta": s_enc,
            "tavg_sec": s_tavg,
        }

        return render_template("dashboard.html",
                               have_data=True, cards=cards,
                               labels=ordered_dates, series=series, table=table)
    finally:
        db.close()

# -----------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

