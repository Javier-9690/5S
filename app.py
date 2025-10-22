import os
import io
import csv
import re
from statistics import mean
from datetime import datetime, date, time, timedelta

from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, send_file, jsonify
)

# ---------- BD ----------
from sqlalchemy import (
    create_engine, Column, Integer, String, Date, DateTime, Time, Float, Text
)
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError

# Excel
from openpyxl import Workbook, load_workbook

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret")

# ---------- Utilidades ----------
def normalize_db_url(url: str) -> str:
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
ENGINE = create_engine(normalize_db_url(DATABASE_URL), pool_pre_ping=True)
SessionLocal = sessionmaker(bind=ENGINE, autocommit=False, autoflush=False)
Base = declarative_base()

# ---------- Semanas ----------
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
def week_range(week_number: int):
    if week_number not in WEEK_MAP: return (None, None)
    s, e = WEEK_MAP[week_number]
    return (date.fromisoformat(s), date.fromisoformat(e))

# ---------- Tiempo mm:ss ----------
TIME_RE = re.compile(r"^\d{2}:\d{2}$")
def mmss_to_seconds(s: str) -> int:
    if not s: return 0
    m, ss = s.split(":"); return int(m)*60 + int(ss)
def seconds_to_mmss(x: int) -> str:
    m, s = divmod(max(0, int(x)), 60); return f"{m:02d}:{s:02d}"

# ---------- Modelos ----------
class CensusEntry(Base):
    __tablename__ = "census_entries"
    id = Column(Integer, primary_key=True)
    fecha = Column(Date, nullable=False)
    censo_dia = Column(Integer, nullable=False, default=0)
    censo_noche = Column(Integer, nullable=False, default=0)
    total = Column(Integer, nullable=False, default=0)
    creado = Column(DateTime, nullable=False, default=datetime.utcnow)

class EventSeguridad(Base):
    __tablename__ = "eventos_seguridad"
    id = Column(Integer, primary_key=True)
    fecha = Column(Date, nullable=False)
    horario = Column(String(50), nullable=False)
    que_ocurrio = Column(Text, nullable=False)
    nombre_afectado = Column(String(200), nullable=True)
    accion = Column(Text, nullable=True)
    creado = Column(DateTime, nullable=False, default=datetime.utcnow)

class DuplicidadEntry(Base):
    __tablename__ = "duplicidades"
    id = Column(Integer, primary_key=True)
    semana = Column(Integer, nullable=False)
    fecha = Column(Date, nullable=False)
    id_interno = Column(String(100), nullable=True)
    empresa_contratista = Column(String(200), nullable=True)
    descripcion_problema = Column(Text, nullable=True)
    tipo_riesgo = Column(String(200), nullable=True)  # Psicosocial
    pabellon = Column(String(100), nullable=True)
    habitacion = Column(String(100), nullable=True)
    ingresar_contacto = Column(String(200), nullable=True)
    nombre_usuario = Column(String(200), nullable=True)
    responsable = Column(String(200), nullable=True)
    estatus = Column(String(50), nullable=True)  # Cerrado/Abierto
    notificacion_usuario = Column(String(200), nullable=True)
    plan_accion = Column(Text, nullable=True)
    fecha_cierre = Column(Date, nullable=True)
    creado = Column(DateTime, nullable=False, default=datetime.utcnow)

class EncuestaEntry(Base):
    __tablename__ = "encuestas"
    id = Column(Integer, primary_key=True)
    fecha_hora = Column(DateTime, nullable=False)
    q1_respuesta = Column(Text, nullable=True)
    q1_puntaje = Column(Integer, nullable=True)
    q2_respuesta = Column(Text, nullable=True)
    q2_puntaje = Column(Integer, nullable=True)
    q3_respuesta = Column(Text, nullable=True)
    q3_puntaje = Column(Integer, nullable=True)
    q4_respuesta = Column(Text, nullable=True)
    q4_puntaje = Column(Integer, nullable=True)
    q5_respuesta = Column(Text, nullable=True)
    q5_puntaje = Column(Integer, nullable=True)
    total = Column(Integer, nullable=True)
    promedio = Column(Float, nullable=True)
    comentarios = Column(Text, nullable=True)
    creado = Column(DateTime, nullable=False, default=datetime.utcnow)

class AtencionEntry(Base):
    __tablename__ = "atencion_publico"
    id = Column(Integer, primary_key=True)
    fecha = Column(Date, nullable=False)
    tiempo_promedio_sec = Column(Integer, nullable=False, default=0)  # mm:ss -> seg
    cantidad = Column(Integer, nullable=False, default=0)
    creado = Column(DateTime, nullable=False, default=datetime.utcnow)

Base.metadata.create_all(ENGINE)

# ---------- Helpers filtros ----------
def resolve_filters(args):
    semana = args.get("semana", type=int)
    d_from = args.get("from")
    d_to = args.get("to")
    if semana and semana in WEEK_MAP:
        return *week_range(semana), semana
    df = date.fromisoformat(d_from) if d_from else None
    dt = date.fromisoformat(d_to) if d_to else None
    return df, dt, None

# ---------- Rutas básicas ----------
@app.get("/health")
def health():
    return jsonify(status="ok"), 200

@app.get("/")
def home():
    return redirect(url_for("panel", tab="censo"))

# ---------- PANEL (sidebar SIEMPRE en base.html) ----------
@app.route("/panel", methods=["GET", "POST"])
def panel():
    tab = request.args.get("tab", "censo")  # censo | eventos | duplicidades | encuesta | atencion
    db = SessionLocal()
    try:
        if request.method == "POST":
            if tab == "censo":
                fecha = date.fromisoformat(request.form["fecha"])
                cd = int(request.form.get("censo_dia", 0) or 0)
                cn = int(request.form.get("censo_noche", 0) or 0)
                total = int(request.form.get("total", cd + cn) or (cd + cn))
                db.add(CensusEntry(fecha=fecha, censo_dia=cd, censo_noche=cn, total=total))
                db.commit(); flash("Censo guardado.")

            elif tab == "eventos":
                fecha = date.fromisoformat(request.form["fecha"])
                horario = request.form.get("horario", "").strip()
                que = request.form.get("que_ocurrio", "").strip()
                nom = request.form.get("nombre_afectado", "").strip()
                accion = request.form.get("accion", "").strip()
                db.add(EventSeguridad(fecha=fecha, horario=horario, que_ocurrio=que,
                                      nombre_afectado=nom, accion=accion))
                db.commit(); flash("Evento de seguridad guardado.")

            elif tab == "duplicidades":
                semana = int(request.form["semana"])
                fecha = date.fromisoformat(request.form["fecha"])
                rec = DuplicidadEntry(
                    semana=semana,
                    fecha=fecha,
                    id_interno=request.form.get("id", "").strip(),
                    empresa_contratista=request.form.get("empresa_contratista", "").strip(),
                    descripcion_problema=request.form.get("descripcion_problema", "").strip(),
                    tipo_riesgo=request.form.get("tipo_riesgo", "").strip(),
                    pabellon=request.form.get("pabellon", "").strip(),
                    habitacion=request.form.get("habitacion", "").strip(),
                    ingresar_contacto=request.form.get("ingresar_contacto", "").strip(),
                    nombre_usuario=request.form.get("nombre_usuario", "").strip(),
                    responsable=request.form.get("responsable", "").strip(),
                    estatus=request.form.get("estatus", "").strip(),
                    notificacion_usuario=request.form.get("notificacion_usuario", "").strip(),
                    plan_accion=request.form.get("plan_accion", "").strip(),
                    fecha_cierre=(date.fromisoformat(request.form["fecha_cierre"])
                                  if request.form.get("fecha_cierre") else None),
                )
                db.add(rec); db.commit(); flash("Duplicidad guardada.")

            elif tab == "encuesta":
                fh_raw = request.form.get("fecha_hora")
                if "T" in fh_raw: fecha_hora = datetime.fromisoformat(fh_raw)
                else: fecha_hora = datetime.fromisoformat(fh_raw.replace(" ", "T"))
                vals = {}
                total = 0; n = 0
                for i in range(1,6):
                    r = request.form.get(f"q{i}_respuesta", "")
                    p = request.form.get(f"q{i}_puntaje", "")
                    p = int(p) if str(p).isdigit() else None
                    vals[i] = (r, p)
                    if p is not None: total += p; n += 1
                promedio = (total / n) if n>0 else None
                db.add(EncuestaEntry(
                    fecha_hora=fecha_hora,
                    q1_respuesta=vals[1][0], q1_puntaje=vals[1][1],
                    q2_respuesta=vals[2][0], q2_puntaje=vals[2][1],
                    q3_respuesta=vals[3][0], q3_puntaje=vals[3][1],
                    q4_respuesta=vals[4][0], q4_puntaje=vals[4][1],
                    q5_respuesta=vals[5][0], q5_puntaje=vals[5][1],
                    total=total if n>0 else None,
                    promedio=round(promedio,2) if promedio is not None else None,
                    comentarios=request.form.get("comentarios", "").strip(),
                ))
                db.commit(); flash("Encuesta guardada.")

            elif tab == "atencion":
                fecha = date.fromisoformat(request.form["fecha"])
                mmss = request.form.get("tiempo_promedio", "").strip()
                if mmss and not TIME_RE.match(mmss):
                    flash("Tiempo promedio debe ser mm:ss (ej: 03:54)")
                    return redirect(url_for("panel", tab=tab))
                cant = int(request.form.get("cantidad", 0) or 0)
                db.add(AtencionEntry(fecha=fecha, tiempo_promedio_sec=mmss_to_seconds(mmss), cantidad=cant))
                db.commit(); flash("Atención guardada.")

            return redirect(url_for("panel", tab=tab))

        # GET
        return render_template("panel.html", tab=tab, week_map=WEEK_MAP, current_tab=tab)
    finally:
        db.close()

# ---------- Listados con filtros ----------
@app.get("/registros")
def registros():
    d_from, d_to, semana_sel = resolve_filters(request.args)
    db = SessionLocal()
    try:
        if semana_sel:
            d_from, d_to = week_range(semana_sel)

        def between(q, col):
            if d_from: q = q.filter(col >= d_from)
            if d_to:   q = q.filter(col <= d_to)
            return q

        census = between(db.query(CensusEntry), CensusEntry.fecha).order_by(CensusEntry.fecha.desc()).all()
        eventos = between(db.query(EventSeguridad), EventSeguridad.fecha).order_by(EventSeguridad.fecha.desc()).all()
        duplics = between(db.query(DuplicidadEntry), DuplicidadEntry.fecha).order_by(DuplicidadEntry.fecha.desc()).all()
        encuestas = db.query(EncuestaEntry)
        if d_from: encuestas = encuestas.filter(EncuestaEntry.fecha_hora >= datetime.combine(d_from, time.min))
        if d_to:   encuestas = encuestas.filter(EncuestaEntry.fecha_hora <= datetime.combine(d_to, time.max))
        encuestas = encuestas.order_by(EncuestaEntry.fecha_hora.desc()).all()
        atenciones = between(db.query(AtencionEntry), AtencionEntry.fecha).order_by(AtencionEntry.fecha.desc()).all()

        return render_template("list.html",
                               semana_sel=semana_sel, d_from=d_from, d_to=d_to, week_map=WEEK_MAP,
                               census=census, eventos=eventos, duplics=duplics, encuestas=encuestas, atenciones=atenciones,
                               current_tab=None)
    finally:
        db.close()

# ---------- Descargas CSV por entidad ----------
@app.get("/download/<string:entity>.csv")
def download_entity(entity):
    d_from, d_to, semana_sel = resolve_filters(request.args)
    if semana_sel: d_from, d_to = week_range(semana_sel)
    db = SessionLocal()
    try:
        buf = io.StringIO()
        w = None

        if entity == "censo":
            q = db.query(CensusEntry)
            if d_from: q = q.filter(CensusEntry.fecha >= d_from)
            if d_to:   q = q.filter(CensusEntry.fecha <= d_to)
            rows = q.order_by(CensusEntry.fecha).all()
            w = csv.DictWriter(buf, fieldnames=["fecha", "censo_dia", "censo_noche", "total"])
            w.writeheader()
            for r in rows:
                w.writerow({"fecha": r.fecha.isoformat(), "censo_dia": r.censo_dia, "censo_noche": r.censo_noche, "total": r.total})

        elif entity == "eventos":
            q = db.query(EventSeguridad)
            if d_from: q = q.filter(EventSeguridad.fecha >= d_from)
            if d_to:   q = q.filter(EventSeguridad.fecha <= d_to)
            rows = q.order_by(EventSeguridad.fecha).all()
            w = csv.DictWriter(buf, fieldnames=["fecha","horario","que_ocurrio","nombre_afectado","accion"])
            w.writeheader()
            for r in rows:
                w.writerow({"fecha": r.fecha.isoformat(), "horario": r.horario, "que_ocurrio": r.que_ocurrio,
                            "nombre_afectado": r.nombre_afectado or "", "accion": r.accion or ""})

        elif entity == "duplicidades":
            q = db.query(DuplicidadEntry)
            if d_from: q = q.filter(DuplicidadEntry.fecha >= d_from)
            if d_to:   q = q.filter(DuplicidadEntry.fecha <= d_to)
            rows = q.order_by(DuplicidadEntry.fecha).all()
            headers = ["semana","fecha","id","empresa_contratista","descripcion_problema","tipo_riesgo",
                       "pabellon","habitacion","ingresar_contacto","nombre_usuario","responsable","estatus",
                       "notificacion_usuario","plan_accion","fecha_cierre"]
            w = csv.DictWriter(buf, fieldnames=headers)
            w.writeheader()
            for r in rows:
                w.writerow({
                    "semana": r.semana, "fecha": r.fecha.isoformat(), "id": r.id_interno or "",
                    "empresa_contratista": r.empresa_contratista or "", "descripcion_problema": r.descripcion_problema or "",
                    "tipo_riesgo": r.tipo_riesgo or "", "pabellon": r.pabellon or "", "habitacion": r.habitacion or "",
                    "ingresar_contacto": r.ingresar_contacto or "", "nombre_usuario": r.nombre_usuario or "",
                    "responsable": r.responsable or "", "estatus": r.estatus or "",
                    "notificacion_usuario": r.notificacion_usuario or "", "plan_accion": r.plan_accion or "",
                    "fecha_cierre": r.fecha_cierre.isoformat() if r.fecha_cierre else ""
                })

        elif entity == "encuestas":
            q = db.query(EncuestaEntry)
            if d_from: q = q.filter(EncuestaEntry.fecha_hora >= datetime.combine(d_from, time.min))
            if d_to:   q = q.filter(EncuestaEntry.fecha_hora <= datetime.combine(d_to, time.max))
            rows = q.order_by(EncuestaEntry.fecha_hora).all()
            headers = ["fecha_hora","q1_respuesta","q1_puntaje","q2_respuesta","q2_puntaje",
                       "q3_respuesta","q3_puntaje","q4_respuesta","q4_puntaje","q5_respuesta","q5_puntaje",
                       "total","promedio","comentarios"]
            w = csv.DictWriter(buf, fieldnames=headers)
            w.writeheader()
            for r in rows:
                w.writerow({
                    "fecha_hora": r.fecha_hora.isoformat(timespec="minutes"),
                    "q1_respuesta": r.q1_respuesta or "", "q1_puntaje": r.q1_puntaje or "",
                    "q2_respuesta": r.q2_respuesta or "", "q2_puntaje": r.q2_puntaje or "",
                    "q3_respuesta": r.q3_respuesta or "", "q3_puntaje": r.q3_puntaje or "",
                    "q4_respuesta": r.q4_respuesta or "", "q4_puntaje": r.q4_puntaje or "",
                    "q5_respuesta": r.q5_respuesta or "", "q5_puntaje": r.q5_puntaje or "",
                    "total": r.total if r.total is not None else "",
                    "promedio": r.promedio if r.promedio is not None else "",
                    "comentarios": r.comentarios or "",
                })

        elif entity == "atencion":
            q = db.query(AtencionEntry)
            if d_from: q = q.filter(AtencionEntry.fecha >= d_from)
            if d_to:   q = q.filter(AtencionEntry.fecha <= d_to)
            rows = q.order_by(AtencionEntry.fecha).all()
            w = csv.DictWriter(buf, fieldnames=["fecha","tiempo_promedio_mmss","cantidad"])
            w.writeheader()
            for r in rows:
                w.writerow({"fecha": r.fecha.isoformat(), "tiempo_promedio_mmss": seconds_to_mmss(r.tiempo_promedio_sec),
                            "cantidad": r.cantidad})
        else:
            flash("Entidad no válida.")
            return redirect(url_for("registros"))

        return send_file(
            io.BytesIO(buf.getvalue().encode("utf-8-sig")),
            mimetype="text/csv",
            as_attachment=True,
            download_name=f"{entity}.csv"
        )
    finally:
        db.close()

# ---------- Templates Excel por entidad ----------
TEMPLATES = {
    "censo": ["FECHA", "CENSO_DIA", "CENSO_NOCHE", "TOTAL"],
    "eventos": ["FECHA", "HORARIO", "QUE_OCURRIO", "NOMBRE_AFECTADO", "ACCION"],
    "duplicidades": [
        "SEMANA","FECHA","ID","EMPRESA_CONTRATISTA","DESCRIPCION_PROBLEMA","TIPO_RIESGO",
        "PABELLON","HABITACION","INGRESAR_CONTACTO","NOMBRE_USUARIO","RESPONSABLE","ESTATUS",
        "NOTIFICACION_USUARIO","PLAN_ACCION","FECHA_CIERRE"
    ],
    "encuesta": [
        "FECHA_HORA",
        "Q1_RESPUESTA","Q1_PUNTAJE","Q2_RESPUESTA","Q2_PUNTAJE","Q3_RESPUESTA","Q3_PUNTAJE",
        "Q4_RESPUESTA","Q4_PUNTAJE","Q5_RESPUESTA","Q5_PUNTAJE","TOTAL","PROMEDIO","COMENTARIOS"
    ],
    "atencion": ["FECHA","TIEMPO_PROMEDIO_MMSS","CANTIDAD"]
}

@app.get("/template/<string:entity>.xlsx")
def template_xlsx(entity):
    entity = entity.lower()
    if entity not in TEMPLATES:
        flash("Entidad no válida para plantilla.")
        return redirect(url_for("panel", tab="censo"))
    wb = Workbook()
    ws = wb.active
    ws.title = "Plantilla"
    ws.append(TEMPLATES[entity])
    out = io.BytesIO()
    wb.save(out); out.seek(0)
    return send_file(out, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                     as_attachment=True, download_name=f"plantilla_{entity}.xlsx")

# ---------- Importación Excel por entidad ----------
@app.post("/import/<string:entity>")
def import_xlsx(entity):
    entity = entity.lower()
    if entity not in TEMPLATES:
        flash("Entidad no válida para importación.")
        return redirect(url_for("panel", tab="censo"))

    f = request.files.get("file")
    if not f or f.filename == "":
        flash("Sube un archivo .xlsx.")
        return redirect(url_for("panel", tab=entity if entity != "eventos" else "eventos"))

    try:
        wb = load_workbook(filename=io.BytesIO(f.read()), data_only=True)
        ws = wb.active
        headers = [str(c.value).strip() if c.value is not None else "" for c in next(ws.iter_rows(min_row=1, max_row=1))]
        expected = TEMPLATES[entity]
        if [h.upper() for h in headers] != expected:
            flash(f"Encabezados inválidos. Esperado: {', '.join(expected)}")
            return redirect(url_for("panel", tab=entity if entity != "eventos" else "eventos"))

        inserted = 0
        db = SessionLocal()
        try:
            for row in ws.iter_rows(min_row=2, values_only=True):
                if all(cell is None or str(cell).strip()=="" for cell in row):
                    continue
                if entity == "censo":
                    fecha = date.fromisoformat(str(row[0]))
                    cd = int(row[1] or 0); cn = int(row[2] or 0)
                    total = int(row[3] or (cd+cn))
                    db.add(CensusEntry(fecha=fecha, censo_dia=cd, censo_noche=cn, total=total))

                elif entity == "eventos":
                    fecha = date.fromisoformat(str(row[0]))
                    horario = str(row[1] or "").strip()
                    que = str(row[2] or "").strip()
                    nom = str(row[3] or "").strip()
                    acc = str(row[4] or "").strip()
                    db.add(EventSeguridad(fecha=fecha, horario=horario, que_ocurrio=que,
                                          nombre_afectado=nom, accion=acc))

                elif entity == "duplicidades":
                    semana = int(row[0])
                    fecha = date.fromisoformat(str(row[1]))
                    db.add(DuplicidadEntry(
                        semana=semana, fecha=fecha,
                        id_interno=str(row[2] or "").strip(),
                        empresa_contratista=str(row[3] or "").strip(),
                        descripcion_problema=str(row[4] or "").strip(),
                        tipo_riesgo=str(row[5] or "").strip(),
                        pabellon=str(row[6] or "").strip(),
                        habitacion=str(row[7] or "").strip(),
                        ingresar_contacto=str(row[8] or "").strip(),
                        nombre_usuario=str(row[9] or "").strip(),
                        responsable=str(row[10] or "").strip(),
                        estatus=str(row[11] or "").strip(),
                        notificacion_usuario=str(row[12] or "").strip(),
                        plan_accion=str(row[13] or "").strip(),
                        fecha_cierre=(date.fromisoformat(str(row[14])) if row[14] else None)
                    ))

                elif entity == "encuesta":
                    fh_raw = str(row[0])
                    if "T" in fh_raw: fh = datetime.fromisoformat(fh_raw)
                    else: fh = datetime.fromisoformat(fh_raw.replace(" ", "T"))
                    def to_int(v): 
                        try: return int(v)
                        except: return None
                    q1r,q1p,q2r,q2p,q3r,q3p,q4r,q4p,q5r,q5p,tot,prm,com = row[1:14]
                    db.add(EncuestaEntry(
                        fecha_hora=fh,
                        q1_respuesta=str(q1r or ""), q1_puntaje=to_int(q1p),
                        q2_respuesta=str(q2r or ""), q2_puntaje=to_int(q2p),
                        q3_respuesta=str(q3r or ""), q3_puntaje=to_int(q3p),
                        q4_respuesta=str(q4r or ""), q4_puntaje=to_int(q4p),
                        q5_respuesta=str(q5r or ""), q5_puntaje=to_int(q5p),
                        total=to_int(tot),
                        promedio=float(prm) if prm not in (None,"") else None,
                        comentarios=str(com or "")
                    ))

                elif entity == "atencion":
                    fecha = date.fromisoformat(str(row[0]))
                    mmss = str(row[1] or "").strip()
                    if mmss and not TIME_RE.match(mmss):
                        mmss = "00:00"
                    cant = int(row[2] or 0)
                    db.add(AtencionEntry(fecha=fecha, tiempo_promedio_sec=mmss_to_seconds(mmss), cantidad=cant))

                inserted += 1

            db.commit()
            flash(f"Importación de {entity} OK: {inserted} filas.")
        except Exception as e:
            db.rollback()
            flash(f"Error importando {entity}: {e}")
        finally:
            db.close()

    except Exception as e:
        flash(f"No se pudo leer el Excel: {e}")

    # volver al tab correspondiente
    tab = "eventos" if entity=="eventos" else entity
    return redirect(url_for("panel", tab=tab))

# ---------- Dashboard (con toolbar solicitada) ----------
@app.get("/dashboard")
def dashboard():
    d_from, d_to, semana_sel = resolve_filters(request.args)
    if semana_sel: d_from, d_to = week_range(semana_sel)
    db = SessionLocal()
    try:
        per_day = {}
        def bucket(dkey):
            return per_day.setdefault(dkey, {
                "censo": 0,
                "eventos": 0,
                "duplicidades": 0,
                "encuestas": 0,
                "atencion_cant": 0,
                "atencion_tiempos": []
            })

        # Censo
        q = db.query(CensusEntry)
        if d_from: q = q.filter(CensusEntry.fecha >= d_from)
        if d_to:   q = q.filter(CensusEntry.fecha <= d_to)
        for r in q.all():
            b = bucket(r.fecha.isoformat())
            b["censo"] += (r.total or (r.censo_dia + r.censo_noche))

        # Eventos
        q = db.query(EventSeguridad)
        if d_from: q = q.filter(EventSeguridad.fecha >= d_from)
        if d_to:   q = q.filter(EventSeguridad.fecha <= d_to)
        for r in q.all():
            bucket(r.fecha.isoformat())["eventos"] += 1

        # Duplicidades
        q = db.query(DuplicidadEntry)
        if d_from: q = q.filter(DuplicidadEntry.fecha >= d_from)
        if d_to:   q = q.filter(DuplicidadEntry.fecha <= d_to)
        for r in q.all():
            bucket(r.fecha.isoformat())["duplicidades"] += 1

        # Encuestas
        q = db.query(EncuestaEntry)
        if d_from: q = q.filter(EncuestaEntry.fecha_hora >= datetime.combine(d_from, time.min))
        if d_to:   q = q.filter(EncuestaEntry.fecha_hora <= datetime.combine(d_to, time.max))
        for r in q.all():
            bucket(r.fecha_hora.date().isoformat())["encuestas"] += 1

        # Atención
        q = db.query(AtencionEntry)
        if d_from: q = q.filter(AtencionEntry.fecha >= d_from)
        if d_to:   q = q.filter(AtencionEntry.fecha <= d_to)
        for r in q.all():
            b = bucket(r.fecha.isoformat())
            b["atencion_cant"] += r.cantidad
            b["atencion_tiempos"].append(r.tiempo_promedio_sec)

        if not per_day:
            return render_template("dashboard.html", have_data=False, week_map=WEEK_MAP,
                                   d_from=d_from, d_to=d_to, semana_sel=semana_sel, current_tab=None)

        ordered_days = sorted(per_day.keys())
        s_censo, s_eventos, s_dup, s_enc, s_att_cant, s_att_mm = [], [], [], [], [], []
        for k in ordered_days:
            g = per_day[k]
            s_censo.append(g["censo"])
            s_eventos.append(g["eventos"])
            s_dup.append(g["duplicidades"])
            s_enc.append(g["encuestas"])
            s_att_cant.append(g["atencion_cant"])
            prom_s = int(mean(g["atencion_tiempos"])) if g["atencion_tiempos"] else 0
            s_att_mm.append(round(prom_s/60.0, 2))

        cards = {
            "censo_total": sum(s_censo),
            "eventos_total": sum(s_eventos),
            "duplicidades_total": sum(s_dup),
            "encuestas_total": sum(s_enc),
            "atencion_cant_total": sum(s_att_cant),
            "atencion_tiempo_prom_global": (
                seconds_to_mmss(int(mean([int(x*60) for x in s_att_mm if x>0])))
                if any(x>0 for x in s_att_mm) else "00:00"
            ),
        }

        return render_template("dashboard.html",
                               have_data=True,
                               week_map=WEEK_MAP,
                               labels=ordered_days,
                               series={
                                   "censo": s_censo,
                                   "eventos": s_eventos,
                                   "duplicidades": s_dup,
                                   "encuestas": s_enc,
                                   "atencion_cant": s_att_cant,
                                   "atencion_min": s_att_mm
                               },
                               cards=cards,
                               d_from=d_from, d_to=d_to, semana_sel=semana_sel,
                               current_tab=None)
    finally:
        db.close()

# ---------- Main ----------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))


