# app.py
import os
import io
import csv
from datetime import datetime, date, time, timedelta
from typing import Dict, Any, List, Tuple

from flask import (
    Flask, render_template, request, redirect, url_for, flash,
    send_file, send_from_directory
)
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func
from werkzeug.utils import secure_filename

# ---------------------------
# Configuración básica
# ---------------------------
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DB_PATH = os.path.join(BASE_DIR, "app.db")
UPLOAD_DIR_ONBOARD = os.path.join(BASE_DIR, "uploads", "onboarding")

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{DB_PATH}"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SECRET_KEY"] = "changeme-secret"
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB
app.config["UPLOAD_FOLDER_ONBOARD"] = UPLOAD_DIR_ONBOARD

db = SQLAlchemy(app)

# ---------------------------
# Utilidades
# ---------------------------
def ensure_dirs():
    os.makedirs(app.config["UPLOAD_FOLDER_ONBOARD"], exist_ok=True)

@app.before_request
def _ensure_uploads():
    ensure_dirs()

def parse_date(val: str):
    if not val:
        return None
    try:
        return datetime.strptime(val, "%Y-%m-%d").date()
    except Exception:
        return None

def parse_datetime_local(val: str):
    # formato HTML datetime-local -> "YYYY-MM-DDTHH:MM"
    if not val:
        return None
    for fmt in ("%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(val, fmt)
        except Exception:
            continue
    return None

def parse_time_hhmm(val: str):
    if not val:
        return None
    try:
        return datetime.strptime(val, "%H:%M").time()
    except Exception:
        return None

def parse_mmss_to_seconds(val: str):
    if not val:
        return None
    try:
        mm, ss = val.split(":")
        return int(mm) * 60 + int(ss)
    except Exception:
        return None

def seconds_to_mmss(sec: int):
    if sec is None:
        return None
    m = sec // 60
    s = sec % 60
    return f"{m:02d}:{s:02d}"

def build_week_map(year: int) -> Dict[int, Tuple[date, date]]:
    """
    Mapa de semana ISO → (lunes, domingo)
    """
    # ISO: semana 1 es la que contiene el primer jueves del año
    # Buscamos desde semana 1 hasta 53 por si acaso.
    res = {}
    for w in range(1, 54):
        try:
            monday = iso_to_gregorian(year, w, 1)
            sunday = iso_to_gregorian(year, w, 7)
            # Aseguramos que pertenezcan al mismo año o es válido como rango límite
            res[w] = (monday, sunday)
        except Exception:
            pass
    return res

def iso_to_gregorian(iso_year, iso_week, iso_day):
    """
    Convierte ISO year/week/day a fecha gregoriana.
    """
    # Fuente: algoritmo común basado en ISO calendario
    fourth_jan = date(iso_year, 1, 4)
    delta = timedelta(fourth_jan.isoweekday() - 1)
    week1_monday = fourth_jan - delta
    return week1_monday + timedelta(days=(iso_week - 1) * 7 + (iso_day - 1))

# ---------------------------
# Modelos
# ---------------------------
class BaseModel(db.Model):
    __abstract__ = True
    id = db.Column(db.Integer, primary_key=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

# Censo
class CensoEntry(BaseModel):
    __tablename__ = "censo"
    fecha = db.Column(db.Date, nullable=False)
    censo_dia = db.Column(db.Integer, default=0)
    censo_noche = db.Column(db.Integer, default=0)
    total = db.Column(db.Integer, default=0)

# Eventos de seguridad
class EventoSeguridadEntry(BaseModel):
    __tablename__ = "eventos_seguridad"
    fecha = db.Column(db.Date, nullable=False)
    horario = db.Column(db.String(100), nullable=False)
    que_ocurrio = db.Column(db.Text, nullable=False)
    nombre_afectado = db.Column(db.String(255))
    accion = db.Column(db.Text)

# Duplicidades
class DuplicidadEntry(BaseModel):
    __tablename__ = "duplicidades"
    semana = db.Column(db.Integer)  # ISO week number
    fecha = db.Column(db.Date, nullable=False)
    id_interno = db.Column(db.String(100))
    empresa_contratista = db.Column(db.String(255))
    descripcion_problema = db.Column(db.Text)
    tipo_riesgo = db.Column(db.String(100))
    pabellon = db.Column(db.String(100))
    habitacion = db.Column(db.String(100))
    ingresar_contacto = db.Column(db.String(255))
    nombre_usuario = db.Column(db.String(255))
    responsable = db.Column(db.String(255))
    estatus = db.Column(db.String(100))
    notificacion_usuario = db.Column(db.String(255))
    plan_accion = db.Column(db.Text)
    fecha_cierre = db.Column(db.Date)

# Encuesta satisfacción
class EncuestaEntry(BaseModel):
    __tablename__ = "encuesta_satisfaccion"
    fecha_hora = db.Column(db.DateTime, nullable=False)
    q1_respuesta = db.Column(db.String(255)); q1_puntaje = db.Column(db.Integer)
    q2_respuesta = db.Column(db.String(255)); q2_puntaje = db.Column(db.Integer)
    q3_respuesta = db.Column(db.String(255)); q3_puntaje = db.Column(db.Integer)
    q4_respuesta = db.Column(db.String(255)); q4_puntaje = db.Column(db.Integer)
    q5_respuesta = db.Column(db.String(255)); q5_puntaje = db.Column(db.Integer)
    total = db.Column(db.Integer)
    promedio = db.Column(db.Float)
    comentarios = db.Column(db.Text)

# Atención al público
class AtencionPublicoEntry(BaseModel):
    __tablename__ = "atencion_publico"
    fecha = db.Column(db.Date, nullable=False)
    tiempo_promedio_sec = db.Column(db.Integer)  # guardamos en segundos
    cantidad = db.Column(db.Integer, default=0)

# Robos y hurtos
class RoboHurtoEntry(BaseModel):
    __tablename__ = "robos_hurtos"
    fecha = db.Column(db.Date, nullable=False)
    hora = db.Column(db.Time)
    modulo = db.Column(db.String(100))
    habitacion = db.Column(db.String(100))
    empresa = db.Column(db.String(255))
    nombre_cliente = db.Column(db.String(255))
    rut = db.Column(db.String(50))
    medio_reclamo = db.Column(db.String(100))
    especies = db.Column(db.Text)
    observaciones = db.Column(db.Text)
    recepciona = db.Column(db.String(255))

# Misceláneo
class MiscelaneoEntry(BaseModel):
    __tablename__ = "miscelaneo"
    ot = db.Column(db.String(100))
    division = db.Column(db.String(100))
    area = db.Column(db.String(100))
    lugar = db.Column(db.String(100))
    ubicacion = db.Column(db.String(100))
    disciplina = db.Column(db.String(100))
    especialidad = db.Column(db.String(100))
    falla = db.Column(db.String(100))
    empresa = db.Column(db.String(255))
    fecha_creacion = db.Column(db.Date)
    fecha_inicio = db.Column(db.Date)
    fecha_termino = db.Column(db.Date)
    fecha_aprobacion = db.Column(db.Date)
    estado = db.Column(db.String(100))
    comentario = db.Column(db.Text)

# Desviaciones
class DesviacionEntry(BaseModel):
    __tablename__ = "desviaciones"
    n_solicitud = db.Column(db.String(100))
    fecha = db.Column(db.Date, nullable=False)
    id_interno = db.Column(db.String(100))
    empresa_contratista = db.Column(db.String(255))
    descripcion_problema = db.Column(db.Text)
    tipo_riesgo = db.Column(db.String(100))
    tipo_solicitud = db.Column(db.String(100))
    pabellon = db.Column(db.String(100))
    habitacion = db.Column(db.String(100))
    via_solicitud = db.Column(db.String(100))
    quien_informa = db.Column(db.String(255))
    riesgo_material = db.Column(db.String(100))
    correo_destino = db.Column(db.String(255))

# Solicitud y OT
class SolicitudOTEntry(BaseModel):
    __tablename__ = "solicitud_ot"
    n_solicitud = db.Column(db.String(100))
    descripcion_problema = db.Column(db.Text)
    tipo_solicitud = db.Column(db.String(100))
    modulo = db.Column(db.String(100))
    habitacion = db.Column(db.String(100))
    tipo_turno = db.Column(db.String(100))
    jornada = db.Column(db.String(100))
    via_solicitud = db.Column(db.String(100))
    correo_usuario = db.Column(db.String(255))
    tipo_tarea = db.Column(db.String(100))
    ot = db.Column(db.String(100))
    fecha_inicio = db.Column(db.Date)
    estado = db.Column(db.String(100))
    tiempo_respuesta_sec = db.Column(db.Integer)
    satisfaccion_reclamo = db.Column(db.String(100))
    motivo = db.Column(db.String(255))
    observacion = db.Column(db.Text)

# Reclamos
class ReclamoEntry(BaseModel):
    __tablename__ = "reclamos"
    n_solicitud = db.Column(db.String(100))
    fecha = db.Column(db.Date, nullable=False)
    id_interno = db.Column(db.String(100))
    empresa_contratista = db.Column(db.String(255))
    descripcion_problema = db.Column(db.Text)
    tipo_solicitud = db.Column(db.String(100))
    pabellon = db.Column(db.String(100))
    habitacion = db.Column(db.String(100))
    via_solicitud = db.Column(db.String(100))
    ingresar_contacto = db.Column(db.String(255))
    nombre_usuario = db.Column(db.String(255))
    responsable = db.Column(db.String(255))
    estatus = db.Column(db.String(100))
    notificacion_usuario = db.Column(db.String(255))
    plan_accion = db.Column(db.Text)

# ---- NUEVOS 5 MÓDULOS ----

# 1) Activación de alarma
class ActivacionAlarmaEntry(BaseModel):
    __tablename__ = "activacion_alarma"
    modulo = db.Column(db.String(100))
    habitacion = db.Column(db.String(100))
    recepcionista = db.Column(db.String(255))
    fecha = db.Column(db.Date, nullable=False)
    empresa = db.Column(db.String(255))
    id_interno = db.Column(db.String(100))
    co = db.Column(db.String(100))
    aviso_mantencion_h = db.Column(db.Float)
    llegada_mantencion_h = db.Column(db.Float)
    aviso_lider_h = db.Column(db.Float)
    llegada_lider_h = db.Column(db.Float)
    hora_reporte_salfa = db.Column(db.Time)
    tipo_evento = db.Column(db.String(100))
    tipo_actividad = db.Column(db.String(100))
    fecha_reporte = db.Column(db.Date)
    turno = db.Column(db.String(100))
    observaciones = db.Column(db.Text)

# 2) Extensión y excepción
class ExtensionExcepcionEntry(BaseModel):
    __tablename__ = "extension_excepcion"
    fecha_solicitud = db.Column(db.Date, nullable=False)
    id_interno = db.Column(db.String(100))
    empresa = db.Column(db.String(255))
    co = db.Column(db.String(100))
    gerencia = db.Column(db.String(100))
    proyecto = db.Column(db.String(255))
    cantidad_clientes = db.Column(db.Integer)
    desde = db.Column(db.Date)
    hasta = db.Column(db.Date)
    aprobador = db.Column(db.String(255))
    observacion = db.Column(db.Text)

# 3) Registro Onboarding (con PDF)
class OnboardingEntry(BaseModel):
    __tablename__ = "onboarding"
    fecha_hora = db.Column(db.DateTime, nullable=False)
    nombre = db.Column(db.String(255), nullable=False)
    rut = db.Column(db.String(50), nullable=False)
    empresa = db.Column(db.String(255))
    id_interno = db.Column(db.String(100))
    archivo_pdf = db.Column(db.String(500))  # ruta relativa en uploads

# 4) Apertura de habitación
class AperturaHabitacionEntry(BaseModel):
    __tablename__ = "apertura_habitacion"
    fecha = db.Column(db.Date, nullable=False)
    habitacion = db.Column(db.String(100), nullable=False)
    hora = db.Column(db.Time)
    responsable = db.Column(db.String(255))
    estado_chapa = db.Column(db.Text)

# 5) Cumplimiento EECC
class EECCCumplimientoEntry(BaseModel):
    __tablename__ = "eecc_cumplimiento"
    empresa = db.Column(db.String(255), nullable=False)
    n_contrato = db.Column(db.String(100))
    co = db.Column(db.String(100))
    correo = db.Column(db.String(255))
    id_interno = db.Column(db.String(100))
    turno = db.Column(db.String(100))

# ---------------------------
# Inicialización DB
# ---------------------------
with app.app_context():
    db.create_all()

# ---------------------------
# Constantes y helpers vista
# ---------------------------
CURRENT_YEAR = date.today().year
WEEK_MAP = build_week_map(CURRENT_YEAR)  # {num: (desde, hasta)}

def week_of_date(d: date) -> int:
    if not d: return None
    return d.isocalendar().week

# ---------------------------
# Rutas
# ---------------------------
@app.route("/")
def index():
    return redirect(url_for("panel", tab="censo"))

@app.route("/dashboard")
def dashboard():
    # Placeholder simple (puedes enriquecerlo más tarde)
    total_censo = db.session.query(func.sum(CensoEntry.total)).scalar() or 0
    total_eventos = db.session.query(func.count(EventoSeguridadEntry.id)).scalar() or 0
    return render_template("base.html", current_tab=None, content=f"Dashboard: Censo total={total_censo}, Eventos={total_eventos}")

@app.route("/panel", methods=["GET", "POST"])
def panel():
    tab = request.args.get("tab", "censo")

    if request.method == "POST":
        try:
            if tab == "censo":
                fecha = parse_date(request.form.get("fecha"))
                dia = request.form.get("censo_dia") or 0
                noche = request.form.get("censo_noche") or 0
                total = request.form.get("total")
                if not total:
                    total = int(dia) + int(noche)
                entry = CensoEntry(fecha=fecha, censo_dia=int(dia), censo_noche=int(noche), total=int(total))
                db.session.add(entry)

            elif tab == "eventos":
                entry = EventoSeguridadEntry(
                    fecha=parse_date(request.form.get("fecha")),
                    horario=request.form.get("horario") or "",
                    que_ocurrio=request.form.get("que_ocurrio") or "",
                    nombre_afectado=request.form.get("nombre_afectado") or None,
                    accion=request.form.get("accion") or None
                )
                db.session.add(entry)

            elif tab == "duplicidades":
                fecha = parse_date(request.form.get("fecha"))
                semana = int(request.form.get("semana") or (week_of_date(fecha) or 0))
                entry = DuplicidadEntry(
                    semana=semana, fecha=fecha,
                    id_interno=request.form.get("id"),
                    empresa_contratista=request.form.get("empresa_contratista"),
                    descripcion_problema=request.form.get("descripcion_problema"),
                    tipo_riesgo=request.form.get("tipo_riesgo"),
                    pabellon=request.form.get("pabellon"),
                    habitacion=request.form.get("habitacion"),
                    ingresar_contacto=request.form.get("ingresar_contacto"),
                    nombre_usuario=request.form.get("nombre_usuario"),
                    responsable=request.form.get("responsable"),
                    estatus=request.form.get("estatus"),
                    notificacion_usuario=request.form.get("notificacion_usuario"),
                    plan_accion=request.form.get("plan_accion"),
                    fecha_cierre=parse_date(request.form.get("fecha_cierre"))
                )
                db.session.add(entry)

            elif tab == "encuesta":
                fh = parse_datetime_local(request.form.get("fecha_hora"))
                # Respuestas + puntajes
                qR = {}
                qP = {}
                for i in range(1, 5 + 1):
                    qR[i] = request.form.get(f"q{i}_respuesta")
                    raw = request.form.get(f"q{i}_puntaje")
                    qP[i] = int(raw) if raw not in (None, "",) else None
                puntajes = [p for p in qP.values() if p is not None]
                total = sum(puntajes) if puntajes else None
                promedio = (total / len(puntajes)) if puntajes else None
                entry = EncuestaEntry(
                    fecha_hora=fh,
                    q1_respuesta=qR[1], q1_puntaje=qP[1],
                    q2_respuesta=qR[2], q2_puntaje=qP[2],
                    q3_respuesta=qR[3], q3_puntaje=qP[3],
                    q4_respuesta=qR[4], q4_puntaje=qP[4],
                    q5_respuesta=qR[5], q5_puntaje=qP[5],
                    total=total, promedio=promedio,
                    comentarios=request.form.get("comentarios")
                )
                db.session.add(entry)

            elif tab == "atencion":
                entry = AtencionPublicoEntry(
                    fecha=parse_date(request.form.get("fecha")),
                    tiempo_promedio_sec=parse_mmss_to_seconds(request.form.get("tiempo_promedio")),
                    cantidad=int(request.form.get("cantidad") or 0),
                )
                db.session.add(entry)

            elif tab == "robos":
                entry = RoboHurtoEntry(
                    fecha=parse_date(request.form.get("fecha")),
                    hora=parse_time_hhmm(request.form.get("hora")),
                    modulo=request.form.get("modulo"),
                    habitacion=request.form.get("habitacion"),
                    empresa=request.form.get("empresa"),
                    nombre_cliente=request.form.get("nombre_cliente"),
                    rut=request.form.get("rut"),
                    medio_reclamo=request.form.get("medio_reclamo"),
                    especies=request.form.get("especies"),
                    observaciones=request.form.get("observaciones"),
                    recepciona=request.form.get("recepciona"),
                )
                db.session.add(entry)

            elif tab == "miscelaneo":
                entry = MiscelaneoEntry(
                    ot=request.form.get("ot"),
                    division=request.form.get("division"),
                    area=request.form.get("area"),
                    lugar=request.form.get("lugar"),
                    ubicacion=request.form.get("ubicacion"),
                    disciplina=request.form.get("disciplina"),
                    especialidad=request.form.get("especialidad"),
                    falla=request.form.get("falla"),
                    empresa=request.form.get("empresa"),
                    fecha_creacion=parse_date(request.form.get("fecha_creacion")),
                    fecha_inicio=parse_date(request.form.get("fecha_inicio")),
                    fecha_termino=parse_date(request.form.get("fecha_termino")),
                    fecha_aprobacion=parse_date(request.form.get("fecha_aprobacion")),
                    estado=request.form.get("estado"),
                    comentario=request.form.get("comentario"),
                )
                db.session.add(entry)

            elif tab == "desviaciones":
                entry = DesviacionEntry(
                    n_solicitud=request.form.get("n_solicitud"),
                    fecha=parse_date(request.form.get("fecha")),
                    id_interno=request.form.get("id_interno"),
                    empresa_contratista=request.form.get("empresa_contratista"),
                    descripcion_problema=request.form.get("descripcion_problema"),
                    tipo_riesgo=request.form.get("tipo_riesgo"),
                    tipo_solicitud=request.form.get("tipo_solicitud"),
                    pabellon=request.form.get("pabellon"),
                    habitacion=request.form.get("habitacion"),
                    via_solicitud=request.form.get("via_solicitud"),
                    quien_informa=request.form.get("quien_informa"),
                    riesgo_material=request.form.get("riesgo_material"),
                    correo_destino=request.form.get("correo_destino"),
                )
                db.session.add(entry)

            elif tab == "solicitud_ot":
                entry = SolicitudOTEntry(
                    n_solicitud=request.form.get("n_solicitud"),
                    descripcion_problema=request.form.get("descripcion_problema"),
                    tipo_solicitud=request.form.get("tipo_solicitud"),
                    modulo=request.form.get("modulo"),
                    habitacion=request.form.get("habitacion"),
                    tipo_turno=request.form.get("tipo_turno"),
                    jornada=request.form.get("jornada"),
                    via_solicitud=request.form.get("via_solicitud"),
                    correo_usuario=request.form.get("correo_usuario"),
                    tipo_tarea=request.form.get("tipo_tarea"),
                    ot=request.form.get("ot"),
                    fecha_inicio=parse_date(request.form.get("fecha_inicio")),
                    estado=request.form.get("estado"),
                    tiempo_respuesta_sec=parse_mmss_to_seconds(request.form.get("tiempo_respuesta")),
                    satisfaccion_reclamo=request.form.get("satisfaccion_reclamo"),
                    motivo=request.form.get("motivo"),
                    observacion=request.form.get("observacion"),
                )
                db.session.add(entry)

            elif tab == "reclamos":
                entry = ReclamoEntry(
                    n_solicitud=request.form.get("n_solicitud"),
                    fecha=parse_date(request.form.get("fecha")),
                    id_interno=request.form.get("id_interno"),
                    empresa_contratista=request.form.get("empresa_contratista"),
                    descripcion_problema=request.form.get("descripcion_problema"),
                    tipo_solicitud=request.form.get("tipo_solicitud"),
                    pabellon=request.form.get("pabellon"),
                    habitacion=request.form.get("habitacion"),
                    via_solicitud=request.form.get("via_solicitud"),
                    ingresar_contacto=request.form.get("ingresar_contacto"),
                    nombre_usuario=request.form.get("nombre_usuario"),
                    responsable=request.form.get("responsable"),
                    estatus=request.form.get("estatus"),
                    notificacion_usuario=request.form.get("notificacion_usuario"),
                    plan_accion=request.form.get("plan_accion"),
                )
                db.session.add(entry)

            # ---- NUEVOS 5 ----
            elif tab == "activacion_alarma":
                entry = ActivacionAlarmaEntry(
                    modulo=request.form.get("modulo"),
                    habitacion=request.form.get("habitacion"),
                    recepcionista=request.form.get("recepcionista"),
                    fecha=parse_date(request.form.get("fecha")),
                    empresa=request.form.get("empresa"),
                    id_interno=request.form.get("id_interno"),
                    co=request.form.get("co"),
                    aviso_mantencion_h=float(request.form.get("aviso_mantencion_h") or 0) if request.form.get("aviso_mantencion_h") else None,
                    llegada_mantencion_h=float(request.form.get("llegada_mantencion_h") or 0) if request.form.get("llegada_mantencion_h") else None,
                    aviso_lider_h=float(request.form.get("aviso_lider_h") or 0) if request.form.get("aviso_lider_h") else None,
                    llegada_lider_h=float(request.form.get("llegada_lider_h") or 0) if request.form.get("llegada_lider_h") else None,
                    hora_reporte_salfa=parse_time_hhmm(request.form.get("hora_reporte_salfa")),
                    tipo_evento=request.form.get("tipo_evento"),
                    tipo_actividad=request.form.get("tipo_actividad"),
                    fecha_reporte=parse_date(request.form.get("fecha_reporte")),
                    turno=request.form.get("turno"),
                    observaciones=request.form.get("observaciones"),
                )
                db.session.add(entry)

            elif tab == "extension_excepcion":
                entry = ExtensionExcepcionEntry(
                    fecha_solicitud=parse_date(request.form.get("fecha_solicitud")),
                    id_interno=request.form.get("id_interno"),
                    empresa=request.form.get("empresa"),
                    co=request.form.get("co"),
                    gerencia=request.form.get("gerencia"),
                    proyecto=request.form.get("proyecto"),
                    cantidad_clientes=int(request.form.get("cantidad_clientes") or 0) if request.form.get("cantidad_clientes") else None,
                    desde=parse_date(request.form.get("desde")),
                    hasta=parse_date(request.form.get("hasta")),
                    aprobador=request.form.get("aprobador"),
                    observacion=request.form.get("observacion"),
                )
                db.session.add(entry)

            elif tab == "onboarding":
                fh = parse_datetime_local(request.form.get("fecha_hora"))
                fname_saved = None
                if "archivo_pdf" in request.files:
                    file = request.files["archivo_pdf"]
                    if file and file.filename:
                        fname = secure_filename(file.filename)
                        # Evitamos choques de nombre
                        stem, ext = os.path.splitext(fname)
                        if ext.lower() != ".pdf":
                            flash("El archivo debe ser PDF.")
                            return redirect(url_for("panel", tab=tab))
                        final_name = f"{stem}_{int(datetime.utcnow().timestamp())}{ext}"
                        dst = os.path.join(app.config["UPLOAD_FOLDER_ONBOARD"], final_name)
                        file.save(dst)
                        fname_saved = final_name
                entry = OnboardingEntry(
                    fecha_hora=fh,
                    nombre=request.form.get("nombre"),
                    rut=request.form.get("rut"),
                    empresa=request.form.get("empresa"),
                    id_interno=request.form.get("id_interno"),
                    archivo_pdf=fname_saved
                )
                db.session.add(entry)

            elif tab == "apertura_habitacion":
                entry = AperturaHabitacionEntry(
                    fecha=parse_date(request.form.get("fecha")),
                    habitacion=request.form.get("habitacion"),
                    hora=parse_time_hhmm(request.form.get("hora")),
                    responsable=request.form.get("responsable"),
                    estado_chapa=request.form.get("estado_chapa"),
                )
                db.session.add(entry)

            elif tab == "eecc":
                entry = EECCCumplimientoEntry(
                    empresa=request.form.get("empresa"),
                    n_contrato=request.form.get("n_contrato"),
                    co=request.form.get("co"),
                    correo=request.form.get("correo"),
                    id_interno=request.form.get("id_interno"),
                    turno=request.form.get("turno"),
                )
                db.session.add(entry)

            db.session.commit()
            flash("Registro guardado correctamente.")
        except Exception as e:
            db.session.rollback()
            flash(f"Error al guardar: {e}")

        return redirect(url_for("panel", tab=tab))

    return render_template("panel.html", tab=tab, current_tab=tab, week_map=WEEK_MAP)

@app.route("/registros")
def registros():
    d_from = parse_date(request.args.get("from", ""))
    d_to = parse_date(request.args.get("to", ""))
    semana_sel = request.args.get("semana")
    vista = request.args.get("vista", "censo")

    # Censo
    q = CensoEntry.query
    if d_from: q = q.filter(CensoEntry.fecha >= d_from)
    if d_to:   q = q.filter(CensoEntry.fecha <= d_to)
    census = q.order_by(CensoEntry.fecha.desc()).all()

    # Eventos
    q = EventoSeguridadEntry.query
    if d_from: q = q.filter(EventoSeguridadEntry.fecha >= d_from)
    if d_to:   q = q.filter(EventoSeguridadEntry.fecha <= d_to)
    eventos = q.order_by(EventoSeguridadEntry.fecha.desc()).all()

    # Duplicidades
    q = DuplicidadEntry.query
    if d_from: q = q.filter(DuplicidadEntry.fecha >= d_from)
    if d_to:   q = q.filter(DuplicidadEntry.fecha <= d_to)
    if semana_sel:
        try:
            semana_i = int(semana_sel)
            q = q.filter(DuplicidadEntry.semana == semana_i)
        except:
            pass
    duplics = q.order_by(DuplicidadEntry.fecha.desc()).all()

    # Encuestas
    encuestas = EncuestaEntry.query.order_by(EncuestaEntry.fecha_hora.desc()).all()

    # Atención
    q = AtencionPublicoEntry.query
    if d_from: q = q.filter(AtencionPublicoEntry.fecha >= d_from)
    if d_to:   q = q.filter(AtencionPublicoEntry.fecha <= d_to)
    atenciones = q.order_by(AtencionPublicoEntry.fecha.desc()).all()

    # Robos
    q = RoboHurtoEntry.query
    if d_from: q = q.filter(RoboHurtoEntry.fecha >= d_from)
    if d_to:   q = q.filter(RoboHurtoEntry.fecha <= d_to)
    robos = q.order_by(RoboHurtoEntry.fecha.desc()).all()

    # Misceláneo
    miscelaneo = MiscelaneoEntry.query.order_by(MiscelaneoEntry.id.desc()).all()

    # Desviaciones
    q = DesviacionEntry.query
    if d_from: q = q.filter(DesviacionEntry.fecha >= d_from)
    if d_to:   q = q.filter(DesviacionEntry.fecha <= d_to)
    desviaciones = q.order_by(DesviacionEntry.fecha.desc()).all()

    # Solicitud y OT
    solicitudes_ot = SolicitudOTEntry.query.order_by(SolicitudOTEntry.id.desc()).all()

    # Reclamos
    q = ReclamoEntry.query
    if d_from: q = q.filter(ReclamoEntry.fecha >= d_from)
    if d_to:   q = q.filter(ReclamoEntry.fecha <= d_to)
    reclamos = q.order_by(ReclamoEntry.fecha.desc()).all()

    # ---- NUEVOS 5 ----
    q = ActivacionAlarmaEntry.query
    if d_from: q = q.filter(ActivacionAlarmaEntry.fecha >= d_from)
    if d_to:   q = q.filter(ActivacionAlarmaEntry.fecha <= d_to)
    activaciones = q.order_by(ActivacionAlarmaEntry.fecha.desc()).all()

    q = ExtensionExcepcionEntry.query
    if d_from: q = q.filter(ExtensionExcepcionEntry.fecha_solicitud >= d_from)
    if d_to:   q = q.filter(ExtensionExcepcionEntry.fecha_solicitud <= d_to)
    extensiones = q.order_by(ExtensionExcepcionEntry.fecha_solicitud.desc()).all()

    onboardings = OnboardingEntry.query.order_by(OnboardingEntry.fecha_hora.desc()).all()

    q = AperturaHabitacionEntry.query
    if d_from: q = q.filter(AperturaHabitacionEntry.fecha >= d_from)
    if d_to:   q = q.filter(AperturaHabitacionEntry.fecha <= d_to)
    aperturas = q.order_by(AperturaHabitacionEntry.fecha.desc()).all()

    eeccs = EECCCumplimientoEntry.query.order_by(EECCCumplimientoEntry.id.desc()).all()

    return render_template(
        "list.html",
        semana_sel=int(semana_sel) if (semana_sel and semana_sel.isdigit()) else None,
        d_from=d_from, d_to=d_to, week_map=WEEK_MAP,
        census=census, eventos=eventos, duplics=duplics,
        encuestas=encuestas, atenciones=atenciones,
        robos=robos, miscelaneo=miscelaneo, desviaciones=desviaciones,
        solicitudes_ot=solicitudes_ot, reclamos=reclamos,
        activaciones=activaciones, extensiones=extensiones,
        onboardings=onboardings, aperturas=aperturas, eeccs=eeccs,
        vista=vista, current_tab=None
    )

# ---------------------------
# Descarga CSV por entidad
# ---------------------------
def rows_for_entity(entity: str, d_from: date = None, d_to: date = None):
    # Repetimos la lógica de filtros básicos (fecha) donde aplique
    if entity == "censo":
        q = CensoEntry.query
        if d_from: q = q.filter(CensoEntry.fecha >= d_from)
        if d_to:   q = q.filter(CensoEntry.fecha <= d_to)
        items = q.order_by(CensoEntry.fecha.desc()).all()
        headers = ["id","fecha","censo_dia","censo_noche","total"]
        rows = [[r.id,r.fecha,r.censo_dia,r.censo_noche,r.total] for r in items]
        return headers, rows

    if entity == "eventos":
        q = EventoSeguridadEntry.query
        if d_from: q = q.filter(EventoSeguridadEntry.fecha >= d_from)
        if d_to:   q = q.filter(EventoSeguridadEntry.fecha <= d_to)
        items = q.order_by(EventoSeguridadEntry.fecha.desc()).all()
        headers = ["id","fecha","horario","que_ocurrio","nombre_afectado","accion"]
        rows = [[r.id,r.fecha,r.horario,r.que_ocurrio,r.nombre_afectado,r.accion] for r in items]
        return headers, rows

    if entity == "duplicidades":
        q = DuplicidadEntry.query
        if d_from: q = q.filter(DuplicidadEntry.fecha >= d_from)
        if d_to:   q = q.filter(DuplicidadEntry.fecha <= d_to)
        items = q.order_by(DuplicidadEntry.fecha.desc()).all()
        headers = ["id","semana","fecha","id_interno","empresa_contratista","descripcion_problema",
                   "tipo_riesgo","pabellon","habitacion","ingresar_contacto","nombre_usuario","responsable",
                   "estatus","notificacion_usuario","plan_accion","fecha_cierre"]
        rows = [[r.id,r.semana,r.fecha,r.id_interno,r.empresa_contratista,r.descripcion_problema,
                 r.tipo_riesgo,r.pabellon,r.habitacion,r.ingresar_contacto,r.nombre_usuario,r.responsable,
                 r.estatus,r.notificacion_usuario,r.plan_accion,r.fecha_cierre] for r in items]
        return headers, rows

    if entity == "encuestas":
        items = EncuestaEntry.query.order_by(EncuestaEntry.fecha_hora.desc()).all()
        headers = ["id","fecha_hora",
                   "q1_respuesta","q1_puntaje","q2_respuesta","q2_puntaje","q3_respuesta","q3_puntaje",
                   "q4_respuesta","q4_puntaje","q5_respuesta","q5_puntaje","total","promedio","comentarios"]
        rows = [[r.id,r.fecha_hora,r.q1_respuesta,r.q1_puntaje,r.q2_respuesta,r.q2_puntaje,
                 r.q3_respuesta,r.q3_puntaje,r.q4_respuesta,r.q4_puntaje,r.q5_respuesta,r.q5_puntaje,
                 r.total,r.promedio,r.comentarios] for r in items]
        return headers, rows

    if entity == "atencion":
        q = AtencionPublicoEntry.query
        if d_from: q = q.filter(AtencionPublicoEntry.fecha >= d_from)
        if d_to:   q = q.filter(AtencionPublicoEntry.fecha <= d_to)
        items = q.order_by(AtencionPublicoEntry.fecha.desc()).all()
        headers = ["id","fecha","tiempo_promedio_mmss","cantidad"]
        rows = [[r.id,r.fecha,seconds_to_mmss(r.tiempo_promedio_sec),r.cantidad] for r in items]
        return headers, rows

    if entity == "robos":
        q = RoboHurtoEntry.query
        if d_from: q = q.filter(RoboHurtoEntry.fecha >= d_from)
        if d_to:   q = q.filter(RoboHurtoEntry.fecha <= d_to)
        items = q.order_by(RoboHurtoEntry.fecha.desc()).all()
        headers = ["id","fecha","hora","modulo","habitacion","empresa","nombre_cliente","rut",
                   "medio_reclamo","especies","observaciones","recepciona"]
        rows = [[r.id,r.fecha,(r.hora.strftime("%H:%M") if r.hora else ""),r.modulo,r.habitacion,r.empresa,
                 r.nombre_cliente,r.rut,r.medio_reclamo,r.especies,r.observaciones,r.recepciona] for r in items]
        return headers, rows

    if entity == "miscelaneo":
        items = MiscelaneoEntry.query.order_by(MiscelaneoEntry.id.desc()).all()
        headers = ["id","ot","division","area","lugar","ubicacion","disciplina","especialidad","falla",
                   "empresa","fecha_creacion","fecha_inicio","fecha_termino","fecha_aprobacion","estado","comentario"]
        rows = [[r.id,r.ot,r.division,r.area,r.lugar,r.ubicacion,r.disciplina,r.especialidad,r.falla,r.empresa,
                 r.fecha_creacion,r.fecha_inicio,r.fecha_termino,r.fecha_aprobacion,r.estado,r.comentario] for r in items]
        return headers, rows

    if entity == "desviaciones":
        q = DesviacionEntry.query
        if d_from: q = q.filter(DesviacionEntry.fecha >= d_from)
        if d_to:   q = q.filter(DesviacionEntry.fecha <= d_to)
        items = q.order_by(DesviacionEntry.fecha.desc()).all()
        headers = ["id","n_solicitud","fecha","id_interno","empresa_contratista","descripcion_problema","tipo_riesgo",
                   "tipo_solicitud","pabellon","habitacion","via_solicitud","quien_informa","riesgo_material","correo_destino"]
        rows = [[r.id,r.n_solicitud,r.fecha,r.id_interno,r.empresa_contratista,r.descripcion_problema,r.tipo_riesgo,
                 r.tipo_solicitud,r.pabellon,r.habitacion,r.via_solicitud,r.quien_informa,r.riesgo_material,r.correo_destino] for r in items]
        return headers, rows

    if entity == "solicitud_ot":
        items = SolicitudOTEntry.query.order_by(SolicitudOTEntry.id.desc()).all()
        headers = ["id","n_solicitud","descripcion_problema","tipo_solicitud","modulo","habitacion","tipo_turno",
                   "jornada","via_solicitud","correo_usuario","tipo_tarea","ot","fecha_inicio","estado",
                   "tiempo_respuesta_mmss","satisfaccion_reclamo","motivo","observacion"]
        rows = [[r.id,r.n_solicitud,r.descripcion_problema,r.tipo_solicitud,r.modulo,r.habitacion,r.tipo_turno,
                 r.jornada,r.via_solicitud,r.correo_usuario,r.tipo_tarea,r.ot,r.fecha_inicio,r.estado,
                 seconds_to_mmss(r.tiempo_respuesta_sec),r.satisfaccion_reclamo,r.motivo,r.observacion] for r in items]
        return headers, rows

    if entity == "reclamos":
        q = ReclamoEntry.query
        if d_from: q = q.filter(ReclamoEntry.fecha >= d_from)
        if d_to:   q = q.filter(ReclamoEntry.fecha <= d_to)
        items = q.order_by(ReclamoEntry.fecha.desc()).all()
        headers = ["id","n_solicitud","fecha","id_interno","empresa_contratista","descripcion_problema","tipo_solicitud",
                   "pabellon","habitacion","via_solicitud","ingresar_contacto","nombre_usuario","responsable",
                   "estatus","notificacion_usuario","plan_accion"]
        rows = [[r.id,r.n_solicitud,r.fecha,r.id_interno,r.empresa_contratista,r.descripcion_problema,r.tipo_solicitud,
                 r.pabellon,r.habitacion,r.via_solicitud,r.ingresar_contacto,r.nombre_usuario,r.responsable,
                 r.estatus,r.notificacion_usuario,r.plan_accion] for r in items]
        return headers, rows

    # ---- nuevos ----
    if entity == "activacion_alarma":
        q = ActivacionAlarmaEntry.query
        if d_from: q = q.filter(ActivacionAlarmaEntry.fecha >= d_from)
        if d_to:   q = q.filter(ActivacionAlarmaEntry.fecha <= d_to)
        items = q.order_by(ActivacionAlarmaEntry.fecha.desc()).all()
        headers = ["id","modulo","habitacion","recepcionista","fecha","empresa","id_interno","co",
                   "aviso_mantencion_h","llegada_mantencion_h","aviso_lider_h","llegada_lider_h",
                   "hora_reporte_salfa","tipo_evento","tipo_actividad","fecha_reporte","turno","observaciones"]
        rows = [[r.id,r.modulo,r.habitacion,r.recepcionista,r.fecha,r.empresa,r.id_interno,r.co,
                 r.aviso_mantencion_h,r.llegada_mantencion_h,r.aviso_lider_h,r.llegada_lider_h,
                 (r.hora_reporte_salfa.strftime("%H:%M") if r.hora_reporte_salfa else ""),r.tipo_evento,r.tipo_actividad,
                 r.fecha_reporte,r.turno,r.observaciones] for r in items]
        return headers, rows

    if entity == "extension_excepcion":
        q = ExtensionExcepcionEntry.query
        if d_from: q = q.filter(ExtensionExcepcionEntry.fecha_solicitud >= d_from)
        if d_to:   q = q.filter(ExtensionExcepcionEntry.fecha_solicitud <= d_to)
        items = q.order_by(ExtensionExcepcionEntry.fecha_solicitud.desc()).all()
        headers = ["id","fecha_solicitud","id_interno","empresa","co","gerencia","proyecto","cantidad_clientes",
                   "desde","hasta","aprobador","observacion"]
        rows = [[r.id,r.fecha_solicitud,r.id_interno,r.empresa,r.co,r.gerencia,r.proyecto,r.cantidad_clientes,
                 r.desde,r.hasta,r.aprobador,r.observacion] for r in items]
        return headers, rows

    if entity == "onboarding":
        items = OnboardingEntry.query.order_by(OnboardingEntry.fecha_hora.desc()).all()
        headers = ["id","fecha_hora","nombre","rut","empresa","id_interno","archivo_pdf"]
        rows = [[r.id,r.fecha_hora,r.nombre,r.rut,r.empresa,r.id_interno,r.archivo_pdf] for r in items]
        return headers, rows

    if entity == "apertura_habitacion":
        q = AperturaHabitacionEntry.query
        if d_from: q = q.filter(AperturaHabitacionEntry.fecha >= d_from)
        if d_to:   q = q.filter(AperturaHabitacionEntry.fecha <= d_to)
        items = q.order_by(AperturaHabitacionEntry.fecha.desc()).all()
        headers = ["id","fecha","habitacion","hora","responsable","estado_chapa"]
        rows = [[r.id,r.fecha,r.habitacion,(r.hora.strftime("%H:%M") if r.hora else ""),r.responsable,r.estado_chapa] for r in items]
        return headers, rows

    if entity == "eecc":
        items = EECCCumplimientoEntry.query.order_by(EECCCumplimientoEntry.id.desc()).all()
        headers = ["id","empresa","n_contrato","co","correo","id_interno","turno"]
        rows = [[r.id,r.empresa,r.n_contrato,r.co,r.correo,r.id_interno,r.turno] for r in items]
        return headers, rows

    return None, None

@app.route("/download/<entity>")
def download_entity(entity):
    d_from = parse_date(request.args.get("from", ""))
    d_to = parse_date(request.args.get("to", ""))

    headers, rows = rows_for_entity(entity, d_from, d_to)
    if headers is None:
        flash("Entidad no reconocida.")
        return redirect(url_for("registros"))

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(headers)
    writer.writerows(rows)
    output.seek(0)

    filename = f"{entity}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    return send_file(
        io.BytesIO(output.getvalue().encode("utf-8-sig")),
        mimetype="text/csv",
        as_attachment=True,
        download_name=filename
    )

# ---------------------------
# Importación desde Excel
# ---------------------------
try:
    import pandas as pd
except Exception:
    pd = None

ENTITY_MODEL_MAP = {
    "censo": (CensoEntry, ["fecha","censo_dia","censo_noche","total"]),
    "eventos": (EventoSeguridadEntry, ["fecha","horario","que_ocurrio","nombre_afectado","accion"]),
    "duplicidades": (DuplicidadEntry, ["semana","fecha","id_interno","empresa_contratista","descripcion_problema","tipo_riesgo","pabellon","habitacion","ingresar_contacto","nombre_usuario","responsable","estatus","notificacion_usuario","plan_accion","fecha_cierre"]),
    "encuesta": (EncuestaEntry, ["fecha_hora","q1_respuesta","q1_puntaje","q2_respuesta","q2_puntaje","q3_respuesta","q3_puntaje","q4_respuesta","q4_puntaje","q5_respuesta","q5_puntaje","comentarios"]),
    "atencion": (AtencionPublicoEntry, ["fecha","tiempo_promedio_mmss","cantidad"]),
    "robos": (RoboHurtoEntry, ["fecha","hora","modulo","habitacion","empresa","nombre_cliente","rut","medio_reclamo","especies","observaciones","recepciona"]),
    "miscelaneo": (MiscelaneoEntry, ["ot","division","area","lugar","ubicacion","disciplina","especialidad","falla","empresa","fecha_creacion","fecha_inicio","fecha_termino","fecha_aprobacion","estado","comentario"]),
    "desviaciones": (DesviacionEntry, ["n_solicitud","fecha","id_interno","empresa_contratista","descripcion_problema","tipo_riesgo","tipo_solicitud","pabellon","habitacion","via_solicitud","quien_informa","riesgo_material","correo_destino"]),
    "solicitud_ot": (SolicitudOTEntry, ["n_solicitud","descripcion_problema","tipo_solicitud","modulo","habitacion","tipo_turno","jornada","via_solicitud","correo_usuario","tipo_tarea","ot","fecha_inicio","estado","tiempo_respuesta_mmss","satisfaccion_reclamo","motivo","observacion"]),
    "reclamos": (ReclamoEntry, ["n_solicitud","fecha","id_interno","empresa_contratista","descripcion_problema","tipo_solicitud","pabellon","habitacion","via_solicitud","ingresar_contacto","nombre_usuario","responsable","estatus","notificacion_usuario","plan_accion"]),
    # nuevos
    "activacion_alarma": (ActivacionAlarmaEntry, ["modulo","habitacion","recepcionista","fecha","empresa","id_interno","co","aviso_mantencion_h","llegada_mantencion_h","aviso_lider_h","llegada_lider_h","hora_reporte_salfa","tipo_evento","tipo_actividad","fecha_reporte","turno","observaciones"]),
    "extension_excepcion": (ExtensionExcepcionEntry, ["fecha_solicitud","id_interno","empresa","co","gerencia","proyecto","cantidad_clientes","desde","hasta","aprobador","observacion"]),
    "onboarding": (OnboardingEntry, ["fecha_hora","nombre","rut","empresa","id_interno","archivo_pdf"]),
    "apertura_habitacion": (AperturaHabitacionEntry, ["fecha","habitacion","hora","responsable","estado_chapa"]),
    "eecc": (EECCCumplimientoEntry, ["empresa","n_contrato","co","correo","id_interno","turno"]),
}

@app.route("/import/<entity>", methods=["POST"])
def import_xlsx(entity):
    if pd is None:
        flash("Pandas no disponible en el entorno para importar Excel.")
        return redirect(url_for("panel", tab=entity if entity in ("encuesta", "atencion") else "censo"))

    if entity not in ENTITY_MODEL_MAP:
        flash("Entidad no reconocida.")
        return redirect(url_for("registros"))

    file = request.files.get("file")
    if not file or not file.filename.lower().endswith(".xlsx"):
        flash("Sube un archivo .xlsx válido.")
        return redirect(url_for("panel", tab=entity))

    Model, columns = ENTITY_MODEL_MAP[entity]
    try:
        df = pd.read_excel(file)
    except Exception as e:
        flash(f"No se pudo leer el Excel: {e}")
        return redirect(url_for("panel", tab=entity))

    # Normalizamos nombres de columnas
    df.columns = [str(c).strip() for c in df.columns]

    created = 0
    try:
        for _, row in df.iterrows():
            data = {}

            def g(col):  # helper para obtener celda
                return row.get(col) if col in df.columns else None

            # Mapeos específicos (fechas, horas, mm:ss)
            for col in columns:
                val = g(col)

                if col in ("fecha","fecha_cierre","fecha_creacion","fecha_inicio","fecha_termino","fecha_aprobacion","fecha_solicitud","desde","hasta","fecha_reporte"):
                    if isinstance(val, (datetime, date)):
                        data[col] = val.date() if isinstance(val, datetime) else val
                    elif isinstance(val, str):
                        data[col] = parse_date(val)
                    else:
                        data[col] = None

                elif col in ("fecha_hora",):
                    if isinstance(val, datetime):
                        data[col] = val
                    elif isinstance(val, str):
                        dt = parse_datetime_local(val) or (datetime.strptime(val, "%Y-%m-%d %H:%M") if ":" in val else None)
                        data[col] = dt
                    else:
                        data[col] = None

                elif col in ("hora","hora_reporte_salfa"):
                    if isinstance(val, time):
                        data[col] = val
                    elif isinstance(val, str):
                        data[col] = parse_time_hhmm(val)
                    else:
                        data[col] = None

                elif col in ("tiempo_promedio_mmss","tiempo_respuesta_mmss"):
                    data[col.replace("_mmss","_sec")] = parse_mmss_to_seconds(val if isinstance(val, str) else (str(val) if not pd.isna(val) else None))

                elif col in ("q1_puntaje","q2_puntaje","q3_puntaje","q4_puntaje","q5_puntaje","cantidad_clientes","cantidad","censo_dia","censo_noche","total"):
                    if val == "" or (pd.isna(val) if hasattr(pd, "isna") else False):
                        data[col] = None
                    else:
                        try:
                            data[col] = int(val)
                        except Exception:
                            data[col] = None

                elif col in ("aviso_mantencion_h","llegada_mantencion_h","aviso_lider_h","llegada_lider_h"):
                    try:
                        data[col] = float(val) if val != "" and not (hasattr(pd,"isna") and pd.isna(val)) else None
                    except:
                        data[col] = None

                elif col == "semana":
                    try:
                        data[col] = int(val)
                    except:
                        data[col] = None

                else:
                    # cadenas genéricas
                    if val is None or (hasattr(pd,"isna") and pd.isna(val)):
                        data[col] = None
                    else:
                        data[col] = str(val)

            # Calcular total/promedio de encuestas si aplica
            if Model is EncuestaEntry:
                puntajes = []
                for i in range(1,6):
                    p = data.get(f"q{i}_puntaje")
                    if p is not None:
                        puntajes.append(p)
                data["total"] = sum(puntajes) if puntajes else None
                data["promedio"] = (data["total"]/len(puntajes)) if puntajes else None

            obj = Model(**data)
            db.session.add(obj)
            created += 1

        db.session.commit()
        flash(f"Importación completada. Registros creados: {created}")
    except Exception as e:
        db.session.rollback()
        flash(f"Error al importar: {e}")

    # Volvemos a la pestaña relacionada (coincide con nombres de panel)
    back_tab = entity
    if entity == "encuestas": back_tab = "encuesta"
    return redirect(url_for("panel", tab=back_tab))

# ---------------------------
# Plantillas Excel
# ---------------------------
@app.route("/template-xlsx/<entity>")
def template_xlsx(entity):
    if entity not in ENTITY_MODEL_MAP:
        flash("Entidad no reconocida.")
        return redirect(url_for("registros"))

    _, columns = ENTITY_MODEL_MAP[entity]

    # Ajustes de columnas "de entrada" vs columnas calculadas
    cols = columns.copy()
    # Las columnas que en import se llaman *_mmss son de entrada, mapean a *_sec
    # No hay que tocar aquí.

    try:
        import pandas as pd  # aseguramos
        df = pd.DataFrame(columns=cols)
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="plantilla")
        buf.seek(0)
        filename = f"plantilla_{entity}.xlsx"
        return send_file(buf, as_attachment=True, download_name=filename, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    except Exception as e:
        flash(f"No se pudo generar la plantilla: {e}")
        return redirect(url_for("panel", tab=entity))

# ---------------------------
# Eliminar registro
# ---------------------------
@app.route("/delete/<entity>/<int:rid>", methods=["POST"])
def delete_record(entity, rid):
    MODEL = {
        "censo": CensoEntry, "eventos": EventoSeguridadEntry, "duplicidades": DuplicidadEntry,
        "encuestas": EncuestaEntry, "atencion": AtencionPublicoEntry, "robos": RoboHurtoEntry,
        "miscelaneo": MiscelaneoEntry, "desviaciones": DesviacionEntry, "solicitud_ot": SolicitudOTEntry,
        "reclamos": ReclamoEntry,
        "activacion_alarma": ActivacionAlarmaEntry, "extension_excepcion": ExtensionExcepcionEntry,
        "onboarding": OnboardingEntry, "apertura_habitacion": AperturaHabitacionEntry, "eecc": EECCCumplimientoEntry
    }.get(entity)

    if not MODEL:
        flash("Entidad no reconocida.")
        return redirect(request.form.get("next") or url_for("registros"))

    obj = MODEL.query.get(rid)
    if not obj:
        flash("Registro no encontrado.")
        return redirect(request.form.get("next") or url_for("registros"))

    # Si es Onboarding, borrar archivo si existe
    if isinstance(obj, OnboardingEntry) and obj.archivo_pdf:
        try:
            fpath = os.path.join(app.config["UPLOAD_FOLDER_ONBOARD"], obj.archivo_pdf)
            if os.path.exists(fpath):
                os.remove(fpath)
        except Exception:
            pass

    try:
        db.session.delete(obj)
        db.session.commit()
        flash("Registro eliminado.")
    except Exception as e:
        db.session.rollback()
        flash(f"No se pudo eliminar: {e}")

    return redirect(request.form.get("next") or url_for("registros"))

# ---------------------------
# Descargar PDF Onboarding
# ---------------------------
@app.route("/download/onboarding/<int:oid>")
def download_onboarding_pdf(oid):
    obj = OnboardingEntry.query.get(oid)
    if not obj or not obj.archivo_pdf:
        flash("Archivo no disponible.")
        return redirect(url_for("registros", vista="onboarding"))
    return send_from_directory(
        app.config["UPLOAD_FOLDER_ONBOARD"],
        obj.archivo_pdf,
        as_attachment=True,
        download_name=obj.archivo_pdf
    )

# ---------------------------
# Ejecutar
# ---------------------------
if __name__ == "__main__":
    ensure_dirs()
    app.run(host="0.0.0.0", port=5000, debug=True)

