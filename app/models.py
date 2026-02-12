from flask_login import UserMixin
from werkzeug.security import check_password_hash
from geoalchemy2 import Geometry
from .extensions import login_manager, db
import config

class User(UserMixin):
    def __init__(self, user_id, username, email, password_hash, role='admin'):
        self.id = user_id
        self.username = username
        self.email = email
        self.password_hash = password_hash
        self.role = role 

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

@login_manager.user_loader
def load_user(user_id):
    user_data = config.APP_USERS.get(str(user_id))
    if not user_data: return None
    return User(str(user_id), user_data['username'], user_data['email'], user_data['password_hash'], user_data.get('role', 'admin'))

# --- MODELOS DE BASE DE DATOS (ESQUEMA CUENCAS) ---

class EstacionSimparh(db.Model):
    __tablename__ = 'estaciones_simparh'
    __table_args__ = {"schema": "cuencas"}

    id = db.Column(db.String, primary_key=True)
    id_proyecto = db.Column(db.String(50))
    proyecto = db.Column(db.String(100))
    ubicacion = db.Column(db.String(150))
    pdo = db.Column(db.String(100)) 
    partido = db.Column(db.String(100))
    nomcuenca = db.Column(db.String(100))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    geom = db.Column(Geometry('POINT', srid=4326))
    pluv = db.Column(db.Boolean)
    limn = db.Column(db.Boolean)
    viento = db.Column(db.Boolean)
    temp_hum = db.Column(db.Boolean)
    rad_solar = db.Column(db.Boolean)
    freat = db.Column(db.Boolean)
    calidad = db.Column(db.Boolean)
    estado_estacion = db.Column(db.String(50))
    tipo = db.Column(db.String(50)) 

class MedicionEMA(db.Model):
    __tablename__ = 'mediciones_ema'
    __table_args__ = {'schema': 'cuencas'}
    key_unica_mediciones_ema = db.Column(db.String, primary_key=True)
    id_proyecto = db.Column(db.String, db.ForeignKey('cuencas.estaciones_simparh.id_proyecto'))
    metrica = db.Column(db.String)
    valor = db.Column(db.Float)
    fecha = db.Column(db.DateTime, index=True)
    estacion = db.relationship('EstacionSimparh', backref='mediciones', foreign_keys=[id_proyecto])

class RhAforosDw(db.Model):
    __tablename__ = 'rh_aforos_dw'
    __table_args__ = {'schema': 'cuencas'}
    key_aforo = db.Column(db.Integer, primary_key=True)
    id = db.Column(db.Integer) 
    codigo = db.Column(db.String, db.ForeignKey('cuencas.estaciones_simparh.id'))
    fecha_hora = db.Column(db.DateTime)
    aforador = db.Column(db.String)
    instrumento = db.Column(db.String)
    molinete = db.Column(db.String)
    helice = db.Column(db.String)
    he = db.Column(db.Float)
    hg = db.Column(db.Float)
    k_hp = db.Column(db.Float)
    k_hg_h = db.Column(db.Float)
    tirante_max = db.Column(db.Float)
    area_total = db.Column(db.Float)
    area_con_q = db.Column(db.Float)
    vel_total = db.Column(db.Float)
    vel_con_q = db.Column(db.Float)
    q_ppal = db.Column(db.Float)
    q_total = db.Column(db.Float)
    revisado = db.Column(db.Boolean)
    obs = db.Column(db.String)
    estacion_rel = db.relationship('EstacionSimparh', backref='aforos', foreign_keys=[codigo])

class RhEscalasDw(db.Model):
    __tablename__ = 'rh_escalas_dw'
    __table_args__ = {'schema': 'cuencas'}
    key_escala = db.Column(db.String, primary_key=True) 
    codigo = db.Column(db.String, db.ForeignKey('cuencas.estaciones_simparh.id'))
    fecha = db.Column(db.Date)
    altura = db.Column(db.Float)
    cota = db.Column(db.Float)
    obs = db.Column(db.String)
    estacion_rel = db.relationship('EstacionSimparh', backref='escalas', foreign_keys=[codigo])

class MpLecturasDw(db.Model):
    __tablename__ = 'mp_lecturas_dw'
    __table_args__ = {'schema': 'cuencas'}
    key_unica_mp = db.Column(db.String, primary_key=True) 
    id_proyecto = db.Column(db.String, db.ForeignKey('cuencas.estaciones_simparh.id'))
    fecha_hora = db.Column(db.DateTime(timezone=True)) 
    valor = db.Column(db.Float)
    cota = db.Column(db.Float)
    obs = db.Column(db.String)
    estacion_rel = db.relationship('EstacionSimparh', backref='lecturas', foreign_keys=[id_proyecto])