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
    __table_args__ = {'schema': 'cuencas'}
    
    # Llave primaria real de la base de datos (según tu foto es 'id')
    id = db.Column(db.String, primary_key=True)
    
    # Esta es la columna que usamos para relacionar (y para el listado)
    # Le ponemos unique=True para poder buscar rápido por ella
    id_proyecto = db.Column(db.String, unique=True)
    
    # --- ¡ESTA ERA LA COLUMNA QUE FALTABA! ---
    proyecto = db.Column(db.String) 
    
    # Columnas informativas
    ubicacion = db.Column(db.String)
    partido = db.Column(db.String)
    cuenca = db.Column(db.String)
    
    # Geometría
    geom = db.Column(Geometry('POINT', srid=4326))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    
    # Flags de sensores
    pluv = db.Column(db.String)
    limn = db.Column(db.String)
    viento = db.Column(db.String)
    temp_hum = db.Column(db.String)
    rad_solar = db.Column(db.String)
    freat = db.Column(db.String)
    calidad = db.Column(db.String)
    
    estado_estacion = db.Column(db.String)

class MedicionEMA(db.Model):
    __tablename__ = 'mediciones_ema'
    __table_args__ = {'schema': 'cuencas'}
    
    # Usamos la clave única que vi en tu foto como Primary Key
    key_unica_mediciones_ema = db.Column(db.String, primary_key=True)
    
    # Relación por id_proyecto (String)
    id_proyecto = db.Column(db.String, db.ForeignKey('cuencas.estaciones_simparh.id_proyecto'))
    
    metrica = db.Column(db.String)
    valor = db.Column(db.Float)
    fecha = db.Column(db.DateTime, index=True) # Tu foto muestra 'fecha'
    
    # Relación opcional
    estacion = db.relationship('EstacionSimparh', backref='mediciones', foreign_keys=[id_proyecto])