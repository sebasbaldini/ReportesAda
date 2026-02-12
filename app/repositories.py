import pandas as pd
import io
import pytz
from datetime import datetime
from sqlalchemy import func, or_, distinct, cast, String, text, and_
from .extensions import db
from .models import EstacionSimparh, MedicionEMA, RhAforosDw, RhEscalasDw, MpLecturasDw

# ==========================================
# 1. GENERACIÓN DE EXCEL
# ==========================================

def create_excel_simple(df):
    output = io.BytesIO()
    cols_drop = ['geom', 'key_aforo', 'estacion_rel', 'id', 'codigo', 'aforador']
    for c in cols_drop:
        if c in df.columns: 
            try: df.drop(columns=[c], inplace=True)
            except: pass
    if 'fecha_hora' in df.columns:
        df = df.sort_values(by='fecha_hora', ascending=False)
    df.to_excel(output, index=False, sheet_name='Datos Aforo', engine='openpyxl')
    return output

def create_excel_from_dataframe(df):
    output = io.BytesIO()
    df_clean = df.loc[:, ~df.columns.duplicated()]
    cols_drop = ['key_unica_mediciones_ema', 'geom', 'id']
    for c in cols_drop:
        if c in df_clean.columns: df_clean.drop(columns=[c], inplace=True)
    if 'pdo' in df_clean.columns: df_clean.rename(columns={'pdo': 'partido'}, inplace=True)
    cols = df_clean.columns.tolist()
    first_cols = ['id_proyecto', 'partido', 'descripcion', 'Sensor', 'fecha', 'dia', 'hora', 'valor']
    new_order = [c for c in first_cols if c in cols] + [c for c in cols if c not in first_cols]
    df_clean = df_clean[new_order]
    df_clean.to_excel(output, index=False, sheet_name='Datos', engine='openpyxl')
    return output

def create_excel_mp(df):
    output = io.BytesIO()
    cols_drop = ['key_unica_mp', 'estacion_rel', 'id_proyecto']
    for c in cols_drop:
        if c in df.columns: 
            try: df.drop(columns=[c], inplace=True)
            except: pass
    if 'fecha_hora' in df.columns:
        df = df.sort_values(by='fecha_hora', ascending=False)
    
    rename_map = {
        'fecha_hora': 'Fecha y Hora',
        'valor': 'Lectura (m)',
        'cota': 'Cota (msnm)',
        'obs': 'Observaciones',
        'Nombre Estacion': 'Estación'
    }
    df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)
    df.to_excel(output, index=False, sheet_name='Lecturas MP', engine='openpyxl')
    return output

# ==========================================
# 2. CONSULTAS DE ESTACIONES
# ==========================================

def get_all_stations_repo():
    return EstacionSimparh.query\
        .filter(EstacionSimparh.estado_estacion.ilike('Finalizada'))\
        .order_by(EstacionSimparh.ubicacion)\
        .all()

def get_stations_for_map_repo():
    """Consulta HÍBRIDA para el MAPA"""
    ids_con_lecturas = db.session.query(MpLecturasDw.id_proyecto).distinct()
    return EstacionSimparh.query.filter(
        or_(
            EstacionSimparh.estado_estacion.ilike('Finalizada'),
            EstacionSimparh.id.in_(ids_con_lecturas)
        )
    ).order_by(EstacionSimparh.ubicacion).all()

def get_stations_with_telemetry_repo():
    return db.session.query(EstacionSimparh).join(MedicionEMA, EstacionSimparh.id_proyecto == MedicionEMA.id_proyecto).distinct().order_by(EstacionSimparh.ubicacion).all()

def get_stations_with_aforos_repo():
    return db.session.query(EstacionSimparh).join(RhAforosDw, EstacionSimparh.id == RhAforosDw.codigo).distinct().order_by(EstacionSimparh.ubicacion).all()

def get_stations_with_escalas_repo():
    return db.session.query(EstacionSimparh).join(RhEscalasDw, EstacionSimparh.id == RhEscalasDw.codigo).distinct().order_by(EstacionSimparh.ubicacion).all()

def get_stations_with_lecturas_repo():
    return db.session.query(EstacionSimparh).join(MpLecturasDw, EstacionSimparh.id == MpLecturasDw.id_proyecto).distinct().order_by(EstacionSimparh.ubicacion).all()

# ==========================================
# 3. DATOS ESPECÍFICOS
# ==========================================

def get_aforos_data_repo(station_id, f_inicio=None, f_fin=None):
    str_id = str(station_id).strip()
    query = db.session.query(RhAforosDw, EstacionSimparh.ubicacion.label('Nombre Estacion')).join(EstacionSimparh, RhAforosDw.codigo == EstacionSimparh.id).filter(func.trim(cast(RhAforosDw.codigo, String)) == str_id)
    if f_inicio: query = query.filter(RhAforosDw.fecha_hora >= f_inicio)
    if f_fin:
        if isinstance(f_fin, str): f_fin = datetime.strptime(f_fin, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
        query = query.filter(RhAforosDw.fecha_hora <= f_fin)
    try: return pd.read_sql(query.statement, db.session.connection())
    except: return pd.DataFrame()

def get_escalas_data_repo(station_id, f_inicio=None, f_fin=None):
    str_id = str(station_id).strip()
    query = db.session.query(EstacionSimparh.ubicacion.label('Nombre Estacion'), RhEscalasDw.fecha, RhEscalasDw.altura, RhEscalasDw.cota, EstacionSimparh.nomcuenca.label('Cuenca'), RhEscalasDw.obs).join(EstacionSimparh, RhEscalasDw.codigo == EstacionSimparh.id).filter(func.trim(cast(RhEscalasDw.codigo, String)) == str_id)
    if f_inicio: query = query.filter(RhEscalasDw.fecha >= f_inicio)
    if f_fin:
        if isinstance(f_fin, str): f_fin = datetime.strptime(f_fin, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
        query = query.filter(RhEscalasDw.fecha <= f_fin)
    query = query.order_by(RhEscalasDw.fecha.desc())
    try: return pd.read_sql(query.statement, db.session.connection())
    except: return pd.DataFrame()

def get_lecturas_data_repo(station_id, f_inicio=None, f_fin=None):
    str_id = str(station_id).strip()
    query = db.session.query(MpLecturasDw.fecha_hora, MpLecturasDw.valor, MpLecturasDw.cota, MpLecturasDw.obs, EstacionSimparh.ubicacion.label('Nombre Estacion')).join(EstacionSimparh, MpLecturasDw.id_proyecto == EstacionSimparh.id).filter(func.trim(cast(MpLecturasDw.id_proyecto, String)) == str_id)
    
    if f_inicio and isinstance(f_inicio, str):
        f_inicio = datetime.strptime(f_inicio, '%Y-%m-%d').replace(tzinfo=pytz.UTC)
        query = query.filter(MpLecturasDw.fecha_hora >= f_inicio)
    if f_fin and isinstance(f_fin, str):
        f_fin = datetime.strptime(f_fin, '%Y-%m-%d').replace(hour=23, minute=59, second=59, tzinfo=pytz.UTC)
        query = query.filter(MpLecturasDw.fecha_hora <= f_fin)
    
    query = query.order_by(MpLecturasDw.fecha_hora.desc())
    try:
        df = pd.read_sql(query.statement, db.session.connection())
        if not df.empty and 'fecha_hora' in df.columns:
            df['fecha_hora'] = pd.to_datetime(df['fecha_hora']).dt.tz_convert('America/Argentina/Buenos_Aires').dt.tz_localize(None)
        return df
    except: return pd.DataFrame()

# ==========================================
# 4. FUNCIONES DE EMA
# ==========================================

def get_active_stations_ids_today():
    hoy_inicio = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    try:
        results = db.session.query(distinct(MedicionEMA.id_proyecto)).filter(MedicionEMA.fecha >= hoy_inicio).all()
        return {row[0] for row in results if row[0]}
    except: return set()

def get_latest_aforo_dates_repo():
    q = db.session.query(RhAforosDw.codigo, func.max(RhAforosDw.fecha_hora)).group_by(RhAforosDw.codigo).all()
    return {str(row[0]).strip(): row[1] for row in q if row[0]}

def get_latest_escala_dates_repo():
    q = db.session.query(RhEscalasDw.codigo, func.max(RhEscalasDw.fecha)).group_by(RhEscalasDw.codigo).all()
    return {str(row[0]).strip(): row[1] for row in q if row[0]}

def get_latest_mp_data_repo():
    try:
        sql = text("SELECT DISTINCT ON (id_proyecto) id_proyecto, valor, fecha_hora FROM cuencas.mp_lecturas_dw ORDER BY id_proyecto, fecha_hora DESC")
        result = db.session.execute(sql)
        datos = {}
        for row in result:
            key = str(row[0]).strip()
            datos[key] = {'valor': row[1], 'fecha': row[2]}
        return datos
    except: return {}

def get_sensors_for_station_repo(id_p):
    mapa_flags = [('pluv', 1, 'Pluviómetro', 'Pluviometrica'), ('limn', 2, 'Limnímetro (Nivel)', 'Limnigrafica'), ('viento', 3, 'Anemómetro (Velocidad)', 'Anemometrica'), ('viento', 4, 'Veleta (Dirección)', 'Direccion Viento'), ('temp_hum', 5, 'Temperatura Aire', 'Temp Atmosferica'), ('temp_hum', 6, 'Humedad', 'Humedad'), ('freat', 7, 'Freatímetro', 'Freatimetrica'), ('rad_solar', 8, 'Radiación Solar', 'Piranometrica')]
    extras = [(99, 'Batería', 'Bateria'), (98, 'Presión', 'Barometrica'), (90, 'Calidad - Conductividad', 'Conductividad'), (91, 'Calidad - pH', 'PH'), (92, 'Calidad - Turbidimetrica', 'Turbidimetrica'), (93, 'Calidad - Temp. Agua', 'Temp Agua')]
    sensors = []
    if str(id_p) == 'todas':
        seen = set()
        for _, sid, nombre, metrica in mapa_flags:
            if metrica not in seen: sensors.append({'id': sid, 'nombre': nombre, 'table_name': metrica, 'search_text': nombre.lower(), 'fecha_inicio': 'N/A', 'ultimo_dato': 'N/A'}); seen.add(metrica)
        for sid, nom, met in extras: sensors.append({'id': sid, 'nombre': nom, 'table_name': met, 'search_text': nom.lower(), 'fecha_inicio': 'N/A', 'ultimo_dato': 'N/A'})
        return sensors
    try:
        q_dates = db.session.query(MedicionEMA.metrica, func.min(MedicionEMA.fecha), func.max(MedicionEMA.fecha)).filter(MedicionEMA.id_proyecto == id_p).group_by(MedicionEMA.metrica).all()
        fechas_map = {}; ultimos_datos_map = {}
        for row in q_dates:
            if row[1]: fechas_map[row[0]] = row[1].strftime('%Y-%m-%d')
            if row[2]: ultimos_datos_map[row[0]] = row[2].strftime('%d/%m/%Y %H:%M')
        metricas_activas = list(fechas_map.keys()); seen_metrics = set()
        for _, sid, nombre, metrica_db in mapa_flags:
            if any(metrica_db in m for m in metricas_activas):
                if metrica_db not in seen_metrics:
                    sensors.append({'id': sid, 'nombre': nombre, 'table_name': metrica_db, 'search_text': nombre.lower(), 'fecha_inicio': fechas_map.get(metrica_db, 'N/A'), 'ultimo_dato': ultimos_datos_map.get(metrica_db, 'Sin datos')}); seen_metrics.add(metrica_db)
        for sid, nom, met in extras:
            if any(met in m for m in metricas_activas):
                sensors.append({'id': sid, 'nombre': nom, 'table_name': met, 'search_text': nom.lower(), 'fecha_inicio': fechas_map.get(met, 'N/A'), 'ultimo_dato': ultimos_datos_map.get(met, 'Sin datos')})
    except: pass
    return sensors

def get_dashboard_data_repo(id_p):
    data = {}; metricas = { 'temperatura': ('Temp Atmosferica', 'last'), 'nivel_max_hoy': ('Limnigrafica', 'max_today'), 'pluvio_sum_hoy': ('Pluviometrica', 'sum_today'), 'viento_vel': ('Anemometrica', 'last'), 'viento_dir': ('Direccion Viento', 'last'), 'presion': ('Barometrica', 'last'), 'bateria': ('Bateria', 'last') }
    hoy = datetime.now().date()
    for key, (metrica, modo) in metricas.items():
        try:
            q = db.session.query(MedicionEMA).filter_by(id_proyecto=id_p, metrica=metrica)
            res = None; ts = None
            if modo == 'last':
                rec = q.order_by(MedicionEMA.fecha.desc()).first()
                if rec: res = rec.valor; ts = rec.fecha
            elif modo == 'max_today': res = q.filter(func.date(MedicionEMA.fecha) == hoy).with_entities(func.max(MedicionEMA.valor)).scalar(); ts = "Hoy"
            elif modo == 'sum_today': res = q.filter(func.date(MedicionEMA.fecha) == hoy).with_entities(func.sum(MedicionEMA.valor)).scalar(); ts = "Hoy"
            if res is not None:
                ts_str = ts.strftime('%H:%M %d/%m') if isinstance(ts, datetime) else ts
                if key == 'viento_vel': data['viento'] = {'vel_str': f"{round(res,1)} km/h", 'timestamp': ts_str}
                elif key == 'viento_dir': 
                    if 'viento' in data: data['viento']['dir_num'] = res; data['viento']['dir_str'] = f"{int(res)}°"
                else: 
                    unit = 'mm' if 'pluvio' in key else 'm' if 'nivel' in key else '°C' if 'temp' in key else 'hPa' if 'presion' in key else 'V'
                    data[key] = {'valor_num': res, 'valor_str': f"{round(res, 2)} {unit}", 'timestamp': ts_str}
        except: pass
    return data

def get_map_popup_status_repo(id_p):
    sensores_config = get_sensors_for_station_repo(id_p)
    mapa_nombres = {s['table_name']: s['nombre'] for s in sensores_config}
    hoy = datetime.now().replace(hour=0, minute=0, second=0)
    resultado = []
    try:
        stmt = db.session.query(MedicionEMA.metrica, func.max(MedicionEMA.fecha).label('max_fecha')).filter(MedicionEMA.id_proyecto == id_p).group_by(MedicionEMA.metrica).subquery()
        q = db.session.query(MedicionEMA).join(stmt, and_(MedicionEMA.metrica == stmt.c.metrica, MedicionEMA.fecha == stmt.c.max_fecha)).filter(MedicionEMA.id_proyecto == id_p)
        for dato in q.all():
            if dato.metrica in mapa_nombres:
                nom = mapa_nombres[dato.metrica]; est = 'online' if dato.fecha >= hoy else 'offline'; val = str(dato.valor)
                m = dato.metrica
                if 'Pluvio' in m: val += " mm"
                elif 'Limni' in m or 'Freat' in m: val += " m"
                elif 'Cond' in m: val += " µS/cm"
                elif 'Temp' in m: val += " °C"
                elif 'Viento' in m: val += " °"
                elif 'Anemo' in m: val += " km/h"
                elif 'Bat' in m: val += " V"
                elif 'Pres' in m or 'Baro' in m: val = f"{int(dato.valor)} hPa"
                elif 'Hum' in m: val = f"{int(dato.valor)} %"
                elif 'Turbi' in m: val += " NTU"
                resultado.append({'nombre': nom, 'valor': val, 'fecha': dato.fecha.strftime('%Y-%m-%d %H:%M'), 'estado': est})
    except: pass
    resultado.sort(key=lambda x: x['nombre'])
    return resultado

def generate_chart_report_data(id_p, fi, ff, met, proc='raw'):
    if isinstance(ff, str): ff = datetime.strptime(ff, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
    q = db.session.query(MedicionEMA.fecha, MedicionEMA.valor, MedicionEMA.id_proyecto, EstacionSimparh.pdo, EstacionSimparh.nomcuenca, EstacionSimparh.ubicacion.label('descripcion')).join(EstacionSimparh, MedicionEMA.id_proyecto == EstacionSimparh.id_proyecto)
    q = q.filter(MedicionEMA.fecha >= fi, MedicionEMA.fecha <= ff)
    if met == 'Calidad': q = q.filter(or_(MedicionEMA.metrica.ilike('%Calidad%'), MedicionEMA.metrica.ilike('%Conduct%'), MedicionEMA.metrica.ilike('%Turbidimetrica%'), MedicionEMA.metrica.ilike('%Ph%')))
    else: q = q.filter(MedicionEMA.metrica == met)
    if str(id_p) != 'todas': q = q.filter(MedicionEMA.id_proyecto == id_p)
    q = q.order_by(MedicionEMA.fecha.asc())
    try: df = pd.read_sql(q.statement, db.session.connection())
    except: return pd.DataFrame()
    if df.empty: return df
    meta = ['id_proyecto', 'pdo', 'descripcion']
    if proc in ['pluvio_sum', 'daily_sum']: df['dia'] = df['fecha'].dt.date; df = df.groupby(meta + ['dia'])['valor'].sum().reset_index()
    elif proc in ['nivel_max', 'daily_max']: df['dia'] = df['fecha'].dt.date; df = df.groupby(meta + ['dia'])['valor'].max().reset_index()
    elif proc == 'avg_hourly': df['hora'] = df['fecha'].dt.floor('H'); df = df.groupby(meta + ['hora'])['valor'].mean().reset_index()
    elif proc == 'daily_min_max':
        df['dia'] = df['fecha'].dt.date; g = df.groupby(meta + ['dia'])['valor'].agg(['min', 'max']).reset_index()
        g.columns = [c[0] if isinstance(c, tuple) else c for c in g.columns]
        g.rename(columns={'min': 'valor_min', 'max': 'valor_max'}, inplace=True); df = g
    elif proc == 'daily_avg': df['dia'] = df['fecha'].dt.date; df = df.groupby(meta + ['dia'])['valor'].mean().reset_index()
    if 'dia' in df.columns: df = df.sort_values('dia')
    elif 'hora' in df.columns: df = df.sort_values('hora')
    return df

def get_chart_data_repo(ema_id, sensors, fi, ff):
    if not sensors or not ema_id: return pd.DataFrame()
    mets = []
    for s in sensors:
        try:
            p = s.split('|'); 
            if len(p) >= 2: mets.append(p[1])
        except: continue
    if not mets: return pd.DataFrame()
    q = db.session.query(MedicionEMA.fecha, MedicionEMA.valor, MedicionEMA.metrica).filter(MedicionEMA.id_proyecto == ema_id, MedicionEMA.metrica.in_(mets))
    if fi: q = q.filter(MedicionEMA.fecha >= fi)
    if ff:
        if isinstance(ff, str):
            try: dt = datetime.strptime(ff, '%Y-%m-%d').replace(hour=23, minute=59, second=59); q = q.filter(MedicionEMA.fecha <= dt)
            except: pass 
        else: q = q.filter(MedicionEMA.fecha <= ff)
    q = q.order_by(MedicionEMA.fecha.asc())
    try: return pd.read_sql(q.statement, db.session.connection())
    except: return pd.DataFrame()