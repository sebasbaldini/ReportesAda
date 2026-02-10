import pandas as pd
import io
from datetime import datetime
from sqlalchemy import func, or_, distinct, cast, String, text, and_
from .extensions import db
from .models import EstacionSimparh, MedicionEMA, RhAforosDw, RhEscalasDw
from .alert_rules import ALERTA_CONFIG

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
    columnas_orden = []
    if 'partido' in df_clean.columns: columnas_orden.append('partido')
    if 'descripcion' in df_clean.columns: columnas_orden.append('descripcion')
    elif 'id_proyecto' in df_clean.columns: columnas_orden.append('id_proyecto')
    if 'fecha' in df_clean.columns: columnas_orden.append('fecha')
    if columnas_orden: df_clean = df_clean.sort_values(by=columnas_orden, ascending=True)
    cols = df_clean.columns.tolist()
    first_cols = ['id_proyecto', 'partido', 'descripcion', 'Sensor', 'fecha', 'dia', 'hora', 'valor']
    new_order = [c for c in first_cols if c in cols] + [c for c in cols if c not in first_cols]
    df_clean = df_clean[new_order]
    df_clean.to_excel(output, index=False, sheet_name='Datos', engine='openpyxl')
    return output

# ==========================================
# 2. CONSULTAS DE ESTACIONES
# ==========================================

def get_all_stations_repo():
    return EstacionSimparh.query\
        .filter(EstacionSimparh.estado_estacion.ilike('Finalizada'))\
        .order_by(EstacionSimparh.ubicacion)\
        .all()

def get_stations_with_telemetry_repo():
    q = db.session.query(EstacionSimparh)\
        .join(MedicionEMA, EstacionSimparh.id_proyecto == MedicionEMA.id_proyecto)\
        .distinct()
    return q.order_by(EstacionSimparh.ubicacion).all()

def get_stations_with_aforos_repo():
    q = db.session.query(EstacionSimparh)\
        .join(RhAforosDw, EstacionSimparh.id == RhAforosDw.codigo)\
        .distinct()
    return q.order_by(EstacionSimparh.ubicacion).all()

def get_stations_with_escalas_repo():
    """Devuelve estaciones que tienen datos en la tabla de escalas."""
    q = db.session.query(EstacionSimparh)\
        .join(RhEscalasDw, EstacionSimparh.id == RhEscalasDw.codigo)\
        .distinct()
    return q.order_by(EstacionSimparh.ubicacion).all()

# ==========================================
# 3. DATOS DE AFOROS Y ESCALAS
# ==========================================

def get_aforos_data_repo(station_id, f_inicio=None, f_fin=None):
    str_id = str(station_id).strip()
    query = db.session.query(
        RhAforosDw,
        EstacionSimparh.ubicacion.label('Nombre Estacion')
    ).join(EstacionSimparh, RhAforosDw.codigo == EstacionSimparh.id)\
     .filter(func.trim(cast(RhAforosDw.codigo, String)) == str_id)

    if f_inicio:
        query = query.filter(RhAforosDw.fecha_hora >= f_inicio)
    if f_fin:
        if isinstance(f_fin, str):
            f_fin = datetime.strptime(f_fin, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
        query = query.filter(RhAforosDw.fecha_hora <= f_fin)

    try:
        df = pd.read_sql(query.statement, db.session.connection())
        return df
    except Exception as e:
        print(f"Error descargando aforos: {e}")
        return pd.DataFrame()

def get_escalas_data_repo(station_id, f_inicio=None, f_fin=None):
    str_id = str(station_id).strip()
    query = db.session.query(
        EstacionSimparh.ubicacion.label('Nombre Estacion'),
        RhEscalasDw.fecha,
        RhEscalasDw.altura,
        RhEscalasDw.cota,
        EstacionSimparh.nomcuenca.label('Cuenca'),
        RhEscalasDw.obs        
    ).join(EstacionSimparh, RhEscalasDw.codigo == EstacionSimparh.id)\
     .filter(func.trim(cast(RhEscalasDw.codigo, String)) == str_id)

    if f_inicio:
        query = query.filter(RhEscalasDw.fecha >= f_inicio)
    if f_fin:
        if isinstance(f_fin, str):
            f_fin = datetime.strptime(f_fin, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
        query = query.filter(RhEscalasDw.fecha <= f_fin)

    query = query.order_by(RhEscalasDw.fecha.desc())
    try:
        df = pd.read_sql(query.statement, db.session.connection())
        return df
    except Exception as e:
        print(f"Error descargando escalas: {e}")
        return pd.DataFrame()

# ==========================================
# 4. FUNCIONES DE EMA (SENSORES Y DASHBOARD)
# ==========================================

def get_active_stations_ids_today():
    hoy_inicio = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    try:
        results = db.session.query(distinct(MedicionEMA.id_proyecto)).filter(MedicionEMA.fecha >= hoy_inicio).all()
        return {row[0] for row in results if row[0]}
    except: return set()

def get_latest_aforo_dates_repo():
    q = db.session.query(RhAforosDw.codigo, func.max(RhAforosDw.fecha_hora)).group_by(RhAforosDw.codigo).all()
    return {row[0]: row[1] for row in q if row[0]}

def get_latest_escala_dates_repo():
    q = db.session.query(RhEscalasDw.codigo, func.max(RhEscalasDw.fecha)).group_by(RhEscalasDw.codigo).all()
    return {row[0]: row[1] for row in q if row[0]}

def get_sensors_for_station_repo(id_proyecto_str):
    mapa_flags = [
        ('pluv', 1, 'Pluviómetro', 'Pluviometrica'),
        ('limn', 2, 'Limnímetro (Nivel)', 'Limnigrafica'),
        ('viento', 3, 'Anemómetro (Velocidad)', 'Anemometrica'),
        ('viento', 4, 'Veleta (Dirección)', 'Direccion Viento'),
        ('temp_hum', 5, 'Temperatura Aire', 'Temp Atmosferica'),
        ('temp_hum', 6, 'Humedad', 'Humedad'),
        ('freat', 7, 'Freatímetro', 'Freatimetrica'),
        ('rad_solar', 8, 'Radiación Solar', 'Piranometrica')
    ]
    
    extras = [
        (99, 'Batería', 'Bateria'), (98, 'Presión', 'Barometrica'),
        (90, 'Calidad - Conductividad', 'Conductividad'), 
        (91, 'Calidad - pH', 'PH'),
        (92, 'Calidad - Turbidimetrica', 'Turbidimetrica'),
        (93, 'Calidad - Temp. Agua', 'Temp Agua')
    ]

    sensors = []

    if str(id_proyecto_str) == 'todas':
        seen = set()
        for _, sid, nombre, metrica in mapa_flags:
            if metrica not in seen:
                sensors.append({'id': sid, 'nombre': nombre, 'table_name': metrica, 'search_text': nombre.lower(), 'fecha_inicio': 'N/A', 'ultimo_dato': 'N/A'})
                seen.add(metrica)
        for sid, nom, met in extras:
            sensors.append({'id': sid, 'nombre': nom, 'table_name': met, 'search_text': nom.lower(), 'fecha_inicio': 'N/A', 'ultimo_dato': 'N/A'})
        return sensors

    try:
        q_dates = db.session.query(
            MedicionEMA.metrica, 
            func.min(MedicionEMA.fecha),
            func.max(MedicionEMA.fecha)
        ).filter(MedicionEMA.id_proyecto == id_proyecto_str)\
         .group_by(MedicionEMA.metrica).all()

        fechas_map = {}
        ultimos_datos_map = {}

        for row in q_dates:
            if row[1]: fechas_map[row[0]] = row[1].strftime('%Y-%m-%d')
            if row[2]: ultimos_datos_map[row[0]] = row[2].strftime('%d/%m/%Y %H:%M')

        metricas_activas = list(fechas_map.keys())
        seen_metrics = set()
        
        for _, sid, nombre, metrica_db in mapa_flags:
            if any(metrica_db in m for m in metricas_activas):
                if metrica_db not in seen_metrics:
                    f_inicio = fechas_map.get(metrica_db, 'N/A')
                    f_fin = ultimos_datos_map.get(metrica_db, 'Sin datos')
                    sensors.append({
                        'id': sid, 'nombre': nombre, 'table_name': metrica_db, 
                        'search_text': nombre.lower(), 'fecha_inicio': f_inicio, 'ultimo_dato': f_fin
                    })
                    seen_metrics.add(metrica_db)

        for sid, nom, met in extras:
            if any(met in m for m in metricas_activas):
                f_inicio = fechas_map.get(met, 'N/A')
                f_fin = ultimos_datos_map.get(met, 'Sin datos')
                sensors.append({
                    'id': sid, 'nombre': nom, 'table_name': met, 
                    'search_text': nom.lower(), 'fecha_inicio': f_inicio, 'ultimo_dato': f_fin
                })

    except Exception as e:
        print(f"Error obteniendo sensores para {id_proyecto_str}: {e}")

    return sensors

def get_dashboard_data_repo(id_proyecto_str):
    data = {}
    metricas = { 'temperatura': ('Temp Atmosferica', 'last'), 'nivel_max_hoy': ('Limnigrafica', 'max_today'), 'pluvio_sum_hoy': ('Pluviometrica', 'sum_today'), 'viento_vel': ('Anemometrica', 'last'), 'viento_dir': ('Direccion Viento', 'last'), 'presion': ('Barometrica', 'last'), 'bateria': ('Bateria', 'last') }
    hoy = datetime.now().date()
    for key, (metrica, modo) in metricas.items():
        try:
            q = db.session.query(MedicionEMA).filter_by(id_proyecto=id_proyecto_str, metrica=metrica)
            res = None; ts = None
            if modo == 'last':
                rec = q.order_by(MedicionEMA.fecha.desc()).first()
                if rec: res = rec.valor; ts = rec.fecha
            elif modo == 'max_today':
                res = q.filter(func.date(MedicionEMA.fecha) == hoy).with_entities(func.max(MedicionEMA.valor)).scalar(); ts = "Hoy"
            elif modo == 'sum_today':
                res = q.filter(func.date(MedicionEMA.fecha) == hoy).with_entities(func.sum(MedicionEMA.valor)).scalar(); ts = "Hoy"
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

def get_map_popup_status_repo(id_proyecto_str):
    sensores_config = get_sensors_for_station_repo(id_proyecto_str)
    mapa_nombres = {s['table_name']: s['nombre'] for s in sensores_config}
    hoy_inicio = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    resultado = []

    try:
        stmt_max_fechas = db.session.query(
            MedicionEMA.metrica,
            func.max(MedicionEMA.fecha).label('max_fecha')
        ).filter(MedicionEMA.id_proyecto == id_proyecto_str)\
         .group_by(MedicionEMA.metrica).subquery()

        query = db.session.query(MedicionEMA).join(
            stmt_max_fechas,
            and_(
                MedicionEMA.metrica == stmt_max_fechas.c.metrica,
                MedicionEMA.fecha == stmt_max_fechas.c.max_fecha
            )
        ).filter(MedicionEMA.id_proyecto == id_proyecto_str)
        
        datos_recientes = query.all()

        for dato in datos_recientes:
            if dato.metrica in mapa_nombres:
                nombre_display = mapa_nombres[dato.metrica]
                estado = 'online' if dato.fecha >= hoy_inicio else 'offline'
                fecha_str = dato.fecha.strftime('%Y-%m-%d %H:%M')
                val = dato.valor
                
                valor_str = str(val)
                m_real = dato.metrica
                if 'Pluvio' in m_real: valor_str = f"{val} mm"
                elif 'Limni' in m_real: valor_str = f"{val} m"
                elif 'Conductividad' in m_real: valor_str = f"{val} µS/cm"
                elif 'Freatimetrica' in m_real: valor_str = f"{val} m"
                elif 'Temp' in m_real: valor_str = f"{val} °C"
                elif 'Viento' in m_real: valor_str = f"{val} °"
                elif 'Anemo' in m_real: valor_str = f"{val} °Km/h"
                elif 'Bateria' in m_real: valor_str = f"{val} V"
                elif 'Presion' in m_real or 'Baro' in m_real: valor_str = f"{int(val)} hPa"
                elif 'Humedad' in m_real: valor_str = f"{int(val)} %"
                elif 'Turbi' in m_real: valor_str = f"{val} NTU"

                resultado.append({
                    'nombre': nombre_display, 'valor': valor_str, 
                    'fecha': fecha_str, 'estado': estado
                })

    except Exception as e:
        print(f"Error en popup optimizado: {e}")

    resultado.sort(key=lambda x: x['nombre'])
    return resultado

def generate_chart_report_data(id_proyecto_str, fecha_inicio, fecha_fin, metrica, tipo_proceso='raw'):
    if isinstance(fecha_fin, str):
        fecha_fin = datetime.strptime(fecha_fin, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
    query = db.session.query(
        MedicionEMA.fecha, MedicionEMA.valor, MedicionEMA.id_proyecto,
        EstacionSimparh.pdo, EstacionSimparh.nomcuenca, EstacionSimparh.ubicacion.label('descripcion')
    ).join(EstacionSimparh, MedicionEMA.id_proyecto == EstacionSimparh.id_proyecto)
    query = query.filter(MedicionEMA.fecha >= fecha_inicio, MedicionEMA.fecha <= fecha_fin)
    
    if metrica == 'Calidad':
        query = query.filter(or_(MedicionEMA.metrica.ilike('%Calidad%'), MedicionEMA.metrica.ilike('%Conduct%'), MedicionEMA.metrica.ilike('%Turbidimetrica%'), MedicionEMA.metrica.ilike('%Ph%')))
    else:
        query = query.filter(MedicionEMA.metrica == metrica)
        
    if str(id_proyecto_str) != 'todas':
        query = query.filter(MedicionEMA.id_proyecto == id_proyecto_str)
        
    query = query.order_by(MedicionEMA.fecha.asc())
    try: df = pd.read_sql(query.statement, db.session.connection())
    except: return pd.DataFrame()
    
    if df.empty: return df
    meta_cols = ['id_proyecto', 'pdo', 'descripcion']
    if tipo_proceso == 'pluvio_sum' or tipo_proceso == 'daily_sum':
        df['dia'] = df['fecha'].dt.date
        df = df.groupby(meta_cols + ['dia'])['valor'].sum().reset_index()
    elif tipo_proceso == 'nivel_max' or tipo_proceso == 'daily_max':
        df['dia'] = df['fecha'].dt.date
        df = df.groupby(meta_cols + ['dia'])['valor'].max().reset_index()
    elif tipo_proceso == 'avg_hourly':
        df['hora'] = df['fecha'].dt.floor('H')
        df = df.groupby(meta_cols + ['hora'])['valor'].mean().reset_index()
    elif tipo_proceso == 'daily_min_max':
        df['dia'] = df['fecha'].dt.date
        grouped = df.groupby(meta_cols + ['dia'])['valor'].agg(['min', 'max']).reset_index()
        grouped.columns = [c[0] if isinstance(c, tuple) else c for c in grouped.columns] 
        grouped.rename(columns={'min': 'valor_min', 'max': 'valor_max'}, inplace=True)
        df = grouped
    elif tipo_proceso == 'daily_avg':
        df['dia'] = df['fecha'].dt.date
        df = df.groupby(meta_cols + ['dia'])['valor'].mean().reset_index()
    if 'dia' in df.columns: df = df.sort_values('dia')
    elif 'hora' in df.columns: df = df.sort_values('hora')
    return df

def get_chart_data_repo(ema_id, sensor_info_list, f_inicio, f_fin):
    if not sensor_info_list or not ema_id: return pd.DataFrame()
    metricas_reales = []
    for item in sensor_info_list:
        try:
            parts = item.split('|')
            if len(parts) >= 2: metricas_reales.append(parts[1])
        except: continue

    if not metricas_reales: return pd.DataFrame()

    query = db.session.query(MedicionEMA.fecha, MedicionEMA.valor, MedicionEMA.metrica)\
              .filter(MedicionEMA.id_proyecto == ema_id, MedicionEMA.metrica.in_(metricas_reales))

    if f_inicio: query = query.filter(MedicionEMA.fecha >= f_inicio)
    if f_fin:
        if isinstance(f_fin, str):
            try:
                f_fin_dt = datetime.strptime(f_fin, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
                query = query.filter(MedicionEMA.fecha <= f_fin_dt)
            except: pass 
        else: query = query.filter(MedicionEMA.fecha <= f_fin)

    query = query.order_by(MedicionEMA.fecha.asc())
    try:
        df = pd.read_sql(query.statement, db.session.connection())
        return df
    except Exception as e:
        print(f"Error en get_chart_data_repo: {e}")
        return pd.DataFrame()

def get_sensor_start_date(table_name, ema_id_str):
    fecha_str = "N/A"
    try:
        min_fecha = db.session.query(func.min(MedicionEMA.fecha))\
            .filter(MedicionEMA.id_proyecto == ema_id_str)\
            .filter(MedicionEMA.metrica == table_name)\
            .scalar()
        if min_fecha:
            fecha_str = min_fecha.strftime('%Y-%m-%d')
    except Exception as e:
        print(f"Error buscando fecha min: {e}")
    return fecha_str

# ==========================================
# 6. SISTEMA DE ALERTAS (SEMÁFORO)
# ==========================================


def check_alert_status_repo(id_proyecto_str, nombre_estacion=None):
    """
    Verifica alertas dinámicamente usando app/alert_rules.py
    """
    str_id = str(id_proyecto_str).strip()
    nombre_limpio = str(nombre_estacion).upper() if nombre_estacion else ""
    
    config = None
    
    # 1. BÚSQUEDA INTELIGENTE EN EL ARCHIVO DE REGLAS
    # Recorremos todas las claves definidas en alert_rules.py (ej: 'FALBO', 'ARROYO', etc.)
    for palabra_clave, reglas in ALERTA_CONFIG.items():
        if palabra_clave in nombre_limpio:
            config = reglas
            break # Encontramos la regla, dejamos de buscar
    
    # Si no encontramos ninguna regla para esta estación, retornamos None (Verde normal/Gris)
    if not config:
        return None 
        
    try:
        # 2. BUSCAR DATO
        # Usamos LIKE para ser tolerantes con los espacios en el ID de la base
        ultimo_dato = db.session.query(MedicionEMA.valor)\
            .filter(MedicionEMA.id_proyecto.ilike(f"%{str_id}%"))\
            .filter(MedicionEMA.metrica == 'Limnigrafica')\
            .order_by(MedicionEMA.fecha.desc())\
            .first()
            
        if not ultimo_dato:
            return None 
            
        valor = float(ultimo_dato.valor)
        
        # 3. COMPARAR CON LOS UMBRALES DE LA REGLA ENCONTRADA
        if valor >= config['rojo']:
            return 'ROJO'
        elif valor >= config['naranja']:
            return 'NARANJA'
        elif valor >= config['amarillo']:
            return 'AMARILLO'
        else:
            return 'VERDE'
            
    except Exception as e:
        print(f"Error calculando alerta: {e}")
        return None