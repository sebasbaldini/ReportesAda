import pandas as pd
import io
from datetime import datetime, timedelta
from sqlalchemy import func, or_, distinct
from .extensions import db
from .models import EstacionSimparh, MedicionEMA

#
def create_excel_from_dataframe(df):
    output = io.BytesIO()
    # Evitamos columnas duplicadas por si acaso
    df_clean = df.loc[:, ~df.columns.duplicated()]
    
    # Eliminamos columnas técnicas
    cols_drop = ['key_unica_mediciones_ema', 'geom', 'id']
    for c in cols_drop:
        if c in df_clean.columns: df_clean.drop(columns=[c], inplace=True)
    
    # Renombramos pdo a partido (esto ya lo tenías)
    if 'pdo' in df_clean.columns:
        df_clean.rename(columns={'pdo': 'partido'}, inplace=True)

    # --- NUEVO ORDENAMIENTO (AGREGAR ESTO) ---
    # Esto soluciona que la info salga mezclada.
    # Ordenamos por: 1. Partido, 2. EMA (descripción), 3. Fecha
    columnas_orden = []
    
    # Prioridad 1: Agrupar por Municipio
    if 'partido' in df_clean.columns:
        columnas_orden.append('partido')
        
    # Prioridad 2: Agrupar por Estación (para que los días salgan pegados)
    # Usamos 'descripcion' (ubicación) o 'id_proyecto'
    if 'descripcion' in df_clean.columns:
        columnas_orden.append('descripcion')
    elif 'id_proyecto' in df_clean.columns:
        columnas_orden.append('id_proyecto')
        
    # Prioridad 3: Orden cronológico dentro de cada estación
    if 'fecha' in df_clean.columns:
        columnas_orden.append('fecha')
    elif 'dia' in df_clean.columns:   # Para reportes de sumas diarias
        columnas_orden.append('dia')
    elif 'hora' in df_clean.columns:  # Para promedios horarios
        columnas_orden.append('hora')

    # Aplicamos el ordenamiento a la tabla
    if columnas_orden:
        df_clean = df_clean.sort_values(by=columnas_orden, ascending=True)
    # -----------------------------------------

    cols = df_clean.columns.tolist()
    
    # Mantenemos tu lógica de orden visual de columnas
    first_cols = ['id_proyecto', 'partido', 'descripcion', 'Sensor', 'fecha', 'dia', 'hora', 'valor']
    new_order = [c for c in first_cols if c in cols] + [c for c in cols if c not in first_cols]
    df_clean = df_clean[new_order]

    df_clean.to_excel(output, index=False, sheet_name='Datos', engine='openpyxl')
    return output

#
def get_all_stations_repo():
    # Usamos 'ilike' para que busque "Finalizada", "finalizada", "FINALIZADA", etc.
    # El % % no es necesario si la palabra es exacta, pero ilike ayuda con mayúsculas.
    return EstacionSimparh.query\
        .filter(EstacionSimparh.estado_estacion.ilike('Finalizada'))\
        .order_by(EstacionSimparh.ubicacion)\
        .all()

def get_active_stations_ids_today():
    hoy_inicio = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    try:
        results = db.session.query(distinct(MedicionEMA.id_proyecto))\
                    .filter(MedicionEMA.fecha >= hoy_inicio).all()
        return {row[0] for row in results if row[0]}
    except Exception as e:
        print(f"Error consultando estado: {e}")
        return set()

# ... (imports anteriores siguen igual)

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
    
    sensors = []

    # Caso especial para "TODAS" (Sin cambios)
    if str(id_proyecto_str) == 'todas':
        seen = set()
        for _, sid, nombre, metrica in mapa_flags:
            if metrica not in seen:
                sensors.append({'id': sid, 'nombre': nombre, 'table_name': metrica, 'search_text': nombre.lower(), 'fecha_inicio': 'N/A'})
                seen.add(metrica)
        
        extras = [
            (99, 'Batería', 'Bateria'), (98, 'Presión', 'Barometrica'),
            (90, 'Calidad - Conductividad', 'Conductividad'), 
            (91, 'Calidad - pH', 'PH'),
            (92, 'Calidad - Turbidez', 'Turbidez'),
            (93, 'Calidad - Temp. Agua', 'Temp Agua')
        ]
        for sid, nom, met in extras:
            sensors.append({'id': sid, 'nombre': nom, 'table_name': met, 'search_text': nom.lower(), 'fecha_inicio': 'N/A'})
        return sensors

    # --- LÓGICA PARA UNA ESTACIÓN ESPECÍFICA ---
    estacion = EstacionSimparh.query.filter_by(id_proyecto=id_proyecto_str).first()
    if not estacion: return []

    for attr, sid, nombre, metrica in mapa_flags:
        # 1. Verificar si está habilitado por configuración (Base de datos de estaciones)
        val = getattr(estacion, attr)
        is_config_active = False
        if val is True: is_config_active = True
        elif isinstance(val, str) and val.lower() in ['true', 't', '1', 's', 'si', 'yes']: is_config_active = True
        
        if is_config_active:
            # 2. VALIDACIÓN EXTRA (ANTI-FANTASMAS):
            # Verificar si REALMENTE existe al menos un dato en la tabla de mediciones.
            # Si está configurado como True, pero no tiene datos históricos, lo ocultamos.
            try:
                # Usamos .first() que es muy rápido, solo queremos saber si existe 1 registro.
                existe_dato = db.session.query(MedicionEMA.key_unica_mediciones_ema)\
                    .filter_by(id_proyecto=id_proyecto_str, metrica=metrica)\
                    .first()

                if existe_dato:
                    # Si existe dato, buscamos la fecha de inicio
                    fecha_inicio = "N/A"
                    min_date = db.session.query(func.min(MedicionEMA.fecha))\
                        .filter_by(id_proyecto=id_proyecto_str, metrica=metrica).scalar()
                    if min_date: fecha_inicio = min_date.strftime('%Y-%m-%d')
                    
                    sensors.append({'id': sid, 'nombre': nombre, 'table_name': metrica, 'search_text': nombre.lower(), 'fecha_inicio': fecha_inicio})
            except Exception as e:
                print(f"Error verificando sensor fantasma {metrica}: {e}")
                # En caso de error de DB, podrías optar por mostrarlo u ocultarlo. 
                # Aquí lo ocultamos por seguridad si falla la consulta.
                pass

    # --- SENSORES NO CONFIGURADOS (Detectados automáticamente) ---
    # Esto sigue igual: busca cualquier otra cosa que exista en la tabla mediciones
    tiene_calidad = False
    val_cal = getattr(estacion, 'calidad', None)
    if val_cal is True: tiene_calidad = True
    elif isinstance(val_cal, str) and val_cal.lower() in ['true', 't', '1', 's', 'si', 'yes']: tiene_calidad = True

    try:
        res = db.session.query(distinct(MedicionEMA.metrica)).filter_by(id_proyecto=id_proyecto_str).all()
        metricas_reales_en_db = [r[0] for r in res if r[0]]
        
        terminos_calidad = {
            'Conduct': 'Calidad - Conductividad',
            'PH': 'Calidad - pH',
            'Turbid': 'Calidad - Turbidez',
            'Temp Agua': 'Calidad - Temp. Agua',
            'Bateria': 'Batería',
            'Barometrica': 'Presión Barométrica',
            'OD': 'Calidad - Oxígeno'
        }

        for m_real in metricas_reales_en_db:
            # Si ya lo agregamos arriba (en el bucle de flags), lo saltamos
            if any(s['table_name'] == m_real for s in sensors): continue

            nombre_display = None
            for key, display in terminos_calidad.items():
                if key.lower() in m_real.lower():
                    nombre_display = display if 'Calidad' in display else f"{display} ({m_real})"
                    if 'Temp Agua' in m_real: nombre_display = 'Calidad - Temp. Agua'
                    break
            
            if nombre_display:
                fecha_inicio = "N/A"
                try:
                    md = db.session.query(func.min(MedicionEMA.fecha)).filter_by(id_proyecto=id_proyecto_str, metrica=m_real).scalar()
                    if md: fecha_inicio = md.strftime('%Y-%m-%d')
                except: pass
                
                sensors.append({
                    'id': 90 + len(sensors), 
                    'nombre': nombre_display, 
                    'table_name': m_real, 
                    'search_text': nombre_display.lower(), 
                    'fecha_inicio': fecha_inicio
                })

    except Exception as e:
        print(f"Error query metricas: {e}")

    # (El bloque final 'if tiene_calidad' que agrega sensores vacíos lo dejamos o lo quitamos según prefieras. 
    # Por ahora lo dejo tal cual estaba en tu original, aunque podrías borrarlo si quieres ocultar todo lo vacío).
    if tiene_calidad:
        defaults = [
            ('PH', 'Calidad - pH'),
            ('Conductividad', 'Calidad - Conductividad'),
            ('Turbidez', 'Calidad - Turbidez'),
            ('Temp Agua', 'Calidad - Temp. Agua')
        ]
        for m_db, nom_disp in defaults:
            ya_esta = False
            for s in sensors:
                if s['table_name'] == m_db or s['nombre'] == nom_disp:
                    ya_esta = True
                    break
            if not ya_esta:
                sensors.append({'id': 100 + len(sensors), 'nombre': nom_disp + " (Sin Datos)", 'table_name': m_db, 'search_text': nom_disp.lower(), 'fecha_inicio': 'N/A'})

    return sensors



def get_dashboard_data_repo(id_proyecto_str):
    data = {}
    metricas = {
        'temperatura': ('Temp Atmosferica', 'last'), 'nivel_max_hoy': ('Limnigrafica', 'max_today'),
        'pluvio_sum_hoy': ('Pluviometrica', 'sum_today'), 'viento_vel': ('Anemometrica', 'last'),
        'viento_dir': ('Direccion Viento', 'last'), 'presion': ('Barometrica', 'last'), 'bateria': ('Bateria', 'last')
    }
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
    resultado = []
    hoy_inicio = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    for sensor in sensores_config:
        metrica_real = sensor['table_name']
        nombre_display = sensor['nombre']
        
        estado = 'offline'
        valor_str = "Sin datos"
        fecha_str = "-"
        
        try:
            ultimo_dato = db.session.query(MedicionEMA)\
                .filter_by(id_proyecto=id_proyecto_str, metrica=metrica_real)\
                .order_by(MedicionEMA.fecha.desc())\
                .first()
            
            if ultimo_dato:
                if ultimo_dato.fecha >= hoy_inicio:
                    estado = 'online'
                
                fecha_str = ultimo_dato.fecha.strftime('%d/%m %H:%M')
                val = ultimo_dato.valor
                
                if 'Pluvio' in metrica_real: valor_str = f"{val} mm"
                elif 'Limni' in metrica_real: valor_str = f"{val} m"
                elif 'Temp' in metrica_real: valor_str = f"{val} °C"
                elif 'Viento' in metrica_real or 'Anemo' in metrica_real: valor_str = f"{val} km/h"
                elif 'Bateria' in metrica_real: valor_str = f"{val} V"
                elif 'Presion' in metrica_real or 'Baro' in metrica_real: valor_str = f"{int(val)} hPa"
                elif 'Humedad' in metrica_real: valor_str = f"{int(val)} %"
                else: valor_str = str(val)

                resultado.append({
                    'nombre': nombre_display, 
                    'valor': valor_str, 
                    'fecha': fecha_str, 
                    'estado': estado
                })
            
        except Exception as e:
            print(f"Error popup sensor {metrica_real}: {e}")

    return resultado

def generate_chart_report_data(id_proyecto_str, fecha_inicio, fecha_fin, metrica, tipo_proceso='raw'):
    if isinstance(fecha_fin, str):
        fecha_fin = datetime.strptime(fecha_fin, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
    
    # CAMBIO 2: EstacionSimparh.partido -> EstacionSimparh.pdo
    query = db.session.query(
        MedicionEMA.fecha, 
        MedicionEMA.valor, 
        MedicionEMA.id_proyecto,
        EstacionSimparh.pdo,               
        EstacionSimparh.ubicacion.label('descripcion')
    ).join(EstacionSimparh, MedicionEMA.id_proyecto == EstacionSimparh.id_proyecto)
    
    query = query.filter(MedicionEMA.fecha >= fecha_inicio, MedicionEMA.fecha <= fecha_fin)
    
    if metrica == 'Calidad':
        query = query.filter(or_(MedicionEMA.metrica.ilike('%Calidad%'), MedicionEMA.metrica.ilike('%Conduct%'), MedicionEMA.metrica.ilike('%Turbiedad%'), MedicionEMA.metrica.ilike('%Ph%')))
    else:
        query = query.filter(MedicionEMA.metrica == metrica)

    if str(id_proyecto_str) != 'todas':
        query = query.filter(MedicionEMA.id_proyecto == id_proyecto_str)
        
    query = query.order_by(MedicionEMA.fecha.asc())
    
    try:
        df = pd.read_sql(query.statement, db.session.connection())
    except Exception as e:
        print(f"Error SQL: {e}")
        return pd.DataFrame()
    
    if df.empty: return df
    
    # CAMBIO 3: 'partido' -> 'pdo' en la lista de agrupación
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
        
    if 'dia' in df.columns:
        df = df.sort_values('dia')
    elif 'hora' in df.columns:
        df = df.sort_values('hora')
        
    return df