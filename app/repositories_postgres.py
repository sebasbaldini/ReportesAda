# app/repositories_postgres.py
# (Corregido para evitar error de esquema 'master' y blindar la lista de sensores)

import psycopg2
import pandas as pd
import io
import config 
from datetime import datetime, timedelta

# --- Mapeo de traducciones ---
PROCESS_TYPE_TRANSLATION = {
    'raw': 'Dato Crudo',
    'pluvio_sum': 'Acumulado Diario',
    'nivel_max': 'Maximo Diario',
    'avg_hourly': 'Promedio por Hora',
    'sum_hourly': 'Acumulado por Hora',
    'max_hourly': 'Maximo por Hora'
}

# ==============================================================================
# === CONEXIÓN A LA BD =========================================================
# ==============================================================================
def get_db_connection(db_key):
    db_config = config.DATABASE_CONNECTIONS.get(db_key)
    if not db_config or db_config['driver'] != 'psycopg2':
        raise ValueError(f"Configuración no válida para PG: {db_key}")
    return psycopg2.connect(
        host=db_config['host'], port=db_config['port'], dbname=db_config['name'],
        user=db_config['user'], password=db_config['pass']
    )

# ==============================================================================
# === CACHE DE SENSORES ========================================================
# ==============================================================================
def build_active_sensor_cache(db_key):
    print(f"--- Construyendo cache para: {db_key} (PostgreSQL) ---")
    QUERY = "SELECT DISTINCT id_ema, id_sensor FROM master.medicion_anemometrica UNION SELECT DISTINCT id_ema, id_sensor FROM master.medicion_barometrica UNION SELECT DISTINCT id_ema, id_sensor FROM master.medicion_bateria UNION SELECT DISTINCT id_ema, id_sensor FROM master.medicion_conductiva UNION SELECT DISTINCT id_ema, id_sensor FROM master.medicion_direccion_viento UNION SELECT DISTINCT id_ema, id_sensor FROM master.medicion_freatimetrica UNION SELECT DISTINCT id_ema, id_sensor FROM master.medicion_humedad UNION SELECT DISTINCT id_ema, id_sensor FROM master.medicion_limnigrafica UNION SELECT DISTINCT id_ema, id_sensor FROM master.medicion_ph UNION SELECT DISTINCT id_ema, id_sensor FROM master.medicion_piranometrica UNION SELECT DISTINCT id_ema, id_sensor FROM master.medicion_pluviometrica UNION SELECT DISTINCT id_ema, id_sensor FROM master.medicion_punto_rocio UNION SELECT DISTINCT id_ema, id_sensor FROM master.medicion_temperatura_atmosferica UNION SELECT DISTINCT id_ema, id_sensor FROM master.medicion_temperatura_del_curso UNION SELECT DISTINCT id_ema, id_sensor FROM master.medicion_turbidimetrica;"
    
    temp_cache_db = {}
    try:
        conn = get_db_connection(db_key)
        cursor = conn.cursor()
        cursor.execute(QUERY)
        results = cursor.fetchall()
        cursor.close()
        for row in results:
            ema_id = int(row[0]); sensor_id = int(row[1])
            if ema_id not in temp_cache_db: temp_cache_db[ema_id] = []
            temp_cache_db[ema_id].append(sensor_id)
        conn.close()
        print(f"¡Cache para {db_key} construido con éxito!")
        return temp_cache_db
    except Exception as e:
        print(f"!!! ERROR CRÍTICO al construir el cache para {db_key}: {e}")
        return {}

# ==============================================================================
# === REPOSITORIO DE SENSORES (CORREGIDO) ======================================
# ==============================================================================
def get_sensors_for_ema_repo(db_key, G_SENSOR_CACHE, ema_id):
    db_cache = G_SENSOR_CACHE.get(db_key)
    if not db_cache: return []

    sensores_encontrados = []
    active_sensor_ids = set() 
    
    if ema_id == 'todas':
        for ema_key in db_cache:
            active_sensor_ids.update(db_cache[ema_key])
    else:
        try:
            ema_id_int = int(ema_id)
            if ema_id_int in db_cache:
                active_sensor_ids.update(db_cache[ema_id_int])
        except ValueError:
            return []
            
    if not active_sensor_ids: return []

    try:
        conn = get_db_connection(db_key)
        cursor = conn.cursor()
        
        # Obtenemos los nombres de los sensores
        SQL_QUERY = "SELECT id, nombre, descripcion FROM master.sensor WHERE id = ANY(%s) ORDER BY LOWER(nombre) ASC, nombre ASC;"
        cursor.execute(SQL_QUERY, (list(active_sensor_ids),)) 
        
        sensores_unicos_por_nombre = {} 
        rows = cursor.fetchall()
        
        for row in rows:
            sensor_id, sensor_nombre, sensor_desc = row
            table_name = None
            search_text = (str(sensor_nombre) + " " + str(sensor_desc)).lower()
            
            # Mapear tabla
            for table, keywords in config.TABLE_KEYWORD_MAP.items():
                if any(keyword in search_text for keyword in keywords):
                    table_name = table
                    break
            
            if table_name:
                fecha_inicio_str = "N/A"
                
                # --- NUEVO: Intentar buscar fecha, pero si falla NO romper todo ---
                if ema_id != 'todas': 
                    try:
                        # Forzamos el esquema "master." si no viene en el nombre
                        full_table_name = table_name
                        if "." not in table_name:
                            full_table_name = f"master.{table_name}"

                        sql_min = f"SELECT MIN(tiempo_de_medicion) FROM {full_table_name} WHERE id_ema = %s AND id_sensor = %s"
                        cursor.execute(sql_min, (int(ema_id), int(sensor_id)))
                        min_date = cursor.fetchone()[0]
                        if min_date:
                            fecha_inicio_str = min_date.strftime('%Y-%m-%d')
                    except Exception as e_date:
                        # Solo imprimimos el error, pero permitimos que el sensor se agregue
                        print(f"Advertencia: No se pudo obtener fecha para sensor {sensor_id} en {table_name}: {e_date}")
                        conn.rollback()

                sensor_data = {
                    'id': sensor_id, 
                    'nombre': sensor_nombre.title(), 
                    'table_name': table_name, 
                    'search_text': search_text,
                    'fecha_inicio': fecha_inicio_str 
                }
                
                if ema_id == 'todas':
                    search_key = sensor_nombre.lower() 
                    if search_key not in sensores_unicos_por_nombre:
                        sensor_data['id'] = 0 
                        sensores_unicos_por_nombre[search_key] = sensor_data
                else:
                    sensores_encontrados.append(sensor_data)

        cursor.close()
        conn.close()
        
        if ema_id == 'todas':
            return list(sensores_unicos_por_nombre.values())
        else:
            return sensores_encontrados
            
    except Exception as e:
        print(f"Error CRITICO en get_sensors_for_ema_repo ({db_key}): {e}")
        return []

# ==============================================================================
# === REPOSITORIO DE REPORTES (PostgreSQL) =====================================
# ==============================================================================
def generate_report_repo(db_key, ema_id_form, fecha_inicio_str, fecha_fin_str, sensor_info_list, process_type_list):
    
    fecha_fin_obj = datetime.strptime(fecha_fin_str, '%Y-%m-%d')
    fecha_fin_para_sql_obj = fecha_fin_obj + timedelta(days=1)
    FECHA_INICIO_SQL = fecha_inicio_str
    FECHA_FIN_SQL = fecha_fin_para_sql_obj.strftime('%Y-%m-%d')
    
    all_queries = []
    all_params = []
    
    for sensor_info, process_type in zip(sensor_info_list, process_type_list):
        sensor_id, table_name, sensor_name = sensor_info.split('|')
        
        # Asegurar esquema master en la consulta del reporte también
        full_table_name = table_name
        if "." not in table_name:
             full_table_name = f"master.{table_name}"
        
        base_select = "e.id AS ema_id, e.nombre AS nombre_ema, e.descripcion_lugar AS descripcion_ema, e.latitud, e.longitud, %s AS sensor_nombre"
        group_by_ema_cols = "1, 2, 3, 4, 5, 6" 

        time_cols, value_col, group_by_time_cols = "", "", ""

        if process_type == 'raw':
            time_cols = "t.tiempo_de_medicion, NULL::date AS dia, NULL::timestamp AS hora"
            value_col = "t.valor"
        elif process_type == 'pluvio_sum':
            time_cols = "NULL::timestamp AS tiempo_de_medicion, CAST(t.tiempo_de_medicion AS date) AS dia, NULL::timestamp AS hora"
            value_col = "SUM(t.valor) AS valor"
            group_by_time_cols = "dia"
        elif process_type == 'nivel_max':
            time_cols = "NULL::timestamp AS tiempo_de_medicion, CAST(t.tiempo_de_medicion AS date) AS dia, NULL::timestamp AS hora"
            value_col = "MAX(t.valor) AS valor"
            group_by_time_cols = "dia"
        elif process_type == 'avg_hourly':
            time_cols = "NULL::timestamp AS tiempo_de_medicion, NULL::date AS dia, date_trunc('hour', t.tiempo_de_medicion) AS hora"
            value_col = "ROUND(AVG(t.valor), 3) AS valor" 
            group_by_time_cols = "hora"
        elif process_type == 'sum_hourly':
            time_cols = "NULL::timestamp AS tiempo_de_medicion, NULL::date AS dia, date_trunc('hour', t.tiempo_de_medicion) AS hora"
            value_col = "SUM(t.valor) AS valor"
            group_by_time_cols = "hora"
        elif process_type == 'max_hourly':
            time_cols = "NULL::timestamp AS tiempo_de_medicion, NULL::date AS dia, date_trunc('hour', t.tiempo_de_medicion) AS hora"
            value_col = "MAX(t.valor) AS valor"
            group_by_time_cols = "hora"

        base_join = f"FROM {full_table_name} t JOIN master.estacion e ON t.id_ema = e.id"
        
        where_conditions = []
        where_params = []
        
        if ema_id_form == 'todas':
            where_conditions.append("t.id_sensor IN (SELECT id FROM master.sensor WHERE LOWER(nombre) = LOWER(%s))")
            where_params.append(sensor_name)
        else:
            where_conditions.append("t.id_ema = %s")
            where_params.append(int(ema_id_form))
            where_conditions.append("t.id_sensor = %s")
            where_params.append(int(sensor_id))
        
        where_conditions.append("t.tiempo_de_medicion >= %s")
        where_params.append(FECHA_INICIO_SQL)
        where_conditions.append("t.tiempo_de_medicion < %s")
        where_params.append(FECHA_FIN_SQL)
        
        base_where = "WHERE " + " AND ".join(where_conditions)
        
        group_by_suffix = ""
        if process_type == 'raw':
            group_by_suffix = "" 
        else:
            group_by_suffix = f"GROUP BY {group_by_ema_cols}, {group_by_time_cols}"

        tipo_proceso_display = PROCESS_TYPE_TRANSLATION.get(process_type, process_type)
        
        query_part = f"(SELECT {base_select}, {time_cols}, {value_col}, %s AS tipo_procesamiento {base_join} {base_where} {group_by_suffix})"
        
        query_params = [sensor_name, tipo_proceso_display] + where_params
        
        all_queries.append(query_part)
        all_params.extend(query_params)
    
    if not all_queries: return pd.DataFrame() 

    SQL_QUERY = " \nUNION ALL\n ".join(all_queries)
    SQL_QUERY += " ORDER BY ema_id ASC, sensor_nombre ASC, tiempo_de_medicion ASC, dia ASC, hora ASC;"
    
    conn = get_db_connection(db_key)
    try:
        df = pd.read_sql_query(SQL_QUERY, conn, params=all_params)
        return df
    except Exception as e_pd_query:
        print(f"Error en consulta ({db_key}): {e_pd_query}")
        raise Exception("Error al consultar datos.") from e_pd_query
    finally:
        if conn: conn.close()

# (Las funciones de create_excel, get_ema_list, get_ema_locations, get_ema_live_summary, get_dashboard_data quedan IGUAL)
# Solo asegúrate de copiar y pegar el archivo completo o mantener las otras funciones intactas.

def create_excel_from_dataframe(df):
    output = io.BytesIO()
    df_to_export = df.copy()
    if 'ema_id' in df_to_export.columns: df_to_export = df_to_export.drop(columns=['ema_id'])
    df_to_export.to_excel(output, index=False, sheet_name='Datos', engine='openpyxl')
    return output

def get_ema_list_repo(db_key):
    SQL_QUERY = "SELECT id, nombre FROM master.estacion ORDER BY LENGTH(nombre) ASC, nombre ASC;"
    try:
        conn = get_db_connection(db_key)
        cursor = conn.cursor()
        cursor.execute(SQL_QUERY)
        res = cursor.fetchall()
        cursor.close(); conn.close()
        return res
    except: return []

def get_ema_locations_repo(db_key):
    SQL_QUERY = "SELECT id, nombre, descripcion_lugar, latitud, longitud FROM master.estacion WHERE latitud IS NOT NULL AND longitud IS NOT NULL;"
    locations = []
    try:
        conn = get_db_connection(db_key)
        cursor = conn.cursor()
        cursor.execute(SQL_QUERY)
        for row in cursor.fetchall():
            locations.append({'id': row[0], 'nombre': row[1], 'descripcion': row[2] or "Sin descripción.", 'lat': float(row[3]), 'lon': float(row[4])})
        cursor.close(); conn.close()
        return locations
    except: return []

def get_ema_live_summary_repo(db_key, ema_id):
    data = { 'temperatura': None, 'nivel_max_hoy': None, 'pluvio_sum_hoy': None }
    queries = {
        'temperatura': "SELECT valor FROM master.medicion_temperatura_atmosferica WHERE id_ema = %s ORDER BY tiempo_de_medicion DESC LIMIT 1",
        'nivel_max_hoy': "SELECT MAX(valor) FROM master.medicion_limnigrafica WHERE id_ema = %s AND tiempo_de_medicion >= CURRENT_DATE",
        'pluvio_sum_hoy': "SELECT SUM(valor) FROM master.medicion_pluviometrica WHERE id_ema = %s AND tiempo_de_medicion >= CURRENT_DATE"
    }
    try:
        conn = get_db_connection(db_key)
        cursor = conn.cursor()
        for key, sql in queries.items():
            try:
                cursor.execute(sql, (ema_id,))
                result = cursor.fetchone()
                if result and result[0] is not None: data[key] = result[0]
            except: conn.rollback()
        cursor.close(); conn.close()
        return data
    except: return data

def get_dashboard_data_repo(db_key, ema_id):
    data = {'temperatura': None, 'nivel_max_hoy': None, 'pluvio_sum_hoy': None, 'viento_vel': None, 'viento_dir': None}
    queries = {
        'temperatura': "SELECT valor, tiempo_de_medicion FROM master.medicion_temperatura_atmosferica WHERE id_ema = %s ORDER BY tiempo_de_medicion DESC LIMIT 1",
        'nivel_max_hoy': "SELECT MAX(valor) FROM master.medicion_limnigrafica WHERE id_ema = %s AND tiempo_de_medicion >= CURRENT_DATE",
        'pluvio_sum_hoy': "SELECT SUM(valor) FROM master.medicion_pluviometrica WHERE id_ema = %s AND tiempo_de_medicion >= CURRENT_DATE",
        'viento_vel': "SELECT valor, tiempo_de_medicion FROM master.medicion_anemometrica WHERE id_ema = %s ORDER BY tiempo_de_medicion DESC LIMIT 1",
        'viento_dir': "SELECT valor, tiempo_de_medicion FROM master.medicion_direccion_viento WHERE id_ema = %s ORDER BY tiempo_de_medicion DESC LIMIT 1"
    }
    try:
        conn = get_db_connection(db_key)
        cursor = conn.cursor()
        for key, sql in queries.items():
            try:
                cursor.execute(sql, (ema_id,))
                result = cursor.fetchone()
                if result and result[0] is not None:
                    if key in ['temperatura', 'viento_vel', 'viento_dir']: data[key] = {'valor': result[0], 'timestamp': result[1]}
                    else: data[key] = {'valor': result[0]}
            except: conn.rollback()
        cursor.close(); conn.close()
        return data
    except: return data