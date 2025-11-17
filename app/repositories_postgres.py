# app/repositories_postgres.py
# (Este archivo habla con la base de datos PostgreSQL)

import psycopg2
import pandas as pd
import io
import csv
from datetime import datetime, timedelta
import config 

# --- (El mapa PROCESS_TYPE_TRANSLATION... queda igual) ---
PROCESS_TYPE_TRANSLATION = {
    'raw': 'Dato Crudo',
    'pluvio_sum': 'Acumulado Diario',
    'nivel_max': 'Maximo Diario',
    'avg_hourly': 'Promedio por Hora',
    'sum_hourly': 'Acumulado por Hora',
    'max_hourly': 'Maximo por Hora'
}

# ==============================================================================
# === CONEXIÓN A LA BD (MODIFICADO) ============================================
# ==============================================================================
def get_db_connection(db_key):
    """
    Establece conexión usando una clave específica del diccionario
    DATABASE_CONNECTIONS en config.py
    """
    db_config = config.DATABASE_CONNECTIONS.get(db_key)
    
    if not db_config or db_config['driver'] != 'psycopg2':
        raise ValueError(f"Configuración de DB no válida o faltante para PostgreSQL: {db_key}")

    # Conexión a PostgreSQL
    conn = psycopg2.connect(
        host=db_config['host'],
        port=db_config['port'],
        dbname=db_config['name'],
        user=db_config['user'],
        password=db_config['pass']
    )
    return conn

# ==============================================================================
# === CACHE DE SENSORES (MODIFICADO) ===========================================
# ==============================================================================
def build_active_sensor_cache(db_key):
    """Construye el cache para la base de datos PostgreSQL"""
    
    print(f"--- Construyendo cache para: {db_key} (PostgreSQL) ---")
    QUERY = "SELECT DISTINCT id_ema, id_sensor FROM master.medicion_anemometrica UNION SELECT DISTINCT id_ema, id_sensor FROM master.medicion_barometrica UNION SELECT DISTINCT id_ema, id_sensor FROM master.medicion_bateria UNION SELECT DISTINCT id_ema, id_sensor FROM master.medicion_conductiva UNION SELECT DISTINCT id_ema, id_sensor FROM master.medicion_direccion_viento UNION SELECT DISTINCT id_ema, id_sensor FROM master.medicion_freatimetrica UNION SELECT DISTINCT id_ema, id_sensor FROM master.medicion_humedad UNION SELECT DISTINCT id_ema, id_sensor FROM master.medicion_limnigrafica UNION SELECT DISTINCT id_ema, id_sensor FROM master.medicion_ph UNION SELECT DISTINCT id_ema, id_sensor FROM master.medicion_piranometrica UNION SELECT DISTINCT id_ema, id_sensor FROM master.medicion_pluviometrica UNION SELECT DISTINCT id_ema, id_sensor FROM master.medicion_punto_rocio UNION SELECT DISTINCT id_ema, id_sensor FROM master.medicion_temperatura_atmosferica UNION SELECT DISTINCT id_ema, id_sensor FROM master.medicion_temperatura_del_curso UNION SELECT DISTINCT id_ema, id_sensor FROM master.medicion_turbidimetrica;"
    
    temp_cache_db = {}
    try:
        conn = get_db_connection(db_key)
        try:
             df = pd.read_sql_query(QUERY, conn)
             for index, row in df.iterrows():
                 ema_id = int(row['id_ema'])
                 sensor_id = int(row['id_sensor'])
                 if ema_id not in temp_cache_db: temp_cache_db[ema_id] = []
                 temp_cache_db[ema_id].append(sensor_id)
        except Exception as e_pd:
            print(f"Advertencia al leer con pandas (fallback a cursor) para {db_key}: {e_pd}")
            cursor = conn.cursor()
            cursor.execute(QUERY)
            results = cursor.fetchall(); cursor.close()
            for row in results:
                ema_id = int(row[0]); sensor_id = int(row[1])
                if ema_id not in temp_cache_db: temp_cache_db[ema_id] = []
                temp_cache_db[ema_id].append(sensor_id)
        conn.close()
        print(f"¡Cache para {db_key} construido con éxito!")
        return temp_cache_db
    except Exception as e:
        print(f"!!! ERROR CRÍTICO al construir el cache para {db_key}: {e}")
        return {} # Devuelve caché vacío si falla

# ==============================================================================
# === REPOSITORIO DE SENSORES (PostgreSQL) =====================================
# ==============================================================================

def get_sensors_for_ema_repo(db_key, G_SENSOR_CACHE, ema_id):
    """
    Busca los sensores para una EMA (o todas)
    de la base de datos PostgreSQL seleccionada.
    """
    
    db_cache = G_SENSOR_CACHE.get(db_key)
    if not db_cache:
        print(f"Advertencia: No se encontró cache para {db_key}")
        return []

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
            print(f"ID de EMA no válido recibido en repo: {ema_id}")
            return []
            
    if not active_sensor_ids: 
        return []

    try:
        conn = get_db_connection(db_key)
        cursor = conn.cursor()
        
        SQL_QUERY = "SELECT id, nombre, descripcion FROM master.sensor WHERE id = ANY(%s) ORDER BY LOWER(nombre) ASC, nombre ASC;"
        cursor.execute(SQL_QUERY, (list(active_sensor_ids),)) 
        
        sensores_unicos_por_nombre = {} 
        
        for row in cursor.fetchall():
            sensor_id, sensor_nombre, sensor_desc = row
            table_name = None
            search_text = (str(sensor_nombre) + " " + str(sensor_desc)).lower()
            
            for table, keywords in config.TABLE_KEYWORD_MAP.items():
                if any(keyword in search_text for keyword in keywords):
                    table_name = table
                    break
            
            if table_name:
                sensor_data = {
                    'id': sensor_id, 
                    'nombre': sensor_nombre.title(), 
                    'table_name': table_name, 
                    'search_text': search_text
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
        print(f"Error en get_sensors_for_ema_repo ({db_key}): {e}")
        import traceback; traceback.print_exc()
        raise e 

# ==============================================================================
# === REPOSITORIO DE REPORTES (PostgreSQL) =====================================
# ==============================================================================

def generate_report_repo(db_key, ema_id_form, fecha_inicio_str, fecha_fin_str, sensor_info_list, process_type_list):
    """
    Construye y ejecuta la consulta SQL principal para generar el reporte.
    Usa la base de datos PostgreSQL seleccionada.
    """
    
    fecha_fin_obj = datetime.strptime(fecha_fin_str, '%Y-%m-%d')
    fecha_fin_para_sql_obj = fecha_fin_obj + timedelta(days=1)
    FECHA_INICIO_SQL = fecha_inicio_str
    FECHA_FIN_SQL = fecha_fin_para_sql_obj.strftime('%Y-%m-%d')
    
    all_queries = []
    all_params = []
    
    for sensor_info, process_type in zip(sensor_info_list, process_type_list):
        sensor_id, table_name, sensor_name = sensor_info.split('|')
        
        if table_name not in config.ALLOWED_TABLES: 
            print(f"Advertencia: Tabla no permitida '{table_name}'. Omitiendo sensor.")
            continue 

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

        base_join = f"FROM {table_name} t JOIN master.estacion e ON t.id_ema = e.id"
        
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
    
    if not all_queries: 
        print("No se generaron consultas válidas.")
        return pd.DataFrame() 

    SQL_QUERY = " \nUNION ALL\n ".join(all_queries)
    SQL_QUERY += " ORDER BY ema_id ASC, sensor_nombre ASC, tiempo_de_medicion ASC, dia ASC, hora ASC;"
    
    conn = get_db_connection(db_key)
    try:
        df = pd.read_sql_query(SQL_QUERY, conn, params=all_params)
        return df
    except Exception as e_pd_query:
        print(f"Error en consulta ({db_key}): {e_pd_query}")
        raise Exception("Pandas no disponible o falló query principal.") from e_pd_query
    finally:
        if conn:
            conn.close()

# ==============================================================================
# === REPOSITORIO DE EXCEL =====================================================
# ==============================================================================

def create_excel_from_dataframe(df):
    """Convierte un DataFrame a un objeto BytesIO (Excel en memoria)."""
    output = io.BytesIO()
    
    df_to_export = df.copy()
    if 'ema_id' in df_to_export.columns:
        df_to_export = df_to_export.drop(columns=['ema_id'])

    df_to_export.to_excel(output, index=False, sheet_name='Datos', engine='openpyxl')
    return output


# ==============================================================================
# === REPOSITORIO DE EMAS (PostgreSQL) =========================================
# ==============================================================================
def get_ema_list_repo(db_key):
    """Busca en la BD (db_key) la lista de todas las EMAs (ID y Nombre)."""
    SQL_QUERY = "SELECT id, nombre FROM master.estacion ORDER BY LENGTH(nombre) ASC, nombre ASC;"
    try:
        conn = get_db_connection(db_key)
        cursor = conn.cursor()
        cursor.execute(SQL_QUERY)
        emas_raw_list = cursor.fetchall() # Devuelve lista de tuplas (ej: [(1, 'EMA 1'), ...])
        cursor.close()
        conn.close()
        return emas_raw_list
    except Exception as e:
        print(f"Error al buscar lista de EMAs en repo ({db_key}): {e}")
        return []

# ==============================================================================
# === REPOSITORIO DE MAPA (PostgreSQL) =========================================
# ==============================================================================
def get_ema_locations_repo(db_key):
    """
    Busca id, nombre, descripcion, lat y lon de todas las EMAs
    de la base de datos (db_key) seleccionada.
    """
    SQL_QUERY = """
        SELECT id, nombre, descripcion_lugar, latitud, longitud 
        FROM master.estacion 
        WHERE latitud IS NOT NULL AND longitud IS NOT NULL;
    """
    locations = []
    try:
        conn = get_db_connection(db_key)
        cursor = conn.cursor()
        cursor.execute(SQL_QUERY)
        for row in cursor.fetchall():
            locations.append({
                'id': row[0],
                'nombre': row[1],
                'descripcion': row[2] or "Sin descripción.",
                'lat': float(row[3]),
                'lon': float(row[4])
            })
        cursor.close()
        conn.close()
        return locations
    except Exception as e:
        print(f"Error al buscar locaciones de EMAs en repo ({db_key}): {e}")
        return []

# ==============================================================================
# === REPOSITORIO DE POPUP MAPA (PostgreSQL) ===================================
# ==============================================================================
def get_ema_live_summary_repo(db_key, ema_id):
    """
    Busca los 3 datos "frescos" para el popup del mapa
    de la base de datos (db_key) seleccionada.
    """
    data = { 'temperatura': None, 'nivel_max_hoy': None, 'pluvio_sum_hoy': None }
    queries = {
        'temperatura': """
            SELECT valor FROM master.medicion_temperatura_atmosferica 
            WHERE id_ema = %s 
            ORDER BY tiempo_de_medicion DESC LIMIT 1
        """,
        'nivel_max_hoy': """
            SELECT MAX(valor) FROM master.medicion_limnigrafica 
            WHERE id_ema = %s AND tiempo_de_medicion >= CURRENT_DATE
        """,
        'pluvio_sum_hoy': """
            SELECT SUM(valor) FROM master.medicion_pluviometrica 
            WHERE id_ema = %s AND tiempo_de_medicion >= CURRENT_DATE
        """
    }
    try:
        conn = get_db_connection(db_key)
        cursor = conn.cursor()
        for key, sql in queries.items():
            try:
                cursor.execute(sql, (ema_id,))
                result = cursor.fetchone()
                if result and result[0] is not None:
                    data[key] = result[0]
            except Exception as e_query:
                print(f"Error al buscar dato '{key}' para EMA {ema_id} ({db_key}): {e_query}")
                conn.rollback() 
        cursor.close()
        conn.close()
        return data
    except Exception as e:
        print(f"Error mayor en get_ema_live_summary_repo ({db_key}): {e}")
        return data

# ==============================================================================
# === REPOSITORIO DE DASHBOARD (PostgreSQL) ====================================
# ==============================================================================
def get_dashboard_data_repo(db_key, ema_id):
    """
    Busca los 4 datos "frescos" para el nuevo dashboard
    de la base de datos (db_key) seleccionada.
    """
    data = {
        'temperatura': None, 'nivel_max_hoy': None, 'pluvio_sum_hoy': None,
        'viento_vel': None, 'viento_dir': None
    }
    queries = {
        'temperatura': """
            SELECT valor, tiempo_de_medicion FROM master.medicion_temperatura_atmosferica 
            WHERE id_ema = %s ORDER BY tiempo_de_medicion DESC LIMIT 1
        """,
        'nivel_max_hoy': """
            SELECT MAX(valor) FROM master.medicion_limnigrafica 
            WHERE id_ema = %s AND tiempo_de_medicion >= CURRENT_DATE
        """,
        'pluvio_sum_hoy': """
            SELECT SUM(valor) FROM master.medicion_pluviometrica 
            WHERE id_ema = %s AND tiempo_de_medicion >= CURRENT_DATE
        """,
        'viento_vel': """
            SELECT valor, tiempo_de_medicion FROM master.medicion_anemometrica 
            WHERE id_ema = %s ORDER BY tiempo_de_medicion DESC LIMIT 1
        """,
        'viento_dir': """
            SELECT valor, tiempo_de_medicion FROM master.medicion_direccion_viento 
            WHERE id_ema = %s ORDER BY tiempo_de_medicion DESC LIMIT 1
        """
    }
    try:
        conn = get_db_connection(db_key)
        cursor = conn.cursor()
        for key, sql in queries.items():
            try:
                cursor.execute(sql, (ema_id,))
                result = cursor.fetchone()
                if result and result[0] is not None:
                    if key in ['temperatura', 'viento_vel', 'viento_dir']:
                        data[key] = {'valor': result[0], 'timestamp': result[1]}
                    else:
                        data[key] = {'valor': result[0]}
            except Exception as e_query:
                print(f"Error al buscar dato '{key}' para EMA {ema_id} ({db_key}): {e_query}")
                conn.rollback() 
        cursor.close()
        conn.close()
        return data
    except Exception as e:
        print(f"Error mayor en get_dashboard_data_repo ({db_key}): {e}")
        return data