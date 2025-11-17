# app/repositories_sqlserver.py
# (Este archivo habla con la base de datos SQL Server)
# [CORRECCIÓN FINAL: "Dato Crudo" de Pluvio (ID 7) vuelve a ser T.Valor (el acumulador)]

import pyodbc
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
    
    if not db_config or db_config['driver'] != 'pyodbc':
        raise ValueError(f"Configuración de DB no válida o faltante para SQL Server: {db_key}")

    # Conexión a SQL Server
    conn_str = (
        f"DRIVER={{{db_config['odbc_driver']}}};"
        f"SERVER={db_config['host']},{db_config['port']};"
        f"DATABASE={db_config['name']};"
        f"UID={db_config['user']};"
        f"PWD={db_config['pass']};"
    )
    conn = pyodbc.connect(conn_str)
    return conn

# ==============================================================================
# === CACHE DE SENSORES (¡CORREGIDO!) ==========================================
# ==============================================================================
def build_active_sensor_cache(db_key):
    """Construye el cache para la base de datos SQL Server"""
    
    print(f"--- Construyendo cache para: {db_key} (SQL Server) ---")
    
    # (Usamos el nombre de tabla correcto: dbo.SensoresRemotas)
    QUERY = """
        SELECT DISTINCT idRemotas, idSensores 
        FROM dbo.SensoresRemotas
        WHERE idRemotas IS NOT NULL AND idSensores IS NOT NULL;
    """
    
    temp_cache_db = {}
    try:
        conn = get_db_connection(db_key)
        try:
             df = pd.read_sql_query(QUERY, conn)
             col_ema = 'idRemotas'
             col_sensor = 'idSensores'
             
             for index, row in df.iterrows():
                 ema_id = int(row[col_ema])
                 sensor_id = int(row[col_sensor])
                 if ema_id not in temp_cache_db: temp_cache_db[ema_id] = []
                 temp_cache_db[ema_id].append(sensor_id)
        
        except Exception as e_pd:
            print(f"Advertencia al leer con pandas (fallback a cursor) para {db_key}: {e_pd}")
            cursor = conn.cursor()
            cursor.execute(QUERY)
            results = cursor.fetchall(); 
            for row in results:
                ema_id = int(row[0]); sensor_id = int(row[1])
                if ema_id not in temp_cache_db: temp_cache_db[ema_id] = []
                temp_cache_db[ema_id].append(sensor_id)
            cursor.close()
        
        conn.close()
        print(f"¡Cache para {db_key} construido con éxito! (Encontradas {len(temp_cache_db)} EMAs)")
        return temp_cache_db
        
    except Exception as e:
        print(f"!!! ERROR CRÍTICO al construir el cache para {db_key}: {e}")
        return {} 

# ==============================================================================
# === HELPER DE COORDENADAS (¡NUEVO!) ==========================================
# ==============================================================================
def dms_to_dd(g, m, s, direccion):
    """Convierte Grados, Minutos, Segundos a Grados Decimales"""
    try:
        g = float(g) if g is not None else 0
        m = float(m) if m is not None else 0
        s = float(s) if s is not None else 0
        
        decimal = g + (m / 60) + (s / 3600)
        
        # O (Oeste) y S (Sur) son negativos
        if direccion in ['O', 'S']:
            decimal = -decimal
        return decimal
    except Exception as e:
        print(f"Error convirtiendo DMS a DD: {g} {m} {s} ({e})")
        return 0.0

# ==============================================================================
# === REPOSITORIO DE SENSORES (SQL Server) [¡CORREGIDO POR ID!] ================
# ==============================================================================
def get_sensors_for_ema_repo(db_key, G_SENSOR_CACHE, ema_id):
    """
    Busca los sensores para una EMA (o todas)
    de la base de datos SQL Server seleccionada.
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
        print(f"Advertencia: La EMA {ema_id} no tiene sensores en el caché.")
        return []

    try:
        conn = get_db_connection(db_key)
        cursor = conn.cursor()
        
        placeholders = ','.join(['?'] * len(active_sensor_ids))
        
        # (Quitamos 'Observaciones' de la consulta)
        SQL_QUERY = f"SELECT id, Nombre FROM dbo.Sensores WHERE id IN ({placeholders}) ORDER BY LOWER(Nombre) ASC, Nombre ASC;"
        params = list(active_sensor_ids)
        
        cursor.execute(SQL_QUERY, params)
        
        sensores_unicos_por_nombre = {} 

        for row in cursor.fetchall():
            sensor_id, sensor_nombre = row
            
            table_name = None
            search_text = (str(sensor_nombre) or "").lower()
            
            # (Usamos los IDs que nos diste)
            if sensor_id == 7: # Pluviometro
                table_name = 'pluviometro'
            elif sensor_id == 8: # Bateria
                table_name = 'bateria'
            elif sensor_id == 15: # Press Atmosférica
                table_name = 'presion'
            
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
        
        if ema_id == 'todas':
            sensores_encontrados = list(sensores_unicos_por_nombre.values())

        cursor.close()
        conn.close()
        
        print(f"--- DEBUG SENSORES ({db_key}): Encontrados {len(sensores_encontrados)} sensores válidos para EMA {ema_id}.")
        return sensores_encontrados
            
    except Exception as e:
        print(f"Error en get_sensors_for_ema_repo ({db_key}): {e}")
        import traceback; traceback.print_exc()
        raise e 

# ==============================================================================
# === REPOSITORIO DE REPORTES (SQL Server) [¡CORREGIDO!] =======================
# ==============================================================================
def generate_report_repo(db_key, ema_id_form, fecha_inicio_str, fecha_fin_str, sensor_info_list, process_type_list):
    """
    Construye y ejecuta la consulta SQL principal para generar el reporte.
    Usa la base de datos SQL Server seleccionada.
    """
    
    FECHA_INICIO_SQL = fecha_inicio_str
    FECHA_FIN_SQL = (datetime.strptime(fecha_fin_str, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    
    all_queries = []
    all_params = []
    
    for sensor_info, process_type in zip(sensor_info_list, process_type_list):
        sensor_id, table_name, sensor_name = sensor_info.split('|')
        sensor_id_int = int(sensor_id) # Usamos el ID para la lógica

        base_select = "e.id AS ema_id, e.Nombre AS nombre_ema, e.Observaciones AS descripcion_ema, NULL AS latitud, NULL AS longitud, ? AS sensor_nombre"
        group_by_ema_cols = "e.id, e.Nombre, e.Observaciones" 

        time_cols, value_col, group_by_time_cols = "", "", ""

        # --- ¡LÓGICA CORREGIDA! ---
        
        if process_type == 'raw':
            # (Dato crudo para CUALQUIER sensor)
            # Esto devuelve el valor del contador tal cual está en la DB,
            # que es lo que querías.
            time_cols = "t.FechaDelDato AS tiempo_de_medicion, NULL AS dia, NULL AS hora"
            value_col = "t.Valor AS valor"
            group_by_time_cols = ""
        
        elif (process_type == 'pluvio_sum' or process_type == 'sum_hourly') and sensor_id_int == 7:
            # (Usamos MAX-MIN para el total diario u horario DEL PLUVIO)
            if process_type == 'pluvio_sum':
                time_cols = "NULL AS tiempo_de_medicion, CAST(t.FechaDelDato AS date) AS dia, NULL AS hora"
                group_by_time_cols = "CAST(t.FechaDelDato AS date)"
            else: # sum_hourly
                time_cols = "NULL AS tiempo_de_medicion, NULL AS dia, DATEADD(hour, DATEPART(hour, t.FechaDelDato), CAST(CAST(t.FechaDelDato AS date) AS datetime)) AS hora"
                group_by_time_cols = "DATEADD(hour, DATEPART(hour, t.FechaDelDato), CAST(CAST(t.FechaDelDato AS date) AS datetime))"
            
            value_col = "MAX(t.Valor) - MIN(t.Valor) AS valor"
        
        elif process_type == 'nivel_max':
            time_cols = "NULL AS tiempo_de_medicion, CAST(t.FechaDelDato AS date) AS dia, NULL AS hora"
            value_col = "MAX(t.Valor) AS valor"
            group_by_time_cols = "CAST(t.FechaDelDato AS date)"
        
        elif process_type == 'avg_hourly':
            time_cols = "NULL AS tiempo_de_medicion, NULL AS dia, DATEADD(hour, DATEPART(hour, t.FechaDelDato), CAST(CAST(t.FechaDelDato AS date) AS datetime)) AS hora"
            value_col = "ROUND(AVG(t.Valor), 3) AS valor"
            group_by_time_cols = "DATEADD(hour, DATEPART(hour, t.FechaDelDato), CAST(CAST(t.FechaDelDato AS date) AS datetime))"
        
        elif process_type == 'max_hourly':
            time_cols = "NULL AS tiempo_de_medicion, NULL AS dia, DATEADD(hour, DATEPART(hour, t.FechaDelDato), CAST(CAST(t.FechaDelDato AS date) AS datetime)) AS hora"
            value_col = "MAX(t.Valor) AS valor"
            group_by_time_cols = "DATEADD(hour, DATEPART(hour, t.FechaDelDato), CAST(CAST(t.FechaDelDato AS date) AS datetime))"

        # (Los JOINS de SQL Server - Corregidos a plural)
        base_join = (
            "FROM dbo.DatosUTR t "
            "JOIN dbo.SensoresRemotas sr ON t.idSensoresRemotas = sr.id "
            "JOIN dbo.Remotas e ON sr.idRemotas = e.id "
            "JOIN dbo.Sensores s ON sr.idSensores = s.id"
        )
        
        where_conditions = []
        where_params = []
        
        if ema_id_form == 'todas':
            where_conditions.append("LOWER(s.Nombre) = LOWER(?)")
            where_params.append(sensor_name)
        else:
            where_conditions.append("e.id = ?")
            where_params.append(int(ema_id_form))
            where_conditions.append("s.id = ?")
            where_params.append(sensor_id_int)
        
        where_conditions.append("t.FechaDelDato >= ?")
        where_params.append(FECHA_INICIO_SQL)
        where_conditions.append("t.FechaDelDato < ?")
        where_params.append(FECHA_FIN_SQL)
        
        base_where = "WHERE " + " AND ".join(where_conditions)
        
        group_by_suffix = ""
        if process_type != 'raw':
            group_by_suffix = f"GROUP BY {group_by_ema_cols}, {group_by_time_cols}"

        tipo_proceso_display = PROCESS_TYPE_TRANSLATION.get(process_type, process_type)
        
        query_part = f"(SELECT {base_select}, {time_cols}, {value_col}, ? AS tipo_procesamiento {base_join} {base_where} {group_by_suffix})"
        
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
# === REPOSITORIO DE EMAS (SQL Server) [¡CORREGIDO!] ============================
# ==============================================================================
def get_ema_list_repo(db_key):
    """Busca en la BD (db_key) la lista de todas las EMAs (ID y Nombre)."""
    
    SQL_QUERY = "SELECT id, Nombre FROM dbo.Remotas ORDER BY Nombre ASC;"
        
    try:
        conn = get_db_connection(db_key)
        cursor = conn.cursor()
        cursor.execute(SQL_QUERY)
        emas_raw_list = cursor.fetchall() 
        cursor.close()
        conn.close()
        return emas_raw_list
    except Exception as e:
        print(f"Error al buscar lista de EMAs en repo ({db_key}): {e}")
        return []

# ==============================================================================
# === REPOSITORIO DE MAPA (SQL Server) [¡CORREGIDO!] ===========================
# ==============================================================================
def get_ema_locations_repo(db_key):
    """
    Busca id, nombre, descripcion, lat y lon de todas las EMAs
    de la base de datos (db_key) seleccionada.
    """
    
    locations = []
    
    SQL_QUERY = """
        SELECT id, Nombre, Observaciones, 
               LatGrados, LatMinutos, LatSegundos, 
               LongGrados, LongMinutos, LongSegundos
        FROM dbo.Remotas 
        WHERE LatGrados IS NOT NULL AND LongGrados IS NOT NULL;
    """
    try:
        conn = get_db_connection(db_key)
        cursor = conn.cursor()
        cursor.execute(SQL_QUERY)
        for row in cursor.fetchall():
            lat = dms_to_dd(row[3], row[4], row[5], 'S') 
            lon = dms_to_dd(row[6], row[7], row[8], 'O')
            
            if lat != 0.0 or lon != 0.0:
                locations.append({
                    'id': row[0],
                    'nombre': row[1],
                    'descripcion': row[2] or "Sin descripción.",
                    'lat': lat,
                    'lon': lon
                })
        
        cursor.close()
        conn.close()
        return locations
        
    except Exception as e:
        print(f"!!! ERROR CRÍTICO al buscar locaciones de EMAs en repo ({db_key}): {e}")
        return []

# ==============================================================================
# === REPOSITORIO DE POPUP MAPA (SQL Server) [¡CORREGIDO POR ID!] ==============
# ==============================================================================
def get_ema_live_summary_repo(db_key, ema_id):
    """
    Busca los 3 datos "frescos" para el popup del mapa
    de la base de datos (db_key) seleccionada.
    (Buscamos Pluvio (7), Batería (8) y Presión (15))
    """
    
    # (Cambiamos 'temperatura' y 'nivel' por 'presion' y 'bateria')
    data = { 'presion': None, 'bateria': None, 'pluvio_sum_hoy': None }
    
    queries = {
        'presion': """
            SELECT TOP 1 d.Valor 
            FROM dbo.DatosUTR d
            JOIN dbo.SensoresRemotas sr ON d.idSensoresRemotas = sr.id
            WHERE sr.idRemotas = ? AND sr.idSensores = 15
            ORDER BY d.FechaDelDato DESC
        """,
        'bateria': """
            SELECT TOP 1 d.Valor
            FROM dbo.DatosUTR d
            JOIN dbo.SensoresRemotas sr ON d.idSensoresRemotas = sr.id
            WHERE sr.idRemotas = ? AND sr.idSensores = 8
            ORDER BY d.FechaDelDato DESC
        """,
        'pluvio_sum_hoy': """
            SELECT MAX(d.Valor) - MIN(d.Valor)
            FROM dbo.DatosUTR d
            JOIN dbo.SensoresRemotas sr ON d.idSensoresRemotas = sr.id
            WHERE sr.idRemotas = ? AND sr.idSensores = 7
            AND d.FechaDelDato >= CAST(GETDATE() AS date)
        """
        # (¡CORREGIDO! Usamos MAX-MIN en lugar de SUM)
    }
    params = (ema_id,) 

    try:
        conn = get_db_connection(db_key)
        cursor = conn.cursor()
        for key, sql in queries.items():
            try:
                cursor.execute(sql, params * sql.count('?'))
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
# === REPOSITORIO DE DASHBOARD (SQL Server) [¡CORREGIDO POR ID!] ==============
# ==============================================================================
def get_dashboard_data_repo(db_key, ema_id):
    """
    Busca los datos "frescos" para el nuevo dashboard
    de la base de datos (db_key) seleccionada.
    (Buscamos Pluvio (7), Batería (8) y Presión (15))
    """
    
    # (Quitamos temp, nivel y viento. Dejamos solo los 3 que existen)
    data = {
        'bateria': None, 'presion': None, 'pluvio_sum_hoy': None
    }
    queries = {
        'bateria': """
            SELECT TOP 1 d.Valor, d.FechaDelDato 
            FROM dbo.DatosUTR d
            JOIN dbo.SensoresRemotas sr ON d.idSensoresRemotas = sr.id
            WHERE sr.idRemotas = ? AND sr.idSensores = 8
            ORDER BY d.FechaDelDato DESC
        """,
        'presion': """
            SELECT TOP 1 d.Valor, d.FechaDelDato
            FROM dbo.DatosUTR d
            JOIN dbo.SensoresRemotas sr ON d.idSensoresRemotas = sr.id
            WHERE sr.idRemotas = ? AND sr.idSensores = 15
            ORDER BY d.FechaDelDato DESC
        """,
        'pluvio_sum_hoy': """
            SELECT MAX(d.Valor) - MIN(d.Valor)
            FROM dbo.DatosUTR d
            JOIN dbo.SensoresRemotas sr ON d.idSensoresRemotas = sr.id
            WHERE sr.idRemotas = ? AND sr.idSensores = 7
            AND d.FechaDelDato >= CAST(GETDATE() AS date)
        """
        # (¡CORREGIDO! Usamos MAX-MIN en lugar de SUM)
    }
    params = (ema_id,)
        
    try:
        conn = get_db_connection(db_key)
        cursor = conn.cursor()
        for key, sql in queries.items():
            try:
                cursor.execute(sql, params * sql.count('?')) 
                result = cursor.fetchone()
                if result and result[0] is not None:
                    # (Bateria y Presion tienen timestamp)
                    if key in ['bateria', 'presion']:
                        data[key] = {'valor': result[0], 'timestamp': result[1]}
                    # (Pluvio es un MAX-MIN, no tiene timestamp)
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