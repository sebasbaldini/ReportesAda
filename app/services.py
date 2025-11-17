# app/services.py
# [CORRECCIÓN FINAL: Arreglado el error de 'round(dict)' para Postgres]

from . import repositories_postgres
from . import repositories_sqlserver
import config
import pandas as pd 
from datetime import datetime

# (El mapa de municipios solo se usará para 'db_principal')
EMA_MUNICIPIO_MAP = {
    1: "Tigre", 2: "Tigre", 3: "San Fernando", 4: "Tigre",
    5: "Tres de Febrero", 6: "San Miguel", 7: "San Miguel", 8: "Moreno",
    9: "Moreno", 10: "Hurlingham", 11: "Moreno", 12: "Gral Las Heras",
    13: "Gral Rodriguez", 14: "Moreno", 15: "Lujan",
}

REPOSITORIES_MAP = {
    'psycopg2': repositories_postgres,
    'pyodbc': repositories_sqlserver
}

G_SENSOR_CACHE = {}


def get_repo_for_db(db_key):
    """
    Devuelve el módulo de repositorio correcto (postgres o sqlserver)
    basado en la clave de la base de datos.
    """
    db_config = config.DATABASE_CONNECTIONS.get(db_key)
    if not db_config:
        raise ValueError(f"No se encontró config para la DB: {db_key}")
        
    driver = db_config.get('driver')
    repo = REPOSITORIES_MAP.get(driver)
    
    if not repo:
        raise ValueError(f"Driver no soportado: {driver}")
        
    return repo


def generate_report_service(db_key, form_data):
    """
    Servicio para generar el reporte.
    """
    try:
        repo = get_repo_for_db(db_key)
        df = repo.generate_report_repo(
            db_key=db_key, 
            ema_id_form=form_data.get('ema_id'),
            fecha_inicio_str=form_data.get('fecha_inicio'),
            fecha_fin_str=form_data.get('fecha_fin'),
            sensor_info_list=form_data.getlist('sensor_info'),
            process_type_list=form_data.getlist('process_type')
        )
        
        # (Solo agregamos el municipio si es la DB principal)
        if 'ema_id' in df.columns and db_key == 'db_principal':
            df['municipio'] = df['ema_id'].map(EMA_MUNICIPIO_MAP).fillna('N/A')
            cols = list(df.columns)
            if 'nombre_ema' in cols:
                municipio_col = cols.pop(cols.index('municipio'))
                nombre_ema_index = cols.index('nombre_ema')
                cols.insert(nombre_ema_index + 1, municipio_col)
                df = df[cols] 

        output_excel = repositories_postgres.create_excel_from_dataframe(df)
        
        fecha_i = form_data.get("fecha_inicio", "inicio")
        fecha_f = form_data.get("fecha_fin", "fin")
        nombre_archivo = f'reporte_EMA_{form_data.get("ema_id")}_{fecha_i}_al_{fecha_f}.xlsx'
        
        return output_excel.getvalue(), nombre_archivo

    except Exception as e:
        print(f"Error en generate_report_service: {e}")
        raise e

def get_sensors_for_ema_service(db_key, ema_id):
    repo = get_repo_for_db(db_key)
    return repo.get_sensors_for_ema_repo(db_key, G_SENSOR_CACHE, ema_id)

def get_ema_list_service(db_key):
    """
    Obtiene la lista de EMAs para mostrar en el desplegable.
    """
    repo = get_repo_for_db(db_key)
    emas_raw_list = repo.get_ema_list_repo(db_key)
    
    emas_display_list = []
    for ema_id, ema_nombre in emas_raw_list: 
        
        # Si la base de datos es la principal, muestra el municipio
        if db_key == 'db_principal':
            municipio = EMA_MUNICIPIO_MAP.get(int(ema_id), "N/A")
            display_text = f"{ema_nombre} ({municipio})"
        else:
            # Si es la base SQL (o cualquier otra), solo muestra el nombre
            display_text = ema_nombre
            
        emas_display_list.append((ema_id, display_text))
        
    return emas_display_list

def get_chart_data_service(db_key, ema_id, sensor_info_list, fecha_inicio, fecha_fin, combine=False):
    
    repo = get_repo_for_db(db_key)

    is_valid_combination = False
    if combine:
        pluvio_sensor = None
        nivel_sensor = None
        for s in sensor_info_list:
            sensor_search_text = s.lower()
            if 'pluvio' in sensor_search_text:
                pluvio_sensor = s
            elif 'limni' in sensor_search_text or 'freati' in sensor_search_text:
                nivel_sensor = s
        if (pluvio_sensor is not None and 
            nivel_sensor is not None and 
            len(sensor_info_list) == 2):
            is_valid_combination = True
            sensor_info_list = [pluvio_sensor, nivel_sensor]
    
    if is_valid_combination:
        print(f"Generando gráfico combinado (Lluvia + Nivel) para {db_key}...")
        dfs_to_merge = []
        datasets_config = []
        
        for sensor_info_str in sensor_info_list:
            sensor_name = sensor_info_str.split('|')[2]
            
            if 'pluvio' in sensor_info_str.lower():
                process_type = 'pluvio_sum' 
                chart_type = 'bar'
                label = f"{sensor_name} (mm)"
            else: 
                process_type = 'nivel_max' 
                chart_type = 'line'
                label = f"{sensor_name} (m)"

            df = repo.generate_report_repo(
                db_key=db_key, 
                ema_id_form=ema_id,
                fecha_inicio_str=fecha_inicio,
                fecha_fin_str=fecha_fin,
                sensor_info_list=[sensor_info_str],
                process_type_list=[process_type]
            )
            
            if not df.empty and 'valor' in df.columns:
                df.rename(columns={'valor': sensor_name}, inplace=True)
                dfs_to_merge.append(df[['dia', sensor_name]])
                datasets_config.append({
                    'name': sensor_name,
                    'type': chart_type,
                    'label': label
                })

        if len(dfs_to_merge) != 2:
             print("Error al recolectar datos para combinar, se devuelven separados.")
             is_valid_combination = False 
        else:
            combined_df = pd.merge(dfs_to_merge[0], dfs_to_merge[1], on='dia', how='outer')
            combined_df.sort_values(by='dia', inplace=True)
            combined_df = combined_df.where(pd.notnull(combined_df), None)
            labels = pd.to_datetime(combined_df['dia']).dt.strftime('%Y-%m-%d').tolist()
            final_datasets = []
            final_scales = {}
            colors = [
                {'bg': 'rgba(54, 162, 235, 0.6)', 'border': 'rgba(54, 162, 235, 1)'}, 
                {'bg': 'rgba(255, 99, 132, 0.6)', 'border': 'rgba(255, 99, 132, 1)'}
            ]
            datasets_config.sort(key=lambda x: x['type'] == 'line') 
            for i, config in enumerate(datasets_config):
                y_axis_id = f'y{i + 1}'
                data = combined_df[config['name']].tolist()
                color = colors[i] 
                final_datasets.append({
                    'label': config['label'], 'data': data, 'type': config['type'], 
                    'yAxisID': y_axis_id, 'backgroundColor': color['bg'],
                    'borderColor': color['border'], 'borderWidth': 2 if config['type'] == 'line' else 1,
                    'fill': False 
                })
                final_scales[y_axis_id] = {
                    'type': 'linear',
                    'position': 'left' if config['type'] == 'bar' else 'right', 
                    'title': { 'display': True, 'text': config['label'] },
                    'grid': { 'drawOnChartArea': (i == 0) } 
                }
            final_chart_type = 'bar' 
            return [{
                'chart_type': final_chart_type, 'labels': labels, 'datasets': final_datasets,
                'options': { 
                    'responsive': True, 'maintainAspectRatio': False, 'scales': final_scales, 
                    'plugins': {
                        'tooltip': { 'mode': 'index', 'intersect': False },
                        'title': { 'display': True, 'text': f'Gráfico Combinado (EMA: {ema_id})' }
                    }
                }
            }]

    if not is_valid_combination:
        print(f"Generando gráficos separados para {db_key}...")
        all_charts_data = []
        for sensor_info_str in sensor_info_list:
            process_type = 'avg_hourly'
            chart_type = 'line' 
            label_base = 'Promedio por Hora'
            
            search_text = sensor_info_str.lower()
            if 'pluvio' in search_text:
                process_type = 'pluvio_sum' 
                chart_type = 'bar'
                label_base = 'Lluvia Acumulada Diaria (mm)'
            elif 'limni' in search_text or 'freati' in search_text:
                process_type = 'nivel_max' 
                chart_type = 'line'
                label_base = 'Nivel Máximo Diario (m)'
            elif 'anemo' in search_text or 'temp' in search_text:
                process_type = 'avg_hourly' 
                chart_type = 'line'
                label_base = 'Promedio por Hora'
            elif 'bateria' in search_text:
                process_type = 'raw' 
                chart_type = 'line'
                label_base = 'Voltaje Batería (V)'
            elif 'presion' in search_text:
                process_type = 'raw' 
                chart_type = 'line'
                label_base = 'Presión (hPa)'

            try:
                sensor_name = sensor_info_str.split('|')[2]
                label = f"{sensor_name} - {label_base}"
            except:
                label = label_base
            
            df = repo.generate_report_repo(
                db_key=db_key, 
                ema_id_form=ema_id,
                fecha_inicio_str=fecha_inicio,
                fecha_fin_str=fecha_fin,
                sensor_info_list=[sensor_info_str],
                process_type_list=[process_type]
            )
            labels = []
            data = []
            if process_type == 'pluvio_sum' or process_type == 'nivel_max':
                if not df.empty and 'dia' in df.columns:
                    labels = pd.to_datetime(df['dia']).dt.strftime('%Y-%m-%d').tolist()
                    data = df['valor'].tolist()
            else: 
                if not df.empty and ('hora' in df.columns and df['hora'].notnull().any()):
                    labels = pd.to_datetime(df['hora']).dt.strftime('%Y-%m-%d %H:%M').tolist()
                    data = df['valor'].tolist()
                elif not df.empty and 'tiempo_de_medicion' in df.columns:
                     labels = pd.to_datetime(df['tiempo_de_medicion']).dt.strftime('%Y-%m-%d %H:%M').tolist()
                     data = df['valor'].tolist()

            bg_color = 'rgba(54, 162, 235, 0.6)' if chart_type == 'bar' else 'rgba(255, 99, 132, 0.6)'
            border_color = 'rgba(54, 162, 235, 1)' if chart_type == 'bar' else 'rgba(255, 99, 132, 1)'
            all_charts_data.append({
                'chart_type': chart_type, 'labels': labels,
                'datasets': [{'label': label, 'data': data,
                    'backgroundColor': bg_color, 'borderColor': border_color, 'borderWidth': 1
                }]
            })
        return all_charts_data


def get_ema_locations_service(db_key):
    """
    Servicio para buscar las locaciones de las EMAs.
    """
    repo = get_repo_for_db(db_key)
    return repo.get_ema_locations_repo(db_key)

def get_ema_live_summary_service(db_key, ema_id):
    """
    Servicio para buscar los datos frescos de una EMA (Popup).
    """
    repo = get_repo_for_db(db_key)
    raw_data = repo.get_ema_live_summary_repo(db_key, ema_id)
    
    formatted_data = {}
    
    if raw_data.get('pluvio_sum_hoy') is not None:
        formatted_data['pluvio_sum_hoy'] = f"{round(raw_data['pluvio_sum_hoy'], 1)} mm (hoy)"
    
    if raw_data.get('temperatura') is not None:
        formatted_data['temperatura'] = f"{round(raw_data['temperatura'], 1)} °C"
    if raw_data.get('nivel_max_hoy') is not None:
        formatted_data['nivel_max_hoy'] = f"{round(raw_data['nivel_max_hoy'], 2)} m"
        
    if raw_data.get('bateria') is not None:
        formatted_data['bateria'] = f"{round(raw_data['bateria'], 2)} V"
    if raw_data.get('presion') is not None:
        formatted_data['presion'] = f"{round(raw_data['presion'], 1)} hPa"

    return formatted_data

def get_dashboard_data_service(db_key, ema_id):
    """
    Servicio para buscar los datos frescos del Dashboard.
    """
    repo = get_repo_for_db(db_key)
    raw_data = repo.get_dashboard_data_repo(db_key, ema_id)
    
    formatted_data = {}
    
    def format_timestamp(ts):
        if isinstance(ts, datetime):
            return ts.strftime('%H:%M hs')
        return 'N/A'

    if raw_data.get('pluvio_sum_hoy'):
        valor = raw_data['pluvio_sum_hoy']['valor'] 
        formatted_data['pluvio_sum_hoy'] = {
            # --- ¡CORRECCIÓN! ---
            'valor_str': f"{round(valor, 1)} mm", 
            'valor_num': round(valor, 1),
            'timestamp': 'Acum. de hoy'
        }

    if raw_data.get('temperatura'):
        valor = raw_data['temperatura']['valor'] 
        formatted_data['temperatura'] = {
            'valor_str': f"{round(valor, 1)} °C", 
            'valor_num': round(valor, 1),
            'timestamp': format_timestamp(raw_data['temperatura']['timestamp'])
        }
    if raw_data.get('nivel_max_hoy'):
        valor = raw_data['nivel_max_hoy']['valor'] 
        formatted_data['nivel_max_hoy'] = {
            # --- ¡CORRECCIÓN! ---
            'valor_str': f"{round(valor, 2)} m", 
            'valor_num': round(valor, 2),
            'timestamp': 'Máx. de hoy'
        }
    if raw_data.get('viento_vel') and raw_data.get('viento_dir'):
        vel = raw_data['viento_vel']['valor'] 
        dir = raw_data['viento_dir']['valor'] 
        formatted_data['viento'] = {
            'vel_str': f"{round(vel, 1)} km/h",
            'dir_str': f"{round(dir)}°", 
            'dir_num': round(dir),
            'timestamp': format_timestamp(raw_data['viento_vel']['timestamp'])
        }

    if raw_data.get('bateria'):
        valor = raw_data['bateria']['valor'] 
        formatted_data['bateria'] = {
            'valor_str': f"{round(valor, 2)} V", 
            'valor_num': round(valor, 2),
            'timestamp': format_timestamp(raw_data['bateria']['timestamp'])
        }
    if raw_data.get('presion'):
        valor = raw_data['presion']['valor'] 
        formatted_data['presion'] = {
            'valor_str': f"{round(valor, 1)} hPa", 
            'valor_num': round(valor, 1),
            'timestamp': format_timestamp(raw_data['presion']['timestamp'])
        }

    return formatted_data


def build_global_cache():
    """
    Construye un caché global de sensores para CADA base de datos.
    """
    print("Inicializando caché global (build_global_cache)...")
    global G_SENSOR_CACHE
    G_SENSOR_CACHE = {}  # Reinicia el caché en memoria

    for db_key, db_conf in config.DATABASE_CONNECTIONS.items():
        try:
            repo = get_repo_for_db(db_key)
            sensor_cache_para_db = repo.build_active_sensor_cache(db_key)
            G_SENSOR_CACHE[db_key] = sensor_cache_para_db
            print(f"✅ Caché de sensores cargado para {db_key} ({len(sensor_cache_para_db)} EMAs con sensores)")
        
        except Exception as e:
            print(f"⚠️ Error al cargar caché de sensores para {db_key}: {e}")
            G_SENSOR_CACHE[db_key] = {} 
            
    return G_SENSOR_CACHE