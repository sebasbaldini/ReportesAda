import pandas as pd
from . import repositories
from flask_login import current_user

# === LISTAS ===
def get_unified_ema_list_service():
    stations = repositories.get_all_stations_repo()
    lista = []
    for s in stations:
        if s.id_proyecto:
            nombre_display = f"{s.ubicacion} ({s.proyecto})" if s.ubicacion else s.proyecto
            lista.append((s.id_proyecto, nombre_display))
    return lista

def get_all_unified_locations_service():
    """Genera los datos para el MAPA con metadatos para filtros"""
    stations = repositories.get_all_stations_repo()
    result = []
    for s in stations:
        if s.latitude and s.longitude:
            # Detectar tipo de red (COMIREC, SIMATH, etc) basado en el proyecto
            tipo_red = "OTRO"
            proyecto_str = (s.proyecto or "").upper()
            if "COMIREC" in proyecto_str: tipo_red = "COMIREC"
            elif "SIMATH" in proyecto_str or "SIMPARH" in proyecto_str: tipo_red = "SIMATH"
            
            result.append({
                'id': s.id_proyecto,
                'nombre': s.ubicacion or s.id_proyecto, 
                'descripcion': f"Cuenca: {s.cuenca} | Partido: {s.partido}",
                'lat': s.latitude,
                'lon': s.longitude,
                'source_db': 'postgres',
                
                # --- DATOS NUEVOS PARA FILTROS ---
                'partido': s.partido or "Sin Definir",
                'cuenca': s.cuenca or "Sin Definir",
                'red': tipo_red # COMIREC o SIMATH
            })
    return result

# ... (El resto de funciones get_sensors, dashboard y charts quedan IGUAL que antes) ...
# (Copia el resto del archivo services.py que ya tenías funcionando)
def get_sensors_for_ema_service(dummy_db, ema_id):
    sensors = repositories.get_sensors_for_station_repo(ema_id)
    if current_user.is_authenticated and getattr(current_user, 'role', 'admin') == 'restricted':
        sensors = [s for s in sensors if 'bateria' not in s['search_text']]
    return sensors

def get_dashboard_data_service(dummy_db, ema_id):
    return repositories.get_dashboard_data_repo(ema_id)

def generate_report_service(dummy_db, form_data):
    ema_id = form_data.get('ema_id')
    f_inicio = form_data.get('fecha_inicio')
    f_fin = form_data.get('fecha_fin')
    sensors = form_data.getlist('sensor_info')
    processes = form_data.getlist('process_type')
    dfs = []
    for s_info, proc in zip(sensors, processes):
        parts = s_info.split('|')
        if len(parts) < 2: continue
        metrica_db = parts[1]
        nombre_sensor = parts[2] if len(parts) > 2 else metrica_db
        df = repositories.generate_chart_report_data(ema_id, f_inicio, f_fin, metrica_db, proc)
        if not df.empty:
            df['Sensor'] = nombre_sensor
            dfs.append(df)
    if not dfs: raise Exception("No se encontraron datos para el rango seleccionado.")
    final_df = pd.concat(dfs, ignore_index=True)
    excel_file = repositories.create_excel_from_dataframe(final_df)
    return excel_file.getvalue(), f"Reporte_{ema_id}.xlsx"

def get_chart_data_service(dummy_db, ema_id, sensor_info_list, f_inicio, f_fin, combine=False):
    result_charts = [] 
    if combine:
        datasets = []
        labels = set()
        scales_config = {
            'x': { 'title': { 'display': True, 'text': 'Fecha' } },
            'y-nivel': { 'type': 'linear', 'display': True, 'position': 'left', 'title': { 'display': True, 'text': 'Nivel (m)' }, 'grid': { 'drawOnChartArea': False } },
            'y-lluvia': { 'type': 'linear', 'display': True, 'position': 'right', 'title': { 'display': True, 'text': 'Lluvia (mm)' }, 'grid': { 'drawOnChartArea': False } }
        }
        for s_info in sensor_info_list:
            ds, fe = _process_single_sensor(s_info, ema_id, f_inicio, f_fin, is_combined=True)
            if ds:
                datasets.append(ds)
                if fe: labels.update(fe)
        if datasets:
            result_charts.append({
                'chart_type': 'bar',
                'labels': sorted(list(labels)),
                'datasets': datasets,
                'options': { 'responsive': True, 'maintainAspectRatio': False, 'interaction': { 'mode': 'index', 'intersect': False }, 'scales': scales_config, 'plugins': { 'title': { 'display': True, 'text': 'Gráfico Combinado' } } }
            })
    else:
        for s_info in sensor_info_list:
            ds, fe = _process_single_sensor(s_info, ema_id, f_inicio, f_fin, is_combined=False)
            if ds:
                parts = s_info.split('|')
                nombre = parts[2] if len(parts) > 2 else "Sensor"
                scales_config = { 'x': { 'title': { 'display': True, 'text': 'Fecha' } }, 'y': { 'title': { 'display': True, 'text': 'Valor' }, 'position': 'left' } }
                result_charts.append({
                    'chart_type': ds['type'],
                    'labels': sorted(list(set(fe))),
                    'datasets': [ds],
                    'options': { 'responsive': True, 'maintainAspectRatio': False, 'scales': scales_config, 'plugins': { 'title': { 'display': True, 'text': nombre } } }
                })
    return result_charts

def _process_single_sensor(s_info, ema_id, f_inicio, f_fin, is_combined):
    parts = s_info.split('|')
    if len(parts) < 2: return None, []
    metrica_db = parts[1]
    nombre = parts[2] if len(parts) > 2 else metrica_db
    proc = 'raw'; tipo_grafico = 'line'; color = 'cyan'; y_axis_id = 'y'
    if 'Pluvio' in metrica_db: 
        proc = 'pluvio_sum'; tipo_grafico = 'bar'; color = 'blue'
        if is_combined: y_axis_id = 'y-lluvia'
    elif 'Limni' in metrica_db or 'Freat' in metrica_db: 
        proc = 'raw'; tipo_grafico = 'line'; color = 'navy'
        if is_combined: y_axis_id = 'y-nivel'
    elif 'Anemo' in metrica_db: color = 'green'
    elif 'Temp' in metrica_db: color = 'red'
    elif 'Baro' in metrica_db: color = 'purple'
    if is_combined and not ('Pluvio' in metrica_db or 'Limni' in metrica_db or 'Freat' in metrica_db):
         y_axis_id = 'y-nivel' 
    df = repositories.generate_chart_report_data(ema_id, f_inicio, f_fin, metrica_db, proc)
    if df.empty: return None, []
    col_t = 'fecha'
    if 'dia' in df.columns: col_t = 'dia'
    elif 'hora' in df.columns: col_t = 'hora'
    dataset = {
        'label': nombre, 'data': df['valor'].tolist(), 'type': tipo_grafico,
        'borderColor': color, 'backgroundColor': color, 'borderWidth': 2
    }
    if is_combined:
        dataset['yAxisID'] = y_axis_id
        dataset['order'] = 0 if tipo_grafico == 'line' else 1 
    return dataset, df[col_t].astype(str).tolist()
def get_all_unified_locations_service():
    stations = repositories.get_all_stations_repo()
    
    # 1. Obtenemos el set de estaciones que reportaron hoy (Rápido)
    active_ids = repositories.get_active_stations_ids_today()
    
    result = []
    for s in stations:
        if s.latitude and s.longitude:
            tipo_red = "OTRO"
            proyecto_str = (s.proyecto or "").upper()
            if "COMIREC" in proyecto_str: tipo_red = "COMIREC"
            elif "SIMATH" in proyecto_str or "SIMPARH" in proyecto_str: tipo_red = "SIMATH"
            
            # 2. Determinamos el estado (Verde/Rojo)
            # Si el ID está en el set de activas, está ONLINE
            estado = 'online' if s.id_proyecto in active_ids else 'offline'

            result.append({
                'id': s.id_proyecto,
                'nombre': s.ubicacion or s.id_proyecto, 
                'descripcion': f"Cuenca: {s.cuenca} | Partido: {s.partido}",
                'lat': s.latitude,
                'lon': s.longitude,
                'source_db': 'postgres',
                'partido': s.partido or "Sin Definir",
                'cuenca': s.cuenca or "Sin Definir",
                'red': tipo_red,
                
                # ¡Nuevo campo para el mapa!
                'status': estado 
            })
    return result