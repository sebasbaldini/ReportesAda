import pandas as pd
from . import repositories
from flask_login import current_user

# ==========================================
# 1. SERVICIOS DE EMAS (Estaciones Automáticas)
# ==========================================

def get_unified_ema_list_service():
    stations = repositories.get_all_stations_repo()
    lista = []
    for s in stations:
        if s.id_proyecto:
            nombre_display = f"{s.ubicacion} ({s.proyecto})" if s.ubicacion else s.proyecto
            lista.append((s.id_proyecto, nombre_display))
    return lista

# --- MODIFICAR EN services.py ---

def get_all_unified_locations_service():
    stations = repositories.get_all_stations_repo()
    active_ids = repositories.get_active_stations_ids_today()
    
    # 1. Obtenemos las últimas fechas disponibles
    last_aforos = repositories.get_latest_aforo_dates_repo()
    last_escalas = repositories.get_latest_escala_dates_repo()

    result = []
    for s in stations:
        if s.latitude and s.longitude:
            tipo_red = "OTRO"
            proyecto_str = (s.proyecto or "").upper()
            if "COMIREC" in proyecto_str: tipo_red = "COMIREC"
            elif "SIMATH" in proyecto_str or "SIMPARH" in proyecto_str: tipo_red = "SIMATH"
            
            # Estado Online/Offline (solo relevante para EMAs)
            estado = 'online' if s.id_proyecto in active_ids else 'offline'
            
            # 2. Detectamos qué datos tiene esta estación
            tiene_ema = bool(s.id_proyecto)
            fecha_aforo = last_aforos.get(s.id)
            fecha_escala = last_escalas.get(s.id)

            # Formateamos las fechas para el frontend
            str_aforo = fecha_aforo.strftime('%d/%m/%Y') if fecha_aforo else None
            str_escala = fecha_escala.strftime('%d/%m/%Y') if fecha_escala else None

            result.append({
                'id': s.id_proyecto if s.id_proyecto else s.id, # ID principal
                'db_id': s.id, # ID de base de datos (para aforos/escalas)
                'nombre': s.ubicacion or s.id_proyecto, 
                'descripcion': f"Cuenca: {s.nomcuenca} | Partido: {s.pdo}",
                'lat': s.latitude, 'lon': s.longitude, 'source_db': 'postgres',
                'partido': s.pdo or "Sin Definir", 'cuenca': s.nomcuenca or "Sin Definir",
                'red': tipo_red, 
                'status': estado,
                # 3. Campos nuevos para la lógica del Popup
                'is_ema': tiene_ema,
                'last_aforo': str_aforo,
                'last_escala': str_escala
            })
    return result

def get_sensors_for_ema_service(dummy_db, ema_id):
    sensors = repositories.get_sensors_for_station_repo(ema_id)
    if current_user.is_authenticated and getattr(current_user, 'role', 'admin') == 'restricted':
        sensors = [s for s in sensors if 'bateria' not in s['search_text']]
    return sensors

def get_dashboard_data_service(dummy_db, ema_id):
    return repositories.get_dashboard_data_repo(ema_id)

def get_map_popup_status_service(dummy_db, ema_id):
    return repositories.get_map_popup_status_repo(ema_id)

def generate_report_service(dummy_db, form_data):
    """Genera reporte de EMAs (Sensores)"""
    # 1. Obtener lista de IDs seleccionados
    raw_ids = form_data.getlist('ema_id')
    if not raw_ids:
        single = form_data.get('ema_id')
        if single: raw_ids = [single]
    
    all_stations_objs = repositories.get_all_stations_repo()
    station_map = {s.id_proyecto: (s.ubicacion or s.id_proyecto) for s in all_stations_objs}

    target_ids = []
    if not raw_ids or 'todas' in raw_ids or (len(raw_ids) == 1 and raw_ids[0] == 'todas'):
        target_ids = list(station_map.keys())
    else:
        target_ids = raw_ids

    f_inicio = form_data.get('fecha_inicio')
    f_fin = form_data.get('fecha_fin')
    sensors = form_data.getlist('sensor_info')
    processes = form_data.getlist('process_type')

    dfs = []
    for ema_id in target_ids:
        nombre_estacion = station_map.get(ema_id, ema_id)
        for s_info, proc in zip(sensors, processes):
            parts = s_info.split('|')
            if len(parts) < 2: continue
            metrica_db = parts[1]
            nombre_sensor = parts[2] if len(parts) > 2 else metrica_db
            try:
                df = repositories.generate_chart_report_data(ema_id, f_inicio, f_fin, metrica_db, proc)
                if not df.empty:
                    df['Estación'] = nombre_estacion
                    df['Sensor'] = nombre_sensor
                    cols = ['Estación', 'Sensor'] + [c for c in df.columns if c not in ['Estación', 'Sensor']]
                    df = df[cols]
                    dfs.append(df)
            except Exception:
                continue

    if not dfs: raise Exception("No se encontraron datos para el rango y estaciones seleccionadas.")
    final_df = pd.concat(dfs, ignore_index=True)
    excel_file = repositories.create_excel_from_dataframe(final_df)
    filename_str = f"Reporte_{target_ids[0]}.xlsx" if len(target_ids) == 1 else "Reporte_MultiEstacion.xlsx"
    return excel_file.getvalue(), filename_str

def get_chart_data_service(dummy_db, ema_id, sensor_info_list, f_inicio, f_fin, combine=False):
    es_un_dia = (f_inicio == f_fin)
    result_charts = [] 

    if combine:
        scales_config = {
            'x': { 'title': { 'display': True, 'text': 'Hora' if es_un_dia else 'Fecha' } },
            'y-nivel': { 
                'type': 'linear', 'display': True, 'position': 'left', 
                'title': { 'display': True, 'text': 'Nivel (m)' },
                'grid': { 'drawOnChartArea': False }
            },
            'y-lluvia': { 
                'type': 'linear', 'display': True, 'position': 'right', 
                'title': { 'display': True, 'text': 'Lluvia (mm)' },
                'grid': { 'drawOnChartArea': False } 
            }
        }
        
        raw_results = []
        all_labels = set()
        
        for s_info in sensor_info_list:
            ds_list, fe = _process_single_sensor(s_info, ema_id, f_inicio, f_fin, is_combined=True, single_day_mode=es_un_dia)
            if ds_list:
                raw_results.append((ds_list, fe))
                all_labels.update(fe)
        
        if not raw_results: return []

        master_labels = sorted(list(all_labels))
        final_datasets = []

        for ds_list, original_labels in raw_results:
            for ds in ds_list:
                current_map = dict(zip(original_labels, ds['data']))
                aligned_data = [current_map.get(lbl, None) for lbl in master_labels]
                ds['data'] = aligned_data
                if ds['type'] == 'line': ds['spanGaps'] = True 
                final_datasets.append(ds)

        final_datasets.sort(key=lambda x: 0 if x['type'] == 'bar' else 1)

        result_charts.append({
            'chart_type': 'bar',
            'labels': master_labels,
            'datasets': final_datasets,
            'options': {
                'responsive': True,
                'maintainAspectRatio': False,
                'interaction': { 'mode': 'index', 'intersect': False },
                'scales': scales_config,
                'plugins': { 'title': { 'display': True, 'text': 'Gráfico Combinado' } }
            }
        })

    else:
        for s_info in sensor_info_list:
            ds_list, fe = _process_single_sensor(s_info, ema_id, f_inicio, f_fin, is_combined=False, single_day_mode=es_un_dia)
            if ds_list:
                parts = s_info.split('|')
                nombre = parts[2] if len(parts) > 2 else "Sensor"
                base_type = ds_list[0]['type']
                scales_config = { 'x': { 'title': { 'display': True, 'text': 'Hora' if es_un_dia else 'Fecha' } }, 'y': { 'title': { 'display': True, 'text': 'Valor' }, 'position': 'left' } }
                
                result_charts.append({
                    'chart_type': base_type,
                    'labels': sorted(list(set(fe))),
                    'datasets': ds_list,
                    'options': { 'responsive': True, 'maintainAspectRatio': False, 'scales': scales_config, 'plugins': { 'title': { 'display': True, 'text': nombre } } }
                })

    return result_charts

def _process_single_sensor(s_info, ema_id, f_inicio, f_fin, is_combined, single_day_mode):
    parts = s_info.split('|')
    if len(parts) < 2: return [], []
    metrica_db = parts[1]
    nombre_base = parts[2] if len(parts) > 2 else metrica_db
    m_upper = metrica_db.upper()

    if single_day_mode:
        proc = 'raw'
    else:
        if 'PLUVIO' in m_upper: proc = 'daily_sum'
        elif 'BATERIA' in m_upper: proc = 'daily_avg' 
        elif any(x in m_upper for x in ['LIMNI', 'FREAT', 'PH', 'CONDUCT', 'TEMP AGUA']): proc = 'daily_min_max'
        else: proc = 'daily_max'

    tipo_grafico = 'line'; color = 'cyan'; y_axis_id = 'y'
    if 'PLUVIO' in m_upper: 
        tipo_grafico = 'bar'; color = 'blue'
        if is_combined: y_axis_id = 'y-lluvia'
    elif 'LIMNI' in m_upper or 'FREAT' in m_upper: 
        tipo_grafico = 'line'; color = 'navy'
        if is_combined: y_axis_id = 'y-nivel'
    elif 'ANEMO' in m_upper: color = 'green'
    elif 'TEMP' in m_upper: color = 'red'
    elif 'BARO' in m_upper: color = 'purple'

    if is_combined and not ('PLUVIO' in m_upper or 'LIMNI' in m_upper or 'FREAT' in m_upper):
         y_axis_id = 'y-nivel'

    df = repositories.generate_chart_report_data(ema_id, f_inicio, f_fin, metrica_db, proc)
    if df.empty: return [], []

    col_t = 'fecha'
    if 'dia' in df.columns: col_t = 'dia'
    elif 'hora' in df.columns: col_t = 'hora'
    
    fechas_str = df[col_t].astype(str).tolist()
    datasets = []

    if 'valor_max' in df.columns and 'valor_min' in df.columns:
        datasets.append({
            'label': f"{nombre_base} (Máx)", 'data': df['valor_max'].tolist(), 
            'type': tipo_grafico, 'borderColor': color, 'backgroundColor': color, 'borderWidth': 2, 'tension': 0.3,
            'yAxisID': y_axis_id if is_combined else 'y', 'order': 0, 'spanGaps': True
        })
        datasets.append({
            'label': f"{nombre_base} (Mín)", 'data': df['valor_min'].tolist(), 
            'type': tipo_grafico, 'borderColor': '#6c757d', 'backgroundColor': '#6c757d', 'borderDash': [5, 5], 'borderWidth': 2, 'tension': 0.3,
            'yAxisID': y_axis_id if is_combined else 'y', 'order': 0, 'spanGaps': True
        })
    else:
        label_suffix = ""
        if proc == 'daily_max': label_suffix = " (Máx Diario)"
        elif proc == 'daily_sum': label_suffix = " (Acumulado)"
        elif proc == 'daily_avg': label_suffix = " (Promedio)"
        
        datasets.append({
            'label': f"{nombre_base}{label_suffix}", 'data': df['valor'].tolist(), 
            'type': tipo_grafico, 'borderColor': color, 'backgroundColor': color, 'borderWidth': 2,
            'yAxisID': y_axis_id if is_combined else 'y',
            'order': 1 if tipo_grafico == 'bar' else 0, 'spanGaps': True
        })

    return datasets, fechas_str

# ==========================================
# 2. SERVICIOS DE AFOROS (RECUPERADOS)
# ==========================================

def get_aforos_active_list_service():
    """Obtiene la lista de estaciones que tienen datos de aforos."""
    stations = repositories.get_stations_with_aforos_repo()
    lista = []
    for s in stations:
        display = f"{s.ubicacion} ({s.id})" if s.ubicacion else s.id
        lista.append((s.id, display))
    return lista

def generate_aforo_report_service(form_data):
    """Genera el Excel de Aforos."""
    station_id = form_data.get('ema_id')
    f_inicio = form_data.get('fecha_inicio')
    f_fin = form_data.get('fecha_fin')

    if not station_id:
        raise Exception("Debe seleccionar una estación.")

    # Llamamos al repo, pasando fechas (pueden ser None si están vacías)
    df = repositories.get_aforos_data_repo(station_id, f_inicio, f_fin)

    if df.empty:
        raise Exception("No se encontraron datos de aforos para los filtros seleccionados.")

    output = repositories.create_excel_simple(df)
    return output.getvalue(), f"Reporte_Aforos_{station_id}.xlsx"

# ==========================================
# 3. SERVICIOS DE ESCALAS (NUEVOS)
# ==========================================

def get_escalas_active_list_service():
    """Obtiene la lista de estaciones que tienen datos de escalas."""
    stations = repositories.get_stations_with_escalas_repo()
    lista = []
    for s in stations:
        display = f"{s.ubicacion} ({s.id})" if s.ubicacion else s.id
        lista.append((s.id, display))
    return lista

def generate_escala_report_service(form_data):
    """Genera el Excel de Escalas."""
    station_id = form_data.get('ema_id')
    f_inicio = form_data.get('fecha_inicio')
    f_fin = form_data.get('fecha_fin')

    if not station_id:
        raise Exception("Debe seleccionar una estación.")

    df = repositories.get_escalas_data_repo(station_id, f_inicio, f_fin)

    if df.empty:
        raise Exception("No se encontraron datos de escalas para los filtros seleccionados.")

    output = repositories.create_excel_simple(df)
    return output.getvalue(), f"Reporte_Escalas_{station_id}.xlsx"