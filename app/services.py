import pandas as pd
import math
from . import repositories
from flask_login import current_user
from datetime import datetime

# ==========================================
# 1. SERVICIOS DE MAPA Y UBICACIONES
# ==========================================

def get_unified_ema_list_service():
    """Usado para los selectores de Dashboard, Gráficos y Reportes de EMAs"""
    stations = repositories.get_all_stations_repo()
    lista = []
    for s in stations:
        if s.id_proyecto:
            # Agregamos el ID antes del nombre
            nombre_base = s.ubicacion if s.ubicacion else s.proyecto
            display = f"ID: {s.id} - {nombre_base}"
            lista.append((s.id_proyecto, display))
    return lista

def get_all_unified_locations_service():
    """Lógica para los puntos del mapa"""
    stations = repositories.get_stations_for_map_repo()
    active_ids = repositories.get_active_stations_ids_today()
    last_aforos = repositories.get_latest_aforo_dates_repo()
    last_escalas = repositories.get_latest_escala_dates_repo()
    last_mp = repositories.get_latest_mp_data_repo() 

    result = []
    for s in stations:
        if s.latitude and s.longitude:
            id_db = str(s.id).strip()
            mp_valor = None
            str_mp_fecha = None
            mp_data = last_mp.get(id_db)
            if not mp_data and s.id_proyecto: mp_data = last_mp.get(str(s.id_proyecto).strip())

            if mp_data:
                raw_val = mp_data['valor']
                try:
                    if raw_val is None or (isinstance(raw_val, float) and math.isnan(raw_val)): mp_valor = None
                    else: mp_valor = float(raw_val)
                except: mp_valor = None
                if mp_data['fecha']: str_mp_fecha = mp_data['fecha'].strftime('%d/%m/%Y %H:%M')
            
            tipo_red = "OTRO"
            p_str = (s.proyecto or "").upper()
            if "COMIREC" in p_str: tipo_red = "COMIREC"
            elif "SIMATH" in p_str or "SIMPARH" in p_str: tipo_red = "SIMATH"
            
            s_id_proj = str(s.id_proyecto).strip().upper() if s.id_proyecto else ""
            estado = 'online' if s_id_proj in active_ids else 'offline'
            tiene_ema = bool(s.id_proyecto)
            
            fa = last_aforos.get(id_db)
            str_aforo = fa.strftime('%d/%m/%Y') if fa else None
            
            fe = last_escalas.get(id_db)
            str_escala = fe.strftime('%d/%m/%Y') if fe else None

            result.append({
                'id': s.id_proyecto if s.id_proyecto else s.id, 
                'db_id': s.id, 
                'nombre': s.ubicacion or s.id_proyecto or "Estación sin nombre", 
                'descripcion': f"Cuenca: {s.nomcuenca or '-'} | Partido: {s.pdo or '-'}",
                'lat': s.latitude, 'lon': s.longitude, 
                'partido': s.pdo or "Sin Definir", 'cuenca': s.nomcuenca or "Sin Definir",
                'red': tipo_red, 'status': estado, 'is_ema': tiene_ema,
                'last_aforo': str_aforo, 'last_escala': str_escala,
                'last_mp_valor': mp_valor, 'last_mp_fecha': str_mp_fecha
            })
    return result

# ==========================================
# 2. REPORTES (Listas con ID)
# ==========================================

def get_lecturas_active_list_service():
    """Lista para Reporte de Monitoreo Participativo"""
    stations = repositories.get_stations_with_lecturas_repo()
    return [(s.id, f"ID: {s.id} - {s.ubicacion}") for s in stations]

def get_aforos_active_list_service():
    """Lista para Reporte de Aforos"""
    stations = repositories.get_stations_with_aforos_repo()
    return [(s.id, f"ID: {s.id} - {s.ubicacion}") for s in stations]

def get_escalas_active_list_service():
    """Lista para Reporte de Escalas"""
    stations = repositories.get_stations_with_escalas_repo()
    return [(s.id, f"ID: {s.id} - {s.ubicacion}") for s in stations]

def generate_lecturas_report_service(form_data):
    sid = form_data.get('ema_id'); fi = form_data.get('fecha_inicio'); ff = form_data.get('fecha_fin')
    if not sid: raise Exception("Seleccione estación")
    df = repositories.get_lecturas_data_repo(sid, fi, ff)
    if df.empty: raise Exception("Sin datos")
    out = repositories.create_excel_mp(df)
    return out.getvalue(), f"Reporte_Lecturas_{sid}.xlsx"

def generate_aforo_report_service(form_data):
    sid = form_data.get('ema_id'); fi = form_data.get('fecha_inicio'); ff = form_data.get('fecha_fin')
    if not sid: raise Exception("Seleccione estación")
    df = repositories.get_aforos_data_repo(sid, fi, ff)
    if df.empty: raise Exception("Sin datos")
    out = repositories.create_excel_simple(df)
    return out.getvalue(), f"Reporte_Aforos_{sid}.xlsx"

def generate_escala_report_service(form_data):
    sid = form_data.get('ema_id'); fi = form_data.get('fecha_inicio'); ff = form_data.get('fecha_fin')
    if not sid: raise Exception("Seleccione estación")
    df = repositories.get_escalas_data_repo(sid, fi, ff)
    if df.empty: raise Exception("Sin datos")
    out = repositories.create_excel_simple(df)
    return out.getvalue(), f"Reporte_Escalas_{sid}.xlsx"

# ==========================================
# 3. GRAFICOS
# ==========================================

def get_sensors_for_ema_service(db, ema_id):
    sensors = repositories.get_sensors_for_station_repo(ema_id)
    if current_user.is_authenticated and getattr(current_user, 'role', 'admin') == 'restricted':
        sensors = [s for s in sensors if 'bateria' not in s['search_text']]
    return sensors

def get_dashboard_data_service(db, ema_id): return repositories.get_dashboard_data_repo(ema_id)
def get_map_popup_status_service(db, ema_id): return repositories.get_map_popup_status_repo(ema_id)

def generate_chart_excel_service(user, ema_id, s_list, fi, ff):
    charts_data = get_chart_data_service(user, ema_id, s_list, fi, ff, combine=False)
    if not charts_data: return None, "sin_datos.xlsx"
    all_stations = repositories.get_all_stations_repo()
    station_obj = next((s for s in all_stations if s.id_proyecto == ema_id), None)
    # Título del Excel con ID
    ema_nombre = f"ID: {station_obj.id} - {station_obj.ubicacion}" if station_obj else ema_id
    output = io.BytesIO()
    try:
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            workbook = writer.book
            title_fmt = workbook.add_format({'bold': True, 'font_size': 14, 'color': '#0d6efd'})
            for idx, chart_obj in enumerate(charts_data):
                labels = chart_obj['labels']; datasets = chart_obj['datasets']
                data_dict = {'Fecha': labels}
                for ds in datasets:
                    clean_label = ds['label'].replace('[', '(').replace(']', ')')
                    data_dict[clean_label] = ds['data']
                df_sheet = pd.DataFrame(data_dict)
                sheet_name = f"Grafico_{idx+1}"
                df_sheet.to_excel(writer, sheet_name=sheet_name, startrow=7, index=False)
                worksheet = writer.sheets[sheet_name]
                worksheet.write(0, 0, f"Reporte de Estación: {ema_nombre}", title_fmt)
                
                try:
                    chart_title_text = "Gráfico"
                    if 'plugins' in chart_obj['options'] and 'title' in chart_obj['options']['plugins']:
                        chart_title_text = chart_obj['options']['plugins']['title']['text']
                    elif 'title' in chart_obj['options']:
                        chart_title_text = chart_obj['options']['title']['text']

                    c_type = 'column' if chart_obj['chart_type'] == 'bar' else 'line'
                    excel_chart = workbook.add_chart({'type': c_type})
                    num_rows = len(df_sheet)
                    header_row = 7; data_start_row = 8; data_end_row = 7 + num_rows
                    for col_num, col_name in enumerate(df_sheet.columns):
                        if col_name == 'Fecha': continue
                        excel_chart.add_series({
                            'name': [sheet_name, header_row, col_num],
                            'categories': [sheet_name, data_start_row, 0, data_end_row, 0],
                            'values': [sheet_name, data_start_row, col_num, data_end_row, col_num],
                        })
                    excel_chart.set_title({'name': chart_title_text})
                    excel_chart.set_size({'width': 800, 'height': 450})
                    worksheet.insert_chart('E2', excel_chart)
                except: pass

    except: return None, "error.xlsx"
    return output, f"Reporte_{ema_id}.xlsx"

def _process_single_sensor(sensor_info_str, ema_id, f_inicio, f_fin, is_combined, single_day_mode):
    try:
        parts = sensor_info_str.split('|'); metric_real = parts[1] if len(parts) > 1 else 'Sensor'; display_name = parts[2] if len(parts) > 2 else metric_real
    except: return [], []
    df = repositories.get_chart_data_repo(ema_id, [sensor_info_str], f_inicio, f_fin)
    if df.empty: return [], []
    if 'fecha' in df.columns: df['fecha'] = pd.to_datetime(df['fecha'])
    df.sort_values('fecha', inplace=True)
    es_nivel = 'Limni' in metric_real or 'Freat' in metric_real or 'Nivel' in display_name
    es_pluvio = 'Pluvio' in metric_real
    delta_days = 0
    try:
        if f_inicio and f_fin: d1 = pd.to_datetime(f_inicio); d2 = pd.to_datetime(f_fin); delta_days = (d2 - d1).days
    except: delta_days = 0
    datasets = []; labels = []
    if delta_days > 0 and not single_day_mode:
        df['fecha_dia'] = df['fecha'].dt.date
        grp_dates = df['fecha_dia'].unique(); grp_dates.sort(); labels = [str(d) for d in grp_dates]
        y_axis = 'y-lluvia' if es_pluvio and is_combined else ('y-nivel' if is_combined else 'y')
        if es_nivel:
            daily_grp = df.groupby('fecha_dia')['valor'].agg(['max', 'min']).reset_index()
            datasets.append({'label': f"{display_name} (Máx)", 'data': daily_grp['max'].tolist(), 'type': 'line', 'borderColor': '#0d6efd', 'yAxisID': y_axis})
            datasets.append({'label': f"{display_name} (Mín)", 'data': daily_grp['min'].tolist(), 'type': 'line', 'borderColor': '#0dcaf0', 'yAxisID': y_axis})
        elif es_pluvio:
            daily_sum = df.groupby('fecha_dia')['valor'].sum().reset_index()
            datasets.append({'label': f"{display_name} (Acumulado)", 'data': daily_sum['valor'].tolist(), 'type': 'bar', 'backgroundColor': '#0d6efd', 'yAxisID': y_axis})
        else:
            daily_avg = df.groupby('fecha_dia')['valor'].mean().reset_index()
            datasets.append({'label': f"{display_name} (Promedio)", 'data': daily_avg['valor'].tolist(), 'type': 'line', 'borderColor': '#6c757d', 'yAxisID': y_axis})
    else:
        labels = df['fecha'].dt.strftime('%Y-%m-%d %H:%M').tolist()
        y_axis = 'y-lluvia' if es_pluvio and is_combined else ('y-nivel' if is_combined else 'y')
        c_type = 'bar' if es_pluvio else 'line'
        datasets.append({'label': display_name, 'data': df['valor'].tolist(), 'type': c_type, 'borderColor': '#0d6efd', 'yAxisID': y_axis})
    return datasets, labels

def get_chart_data_service(dummy_db, ema_id, s_list, fi, ff, combine=False):
    es_un_dia = (fi == ff); result_charts = []
    if combine:
        raw_results = []; all_labels = set()
        for s_info in s_list:
            ds_list, fe = _process_single_sensor(s_info, ema_id, fi, ff, True, es_un_dia)
            if ds_list: raw_results.append((ds_list, fe)); all_labels.update(fe)
        if not raw_results: return []
        master_labels = sorted(list(all_labels)); final_datasets = []
        for ds_list, original_labels in raw_results:
            for ds in ds_list:
                current_map = dict(zip(original_labels, ds['data']))
                ds['data'] = [current_map.get(lbl, None) for lbl in master_labels]
                final_datasets.append(ds)
        
        title_text = 'Gráfico Combinado (Lluvia vs Nivel)'
        result_charts.append({
            'chart_type': 'bar', 
            'labels': master_labels, 
            'datasets': final_datasets, 
            'options': {
                'responsive': True, 
                'scales': {'y-nivel': {'position': 'left'}, 'y-lluvia': {'position': 'right'}},
                'title': { 'display': True, 'text': title_text }, 
                'plugins': { 'title': { 'display': True, 'text': title_text } } 
            }
        })
    else:
        for s_info in s_list:
            ds_list, fe = _process_single_sensor(s_info, ema_id, fi, ff, False, es_un_dia)
            if ds_list: 
                parts = s_info.split('|')
                title_text = parts[2] if len(parts) > 2 else "Sensor"
                result_charts.append({
                    'chart_type': ds_list[0]['type'], 
                    'labels': sorted(list(set(fe))), 
                    'datasets': ds_list, 
                    'options': {
                        'responsive': True,
                        'title': { 'display': True, 'text': title_text },
                        'plugins': { 'title': { 'display': True, 'text': title_text } } 
                    }
                })
    return result_charts

def generate_report_service(dummy_db, form_data):
    raw_ids = form_data.getlist('ema_id')
    if not raw_ids: single = form_data.get('ema_id'); raw_ids = [single] if single else []
    all_stations_objs = repositories.get_all_stations_repo()
    
    # Aquí el mapa de estaciones para el Excel también incluye el ID
    station_map = {s.id_proyecto: f"ID: {s.id} - {s.ubicacion or s.id_proyecto}" for s in all_stations_objs}
    
    target_ids = list(station_map.keys()) if not raw_ids or 'todas' in raw_ids else raw_ids
    fi = form_data.get('fecha_inicio'); ff = form_data.get('fecha_fin')
    sensors = form_data.getlist('sensor_info'); processes = form_data.getlist('process_type')
    dfs = []
    for ema_id in target_ids:
        nombre_estacion = station_map.get(ema_id, ema_id)
        for s_info, proc in zip(sensors, processes):
            parts = s_info.split('|')
            if len(parts) < 2: continue
            try:
                df = repositories.generate_chart_report_data(ema_id, fi, ff, parts[1], proc)
                if not df.empty:
                    df['Estación'] = nombre_estacion; df['Sensor'] = parts[2] if len(parts) > 2 else parts[1]
                    cols = ['Estación', 'Sensor'] + [c for c in df.columns if c not in ['Estación', 'Sensor']]
                    dfs.append(df[cols])
            except: continue
    if not dfs: raise Exception("No se encontraron datos")
    final_df = pd.concat(dfs, ignore_index=True)
    out = repositories.create_excel_from_dataframe(final_df)
    return out.getvalue(), "Reporte.xlsx"