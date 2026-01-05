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
                'id': s.id_proyecto if s.id_proyecto else s.id, 
                'db_id': s.id, 
                'nombre': s.ubicacion or s.id_proyecto, 
                'descripcion': f"Cuenca: {s.nomcuenca} | Partido: {s.pdo}",
                'lat': s.latitude, 'lon': s.longitude, 'source_db': 'postgres',
                'partido': s.pdo or "Sin Definir", 'cuenca': s.nomcuenca or "Sin Definir",
                'red': tipo_red, 
                'status': estado,
                'is_ema': tiene_ema,
                'last_aforo': str_aforo,
                'last_escala': str_escala
            })
    return result

# --- CORRECCIÓN FECHA DE INICIO ---
def get_sensors_for_ema_service(dummy_db, ema_id):
    # Llamamos al repositorio optimizado
    sensors = repositories.get_sensors_for_station_repo(ema_id)
    
    # Filtrado de seguridad (Mantenemos esto)
    if current_user.is_authenticated and getattr(current_user, 'role', 'admin') == 'restricted':
        sensors = [s for s in sensors if 'bateria' not in s['search_text']]
    
    # ELIMINAR EL BUCLE QUE BUSCABA FECHAS AQUÍ. 
    # Ya lo hace el repositorio en una sola consulta.
                
    return sensors

def get_dashboard_data_service(dummy_db, ema_id):
    return repositories.get_dashboard_data_repo(ema_id)

def get_map_popup_status_service(dummy_db, ema_id):
    return repositories.get_map_popup_status_repo(ema_id)

def generate_report_service(dummy_db, form_data):
    """Genera reporte de EMAs (Sensores)"""
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

def _process_single_sensor(sensor_info_str, ema_id, f_inicio, f_fin, is_combined, single_day_mode):
    try:
        parts = sensor_info_str.split('|')
        metric_real = parts[1] if len(parts) > 1 else 'Sensor'
        display_name = parts[2] if len(parts) > 2 else metric_real
    except:
        return [], []

    df = repositories.get_chart_data_repo(ema_id, [sensor_info_str], f_inicio, f_fin)
    if df.empty:
        return [], []

    if 'fecha' in df.columns:
        df['fecha'] = pd.to_datetime(df['fecha'])
    df.sort_values('fecha', inplace=True)

    es_nivel = 'Limni' in metric_real or 'Freat' in metric_real or 'Nivel' in display_name
    es_pluvio = 'Pluvio' in metric_real

    delta_days = 0
    try:
        if f_inicio and f_fin:
            d1 = pd.to_datetime(f_inicio)
            d2 = pd.to_datetime(f_fin)
            delta_days = (d2 - d1).days
    except:
        delta_days = 0

    datasets = []
    labels = []

    # --- CASO A: AGRUPADO POR DÍA (> 1 día) ---
    if delta_days > 0 and not single_day_mode:
        df['fecha_dia'] = df['fecha'].dt.date
        grp_dates = df['fecha_dia'].unique()
        grp_dates.sort()
        labels = [str(d) for d in grp_dates]

        y_axis = 'y'
        if is_combined:
            y_axis = 'y-lluvia' if es_pluvio else 'y-nivel'

        if es_nivel:
            daily_grp = df.groupby('fecha_dia')['valor'].agg(['max', 'min']).reset_index()
            count = len(daily_grp)
            
            datasets.append({
                'label': f"{display_name} (Máx)",
                'data': daily_grp['max'].tolist(),
                'type': 'line',
                'borderColor': '#0d6efd', 'backgroundColor': '#0d6efd',
                'borderWidth': 2, 'tension': 0.3, 'fill': False, 'pointRadius': 3,
                'yAxisID': y_axis
            })
            datasets.append({
                'label': f"{display_name} (Mín)",
                'data': daily_grp['min'].tolist(),
                'type': 'line',
                'borderColor': '#0dcaf0', 'backgroundColor': '#0dcaf0',
                'borderWidth': 2, 'tension': 0.3, 'fill': False, 'pointRadius': 3,
                'yAxisID': y_axis
            })
            if count > 0:
                avg_max = daily_grp['max'].mean()
                avg_min = daily_grp['min'].mean()
                datasets.append({
                    'label': f"Prom. Máx ({avg_max:.2f})", 'data': [avg_max]*count,
                    'type': 'line', 'borderColor': '#dc3545', 'borderWidth': 1.5,
                    'borderDash': [6, 4], 'pointRadius': 0, 'yAxisID': y_axis
                })
                datasets.append({
                    'label': f"Prom. Mín ({avg_min:.2f})", 'data': [avg_min]*count,
                    'type': 'line', 'borderColor': '#ffc107', 'borderWidth': 1.5,
                    'borderDash': [6, 4], 'pointRadius': 0, 'yAxisID': y_axis
                })

        elif es_pluvio:
            daily_sum = df.groupby('fecha_dia')['valor'].sum().reset_index()
            datasets.append({
                'label': f"{display_name} (Acumulado)",
                'data': daily_sum['valor'].tolist(),
                'type': 'bar',
                'backgroundColor': '#0d6efd', 'borderColor': '#0d6efd',
                'borderWidth': 1,
                'yAxisID': y_axis
            })
        else:
            daily_avg = df.groupby('fecha_dia')['valor'].mean().reset_index()
            datasets.append({
                'label': f"{display_name} (Promedio)",
                'data': daily_avg['valor'].tolist(),
                'type': 'line',
                'borderColor': '#6c757d', 'backgroundColor': '#6c757d',
                'borderWidth': 2, 'tension': 0.3, 'fill': False, 'pointRadius': 3,
                'yAxisID': y_axis
            })

    # --- CASO B: DETALLE (<= 1 día) ---
    else:
        labels = df['fecha'].dt.strftime('%Y-%m-%d %H:%M').tolist()
        y_axis = 'y'
        if is_combined:
            y_axis = 'y-lluvia' if es_pluvio else 'y-nivel'

        c_type = 'bar' if es_pluvio else 'line'
        color = '#0d6efd'
        if 'Temp' in metric_real: color = '#dc3545'
        elif 'Humedad' in metric_real: color = '#198754'
        elif 'Viento' in metric_real: color = '#6c757d'
        elif 'Bateria' in metric_real: color = '#ffc107'
        
        datasets.append({
            'label': display_name,
            'data': df['valor'].tolist(),
            'type': c_type,
            'borderColor': color, 'backgroundColor': color,
            'borderWidth': 2 if c_type == 'line' else 1,
            'tension': 0.3, 'fill': False, 'pointRadius': 2,
            'yAxisID': y_axis
        })

    return datasets, labels


# ==============================================================================
# 2. FUNCIÓN PRINCIPAL (Genera JSON para Chart.js)
# ==============================================================================
def get_chart_data_service(dummy_db, ema_id, sensor_info_list, f_inicio, f_fin, combine=False):
    es_un_dia = (f_inicio == f_fin)
    result_charts = [] 

    if combine:
        scales_config = {
            'x': { 'title': { 'display': True, 'text': 'Hora' if es_un_dia else 'Fecha' } },
            'y-nivel': { 
                'type': 'linear', 'display': True, 'position': 'left', 
                'title': { 'display': True, 'text': 'Nivel (m) / Otros' },
                'grid': { 'drawOnChartArea': False }
            },
            'y-lluvia': { 
                'type': 'linear', 'display': True, 'position': 'right', 
                'title': { 'display': True, 'text': 'Lluvia (mm)' },
                'grid': { 'drawOnChartArea': False },
                'beginAtZero': True
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
                'plugins': { 'title': { 'display': True, 'text': 'Gráfico Combinado (Lluvia vs Nivel)' } }
            }
        })

    else:
        for s_info in sensor_info_list:
            ds_list, fe = _process_single_sensor(s_info, ema_id, f_inicio, f_fin, is_combined=False, single_day_mode=es_un_dia)
            if ds_list:
                parts = s_info.split('|')
                nombre = parts[2] if len(parts) > 2 else "Sensor"
                base_type = ds_list[0]['type']
                scales_config = { 
                    'x': { 'title': { 'display': True, 'text': 'Hora' if es_un_dia else 'Fecha' } }, 
                    'y': { 'title': { 'display': True, 'text': 'Valor' }, 'position': 'left' } 
                }
                
                result_charts.append({
                    'chart_type': base_type,
                    'labels': sorted(list(set(fe))),
                    'datasets': ds_list,
                    'options': { 
                        'responsive': True, 
                        'maintainAspectRatio': False, 
                        'scales': scales_config, 
                        'plugins': { 'title': { 'display': True, 'text': nombre } } 
                    }
                })

    return result_charts


# ==========================================
# 2. SERVICIOS DE AFOROS
# ==========================================

def get_aforos_active_list_service():
    stations = repositories.get_stations_with_aforos_repo()
    lista = []
    for s in stations:
        display = f"{s.ubicacion} ({s.id})" if s.ubicacion else s.id
        lista.append((s.id, display))
    return lista

def generate_aforo_report_service(form_data):
    station_id = form_data.get('ema_id')
    f_inicio = form_data.get('fecha_inicio')
    f_fin = form_data.get('fecha_fin')

    if not station_id:
        raise Exception("Debe seleccionar una estación.")

    df = repositories.get_aforos_data_repo(station_id, f_inicio, f_fin)

    if df.empty:
        raise Exception("No se encontraron datos de aforos para los filtros seleccionados.")

    output = repositories.create_excel_simple(df)
    return output.getvalue(), f"Reporte_Aforos_{station_id}.xlsx"

# ==========================================
# 3. SERVICIOS DE ESCALAS
# ==========================================

def get_escalas_active_list_service():
    stations = repositories.get_stations_with_escalas_repo()
    lista = []
    for s in stations:
        display = f"{s.ubicacion} ({s.id})" if s.ubicacion else s.id
        lista.append((s.id, display))
    return lista

def generate_escala_report_service(form_data):
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

# ==========================================
# 4. EXPORTAR EXCEL GRÁFICOS (CON INFO EMA)
# ==========================================

def generate_chart_excel_service(user, ema_id, sensor_info_list, f_inicio, f_fin):
    import pandas as pd
    import io

    # 1. Recuperamos datos del gráfico
    charts_data = get_chart_data_service(user, ema_id, sensor_info_list, f_inicio, f_fin, combine=False)
    if not charts_data: return None, "sin_datos.xlsx"

    # 2. Recuperamos info de la estación para el encabezado
    all_stations = repositories.get_all_stations_repo()
    station_obj = next((s for s in all_stations if s.id_proyecto == ema_id), None)
    
    ema_nombre = station_obj.ubicacion if station_obj else ema_id
    ema_partido = station_obj.pdo if station_obj else "Desconocido"
    ema_lat = str(station_obj.latitude) if station_obj else "-"
    ema_long = str(station_obj.longitude) if station_obj else "-"

    output = io.BytesIO()

    try:
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            # Formatos de encabezado
            title_fmt = workbook.add_format({'bold': True, 'font_size': 14, 'color': '#0d6efd'})
            bold_fmt = workbook.add_format({'bold': True})

            for idx, chart_obj in enumerate(charts_data):
                labels = chart_obj['labels']
                datasets = chart_obj['datasets']
                chart_title = chart_obj['options']['plugins']['title']['text']
                
                # Armamos el DataFrame
                data_dict = {'Fecha': labels}
                for ds in datasets:
                    clean_label = ds['label'].replace('[', '(').replace(']', ')')
                    data_dict[clean_label] = ds['data']
                
                df_sheet = pd.DataFrame(data_dict)
                sheet_name = f"Grafico_{idx+1}"
                
                # 3. ESCRIBIR DATOS (Dejando 7 filas arriba para encabezado)
                df_sheet.to_excel(writer, sheet_name=sheet_name, startrow=7, index=False)
                
                worksheet = writer.sheets[sheet_name]
                
                # 4. ESCRIBIR ENCABEZADO PERSONALIZADO
                worksheet.write(0, 0, f"Reporte de Estación: {ema_nombre}", title_fmt)
                worksheet.write(2, 0, "Partido:", bold_fmt)
                worksheet.write(2, 1, ema_partido)
                worksheet.write(3, 0, "Latitud:", bold_fmt)
                worksheet.write(3, 1, ema_lat)
                worksheet.write(4, 0, "Longitud:", bold_fmt)
                worksheet.write(4, 1, ema_long)
                
                worksheet.write(2, 3, "Desde:", bold_fmt)
                worksheet.write(2, 4, f_inicio)
                worksheet.write(3, 3, "Hasta:", bold_fmt)
                worksheet.write(3, 4, f_fin)

                # Ajuste de ancho de columnas
                worksheet.set_column(0, 0, 22)
                worksheet.set_column(1, 10, 15)

                # Gráfico de Excel insertado al lado
                c_type = 'column' if chart_obj['chart_type'] == 'bar' else 'line'
                excel_chart = workbook.add_chart({'type': c_type})
                
                num_rows = len(df_sheet)
                # Ojo: ajustamos las referencias de fila porque empezamos en fila 7 (indice 6)
                # startrow=7 significa que los headers de tabla estan en fila 8 (indice 7)
                header_row = 7
                data_start_row = 8
                data_end_row = 7 + num_rows

                for col_num, col_name in enumerate(df_sheet.columns):
                    if col_name == 'Fecha': continue
                    excel_chart.add_series({
                        'name':       [sheet_name, header_row, col_num],
                        'categories': [sheet_name, data_start_row, 0, data_end_row, 0],
                        'values':     [sheet_name, data_start_row, col_num, data_end_row, col_num],
                    })

                excel_chart.set_title({'name': chart_title})
                excel_chart.set_size({'width': 800, 'height': 450})
                # Insertamos gráfico más abajo para no tapar el encabezado
                worksheet.insert_chart('E2', excel_chart)
                
    except Exception as e:
        print(f"Error generando XlsxWriter: {e}")
        return None, "error_libreria.xlsx"

    return output, f"Reporte_{ema_id}.xlsx"