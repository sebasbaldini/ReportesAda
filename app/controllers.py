# app/controllers.py (MODIFICADO)

from flask import (
    render_template, request, make_response, jsonify, 
    Blueprint, g, session, redirect, url_for
)
from flask_login import login_required, current_user 
from . import services 
import config

main_bp = Blueprint('main', __name__)

# --- RUTA PRINCIPAL (Homepage) (MODIFICADA) ---
@main_bp.route('/')
@login_required 
def index():
    # 'g.db_key' la define el @before_request en __init__.py
    ema_locations = services.get_ema_locations_service(g.db_key)
    owm_api_key = config.OWM_API_KEY
    return render_template('index.html', 
                           ema_locations=ema_locations, 
                           owm_api_key=owm_api_key)


# --- RUTA DE REPORTES (MODIFICADA) ---
@main_bp.route('/reportes', methods=['GET']) 
@login_required 
def report_page():
    # 'g.db_key' la define el @before_request en __init__.py
    emas_display_list = services.get_ema_list_service(g.db_key)
    return render_template('reportes.html', emas_list=emas_display_list)


# --- RUTA DE GRÁFICOS (Dashboard) (MODIFICADA) ---
@main_bp.route('/graficos', methods=['GET'])
@login_required
def chart_dashboard_page():
    # 'g.db_key' la define el @before_request en __init__.py
    emas_display_list = services.get_ema_list_service(g.db_key)
    
    # Seleccionar EMA por defecto (la 1 o la primera)
    default_ema_id = 1
    # (Hacemos un chequeo más seguro)
    if not emas_display_list:
        default_ema_id = 0
    elif not any(e[0] == 1 for e in emas_display_list):
        default_ema_id = emas_display_list[0][0]

    dashboard_data = {}
    if default_ema_id != 0:
        dashboard_data = services.get_dashboard_data_service(g.db_key, default_ema_id)
    
    return render_template('graficos.html', 
                           emas_list=emas_display_list,
                           selected_ema_id=default_ema_id,
                           dashboard_data=dashboard_data)


# --- RUTA DE GRÁFICOS PERSONALIZADOS (MODIFICADA) ---
@main_bp.route('/graficos-personalizados', methods=['GET'])
@login_required
def custom_chart_page():
    # 'g.db_key' la define el @before_request en __init__.py
    emas_display_list = services.get_ema_list_service(g.db_key)
    return render_template('graficos_personalizados.html', emas_list=emas_display_list)


# --- RUTA DE DESCARGA (MODIFICADA) ---
@main_bp.route('/download-report', methods=['POST']) 
@login_required 
def download_report():
    try:
        # 'g.db_key' la define el @before_request en __init__.py
        excel_data, nombre_archivo = services.generate_report_service(g.db_key, request.form)
        
        response = make_response(excel_data)
        response.headers['Content-Disposition'] = f'attachment; filename={nombre_archivo}'
        response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        return response

    except Exception as e:
        print(f"!!! ERROR EN POST '/download-report': {e}")
        import traceback
        traceback.print_exc()
        return f"Error al generar el reporte: {e}", 500

# --- RUTA DE SENSORES (MODIFICADA) ---
@main_bp.route('/get-sensors/<ema_id>')
@login_required 
def get_sensors_for_ema(ema_id):
    try:
        # 'g.db_key' la define el @before_request en __init__.py
        sensores = services.get_sensors_for_ema_service(g.db_key, ema_id)
        return jsonify(sensores)
    except Exception as e:
        print(f"Error en get_sensors_for_ema: {e}")
        import traceback; traceback.print_exc()
        return jsonify({'error': str(e)}), 500

# --- RUTA API DE GRÁFICOS (MODIFICADA) ---
@main_bp.route('/api/get-chart-data', methods=['GET'])
@login_required
def get_chart_data():
    try:
        ema_id = request.args.get('ema_id')
        sensor_info_list = request.args.getlist('sensor_info') 
        fecha_inicio = request.args.get('fecha_inicio')
        fecha_fin = request.args.get('fecha_fin')
        combine_flag = request.args.get('combine', 'false').lower() == 'true'

        if not all([ema_id, fecha_inicio, fecha_fin]) or not sensor_info_list:
            return jsonify({'error': 'Faltan parámetros (ema, sensor, inicio o fin)'}), 400
        
        # 'g.db_key' la define el @before_request en __init__.py
        charts_data = services.get_chart_data_service(
            db_key=g.db_key,
            ema_id=ema_id,
            sensor_info_list=sensor_info_list,
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            combine=combine_flag
        )
        
        return jsonify(charts_data)
        
    except Exception as e:
        print(f"Error en API get_chart_data: {e}")
        import traceback; traceback.print_exc()
        return jsonify({'error': str(e)}), 500

# --- RUTA API DASHBOARD (MODIFICADA) ---
@main_bp.route('/api/dashboard-data/<int:ema_id>')
@login_required
def get_dashboard_data(ema_id):
    try:
        # 'g.db_key' la define el @before_request en __init__.py
        dashboard_data = services.get_dashboard_data_service(g.db_key, ema_id)
        return jsonify(dashboard_data)
    except Exception as e:
        print(f"Error en API get_dashboard_data: {e}")
        import traceback; traceback.print_exc()
        return jsonify({'error': str(e)}), 500

# --- ¡NUEVA RUTA! Para cambiar la base de datos ---
@main_bp.route('/change-db/<string:db_key>')
@login_required
def change_db(db_key):
    """
    Guarda la base de datos seleccionada en la sesión del usuario.
    """
    if db_key in config.DATABASE_CONNECTIONS:
        session['db_key'] = db_key
        print(f"Cambiando a la base de datos: {db_key}")
    else:
        print(f"Intento de cambio a DB inválida: {db_key}")
    
    # Redirigir a la página desde la que vino
    # (Si no sabemos, lo mandamos a la homepage)
    return redirect(request.referrer or url_for('main.index'))