# app/controllers.py (MODIFICADO: Alertas con Flash)

from flask import (
    render_template, request, make_response, jsonify, 
    Blueprint, g, session, redirect, url_for, flash # <--- Agregamos 'flash'
)
from flask_login import login_required, current_user 
from datetime import datetime # Importante para fechas
from . import services 
import config

main_bp = Blueprint('main', __name__)

@main_bp.route('/')
@login_required 
def index():
    ema_locations = services.get_ema_locations_service(g.db_key)
    owm_api_key = config.OWM_API_KEY
    return render_template('index.html', ema_locations=ema_locations, owm_api_key=owm_api_key)

@main_bp.route('/reportes', methods=['GET']) 
@login_required 
def report_page():
    emas_display_list = services.get_ema_list_service(g.db_key)
    return render_template('reportes.html', emas_list=emas_display_list)

@main_bp.route('/graficos', methods=['GET'])
@login_required
def chart_dashboard_page():
    emas_display_list = services.get_ema_list_service(g.db_key)
    default_ema_id = 1
    if not emas_display_list: default_ema_id = 0
    elif not any(e[0] == 1 for e in emas_display_list): default_ema_id = emas_display_list[0][0]
    dashboard_data = {}
    if default_ema_id != 0:
        dashboard_data = services.get_dashboard_data_service(g.db_key, default_ema_id)
    return render_template('graficos.html', emas_list=emas_display_list, selected_ema_id=default_ema_id, dashboard_data=dashboard_data)

@main_bp.route('/graficos-personalizados', methods=['GET'])
@login_required
def custom_chart_page():
    emas_display_list = services.get_ema_list_service(g.db_key)
    return render_template('graficos_personalizados.html', emas_list=emas_display_list)

# --- RUTA DE DESCARGA (CON ALERTA EN PANTALLA) ---
@main_bp.route('/download-report', methods=['POST']) 
@login_required 
def download_report():
    try:
        # Validación de usuario restringido
        if getattr(current_user, 'role', 'admin') == 'restricted':
            f_inicio = datetime.strptime(request.form.get('fecha_inicio'), '%Y-%m-%d')
            f_fin = datetime.strptime(request.form.get('fecha_fin'), '%Y-%m-%d')
            delta = f_fin - f_inicio
            
            if delta.days > 31:
                # CAMBIO: Usamos flash para mostrar el mensaje en la misma página
                flash('Error: Su usuario está limitado a descargar reportes de máximo 31 días.', 'danger')
                # Recargamos la página de reportes
                return redirect(url_for('main.report_page'))

        excel_data, nombre_archivo = services.generate_report_service(g.db_key, request.form)
        
        response = make_response(excel_data)
        response.headers['Content-Disposition'] = f'attachment; filename={nombre_archivo}'
        response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        return response

    except Exception as e:
        print(f"ERROR DOWNLOAD: {e}")
        flash(f"Ocurrió un error al generar el reporte: {str(e)}", 'danger')
        return redirect(url_for('main.report_page'))

@main_bp.route('/get-sensors/<ema_id>')
@login_required 
def get_sensors_for_ema(ema_id):
    try:
        sensores = services.get_sensors_for_ema_service(g.db_key, ema_id)
        return jsonify(sensores)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@main_bp.route('/api/get-chart-data', methods=['GET'])
@login_required
def get_chart_data():
    try:
        ema_id = request.args.get('ema_id')
        sensor_info_list = request.args.getlist('sensor_info') 
        fecha_inicio = request.args.get('fecha_inicio')
        fecha_fin = request.args.get('fecha_fin')
        combine_flag = request.args.get('combine', 'false').lower() == 'true'

        if not all([ema_id, fecha_inicio, fecha_fin]): return jsonify({'error': 'Faltan parámetros'}), 400
        
        # Validación de fecha (Aquí retornamos JSON porque es AJAX)
        if getattr(current_user, 'role', 'admin') == 'restricted':
            f_inicio = datetime.strptime(fecha_inicio, '%Y-%m-%d')
            f_fin = datetime.strptime(fecha_fin, '%Y-%m-%d')
            delta = f_fin - f_inicio
            if delta.days > 31:
                return jsonify({'error': 'Su usuario está limitado a visualizar máximo 31 días.'}), 403
        
        charts_data = services.get_chart_data_service(g.db_key, ema_id, sensor_info_list, fecha_inicio, fecha_fin, combine_flag)
        return jsonify(charts_data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@main_bp.route('/api/dashboard-data/<int:ema_id>')
@login_required
def get_dashboard_data(ema_id):
    try:
        dashboard_data = services.get_dashboard_data_service(g.db_key, ema_id)
        return jsonify(dashboard_data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@main_bp.route('/change-db/<string:db_key>')
@login_required
def change_db(db_key):
    if db_key in config.DATABASE_CONNECTIONS: session['db_key'] = db_key
    return redirect(request.referrer or url_for('main.index'))