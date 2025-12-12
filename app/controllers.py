from flask import Blueprint, render_template, request, jsonify, make_response, redirect, url_for, flash
from flask_login import login_required, current_user
from . import services
import config

main_bp = Blueprint('main', __name__)

@main_bp.route('/')
@login_required
def index():
    locations = services.get_all_unified_locations_service()
    return render_template('index.html', ema_locations=locations, owm_api_key=config.OWM_API_KEY)

@main_bp.route('/reportes')
@login_required
def report_page():
    emas = services.get_unified_ema_list_service()
    return render_template('reportes.html', emas_list=emas)

@main_bp.route('/graficos')
@login_required
def chart_dashboard_page():
    emas = services.get_unified_ema_list_service()
    sel_id = emas[0][0] if emas else 0
    data = {}
    if sel_id:
        data = services.get_dashboard_data_service(None, sel_id)
    return render_template('graficos.html', emas_list=emas, selected_ema_id=sel_id, dashboard_data=data)

@main_bp.route('/graficos-personalizados')
@login_required
def custom_chart_page():
    emas = services.get_unified_ema_list_service()
    return render_template('graficos_personalizados.html', emas_list=emas)

# --- APIS ---

@main_bp.route('/get-sensors/<string:ema_id>')
@login_required
def get_sensors_for_ema(ema_id):
    sensors = services.get_sensors_for_ema_service(None, ema_id)
    return jsonify(sensors)

@main_bp.route('/api/dashboard-data/<string:ema_id>')
@login_required
def get_dashboard_data(ema_id):
    data = services.get_dashboard_data_service(None, ema_id)
    return jsonify(data)

@main_bp.route('/download-report', methods=['POST'])
@login_required
def download_report():
    try:
        data, filename = services.generate_report_service(None, request.form)
        response = make_response(data)
        response.headers['Content-Disposition'] = f'attachment; filename={filename}'
        response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        return response
    except Exception as e:
        flash(f"Error: {str(e)}", 'danger')
        return redirect(url_for('main.report_page'))

@main_bp.route('/api/get-chart-data')
@login_required
def get_chart_data():
    try:
        data = services.get_chart_data_service(
            None, 
            request.args.get('ema_id'),
            request.args.getlist('sensor_info'),
            request.args.get('fecha_inicio'),
            request.args.get('fecha_fin'),
            request.args.get('combine') == 'true'
        )
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# --- RUTA NUEVA PARA EL POPUP DEL MAPA ---
@main_bp.route('/api/map-status/<string:ema_id>')
@login_required
def get_map_status_api(ema_id):
    try:
        data = services.get_map_popup_status_service(None, ema_id)
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500