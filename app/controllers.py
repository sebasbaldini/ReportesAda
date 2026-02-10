from flask import Blueprint, render_template, request, jsonify, make_response, redirect, url_for, flash
from flask_login import login_required, current_user
from . import services
from .repositories import check_alert_status_repo
import config

main_bp = Blueprint('main', __name__)

@main_bp.route('/')
@login_required
def index():
    # 1. Obtenemos lista (ahora con IDs limpios desde services.py)
    locations = services.get_all_unified_locations_service()
    
    # 2. Inyectamos Alerta
    if locations:
        for loc in locations:
            pid = loc.get('id')
            pnombre = loc.get('nombre')
            # Buscamos por nombre (contiene FALBO) o por ID
            loc['alerta'] = check_alert_status_repo(pid, pnombre)

    return render_template('index.html', ema_locations=locations, owm_api_key=config.OWM_API_KEY)

# ... (El resto del archivo Controllers sigue igual que antes) ...
# Solo asegúrate de copiar el archivo completo que te pasé antes o mantener las rutas de abajo.
# Por simplicidad, aquí dejo las rutas principales, el resto no cambia.

@main_bp.route('/reportes')
@login_required
def report_page():
    return render_template('reportes.html', emas_list=services.get_unified_ema_list_service())

@main_bp.route('/download-report', methods=['POST'])
@login_required
def download_report():
    data, name = services.generate_report_service(None, request.form)
    response = make_response(data)
    response.headers['Content-Disposition'] = f'attachment; filename={name}'
    response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    return response

@main_bp.route('/reportes/aforos')
@login_required
def report_aforos_page():
    return render_template('reportes_aforos.html', emas_list=services.get_aforos_active_list_service())

@main_bp.route('/download-aforo-report', methods=['POST'])
@login_required
def download_aforo_report():
    data, name = services.generate_aforo_report_service(request.form)
    response = make_response(data)
    response.headers['Content-Disposition'] = f'attachment; filename={name}'
    response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    return response

@main_bp.route('/reportes/escalas')
@login_required
def report_escalas_page():
    return render_template('reportes_escalas.html', emas_list=services.get_escalas_active_list_service())

@main_bp.route('/download-escala-report', methods=['POST'])
@login_required
def download_escala_report():
    data, name = services.generate_escala_report_service(request.form)
    response = make_response(data)
    response.headers['Content-Disposition'] = f'attachment; filename={name}'
    response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    return response

@main_bp.route('/graficos')
@login_required
def chart_dashboard_page():
    emas = services.get_unified_ema_list_service()
    sel_id = emas[0][0] if emas else 0
    return render_template('graficos.html', emas_list=emas, selected_ema_id=sel_id, dashboard_data=services.get_dashboard_data_service(None, sel_id))

@main_bp.route('/graficos-personalizados')
@login_required
def custom_chart_page():
    return render_template('graficos_personalizados.html', emas_list=services.get_unified_ema_list_service())

@main_bp.route('/get-sensors/<string:ema_id>')
@login_required
def get_sensors_for_ema(ema_id):
    return jsonify(services.get_sensors_for_ema_service(None, ema_id))

@main_bp.route('/api/dashboard-data/<string:ema_id>')
@login_required
def get_dashboard_data(ema_id):
    return jsonify(services.get_dashboard_data_service(None, ema_id))

@main_bp.route('/download-chart-excel')
@login_required
def download_chart_excel():
    excel_io, filename = services.generate_chart_excel_service(current_user, request.args.get('ema_id'), request.args.getlist('sensor_info'), request.args.get('fecha_inicio'), request.args.get('fecha_fin'))
    if not excel_io: return redirect(url_for('main.custom_chart_page')) 
    response = make_response(excel_io.getvalue())
    response.headers['Content-Disposition'] = f'attachment; filename={filename}'
    response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    return response

@main_bp.route('/api/get-chart-data')
@login_required
def get_chart_data():
    return jsonify(services.get_chart_data_service(None, request.args.get('ema_id'), request.args.getlist('sensor_info'), request.args.get('fecha_inicio'), request.args.get('fecha_fin'), request.args.get('combine') == 'true'))

@main_bp.route('/api/map-status/<string:ema_id>')
@login_required
def get_map_status_api(ema_id):
    return jsonify(services.get_map_popup_status_service(None, ema_id))