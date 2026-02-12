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

# --- REPORTES LECTURAS (NUEVO) ---
@main_bp.route('/reportes/lecturas')
@login_required
def report_lecturas_page():
    lecturas_list = services.get_lecturas_active_list_service()
    return render_template('reportes_lecturas.html', emas_list=lecturas_list)

@main_bp.route('/download-lecturas-report', methods=['POST'])
@login_required
def download_lecturas_report():
    try:
        data, filename = services.generate_lecturas_report_service(request.form)
        response = make_response(data)
        response.headers['Content-Disposition'] = f'attachment; filename={filename}'
        response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        return response
    except Exception as e:
        flash(f"Error Lecturas: {str(e)}", 'danger')
        return redirect(url_for('main.report_lecturas_page'))

# --- REPORTES EMAS ---
@main_bp.route('/reportes')
@login_required
def report_page():
    emas = services.get_unified_ema_list_service()
    return render_template('reportes.html', emas_list=emas)

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

# --- REPORTES AFOROS ---
@main_bp.route('/reportes/aforos')
@login_required
def report_aforos_page():
    aforos = services.get_aforos_active_list_service()
    return render_template('reportes_aforos.html', emas_list=aforos)

@main_bp.route('/download-aforo-report', methods=['POST'])
@login_required
def download_aforo_report():
    try:
        data, filename = services.generate_aforo_report_service(request.form)
        response = make_response(data)
        response.headers['Content-Disposition'] = f'attachment; filename={filename}'
        response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        return response
    except Exception as e:
        flash(f"Error Aforos: {str(e)}", 'danger')
        return redirect(url_for('main.report_aforos_page'))

# --- REPORTES ESCALAS ---
@main_bp.route('/reportes/escalas')
@login_required
def report_escalas_page():
    try: escalas_list = services.get_escalas_active_list_service()
    except: escalas_list = services.get_unified_ema_list_service()
    return render_template('reportes_escalas.html', emas_list=escalas_list)

@main_bp.route('/download-escala-report', methods=['POST'])
@login_required
def download_escala_report():
    try:
        data, filename = services.generate_escala_report_service(request.form)
        response = make_response(data)
        response.headers['Content-Disposition'] = f'attachment; filename={filename}'
        response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        return response
    except Exception as e:
        flash(f"Error Escalas: {str(e)}", 'danger')
        return redirect(url_for('main.report_escalas_page'))

# --- API y GRÁFICOS ---

@main_bp.route('/graficos')
@login_required
def chart_dashboard_page():
    emas = services.get_unified_ema_list_service()
    sel_id = emas[0][0] if emas else 0
    data = {} if not sel_id else services.get_dashboard_data_service(None, sel_id)
    return render_template('graficos.html', emas_list=emas, selected_ema_id=sel_id, dashboard_data=data)

@main_bp.route('/graficos-personalizados')
@login_required
def custom_chart_page():
    emas = services.get_unified_ema_list_service()
    return render_template('graficos_personalizados.html', emas_list=emas)

@main_bp.route('/get-sensors/<string:ema_id>')
@login_required
def get_sensors_for_ema(ema_id):
    return jsonify(services.get_sensors_for_ema_service(None, ema_id))

@main_bp.route('/api/dashboard-data/<string:ema_id>')
@login_required
def get_dashboard_data(ema_id):
    return jsonify(services.get_dashboard_data_service(None, ema_id))

@main_bp.route('/api/map-status/<string:ema_id>')
@login_required
def get_map_status_api(ema_id):
    try: return jsonify(services.get_map_popup_status_service(None, ema_id))
    except Exception as e: return jsonify({'error': str(e)}), 500

@main_bp.route('/download-chart-excel')
@login_required
def download_chart_excel():
    try:
        excel_io, filename = services.generate_chart_excel_service(current_user, request.args.get('ema_id'), request.args.getlist('sensor_info'), request.args.get('fecha_inicio'), request.args.get('fecha_fin'))
        if not excel_io:
             flash("No hay datos", "warning")
             return redirect(url_for('main.custom_chart_page'))
        response = make_response(excel_io.getvalue())
        response.headers['Content-Disposition'] = f'attachment; filename={filename}'
        response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        return response
    except: return redirect(url_for('main.custom_chart_page'))

@main_bp.route('/api/get-chart-data')
@login_required
def get_chart_data():
    try:
        data = services.get_chart_data_service(None, request.args.get('ema_id'), request.args.getlist('sensor_info'), request.args.get('fecha_inicio'), request.args.get('fecha_fin'), request.args.get('combine') == 'true')
        return jsonify(data)
    except Exception as e: return jsonify({'error': str(e)}), 500