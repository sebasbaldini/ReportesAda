# app/__init__.py (MODIFICADO)

from flask import Flask, session, request, g
from .extensions import login_manager
import config
from datetime import timedelta 

# ¡NUEVO! Importamos los servicios
from . import services



def create_app():
    app = Flask(__name__, instance_relative_config=False)
    app.config['SECRET_KEY'] = config.SECRET_KEY
    
    app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(minutes=30)
    
    login_manager.init_app(app)
    login_manager.login_view = 'auth.login' 
    login_manager.login_message = 'Por favor, inicie sesión para acceder.'
    login_manager.login_message_category = 'info'

    # --- ¡NUEVO! Cargar la lista de BDs ---
    # Esto hace que 'db_list' y 'current_db_key' estén
    # disponibles en TODAS las plantillas (para el menú)
    @app.context_processor
    def inject_db_context():
        # Construir la lista para el desplegable
        db_list = [
            {'key': key, 'name': details['display_name']} 
            for key, details in config.DATABASE_CONNECTIONS.items()
        ]
        # Obtener la BD actual de la sesión del usuario
        current_db_key = session.get('db_key', 'db_principal') # 'db_principal' por defecto
        
        return dict(
            db_list=db_list,
            current_db_key=current_db_key,
            current_db_name=config.DATABASE_CONNECTIONS.get(current_db_key, {}).get('display_name', 'Error')
        )

    # --- ¡NUEVO! Guardar la DB seleccionada ---
    # Esto se ejecuta ANTES de cada ruta
    @app.before_request
    def before_request():
        # 1. Resetear el timeout de la sesión
        session.permanent = True
        app.permanent_session_lifetime = timedelta(minutes=30)
        
        # 2. Asegurarnos de que el usuario TENGA una BD seleccionada
        if 'db_key' not in session:
            session['db_key'] = 'db_principal' # 'db_principal' por defecto
        
        # 3. Guardar la clave de la BD en 'g'
        # 'g' es un objeto temporal de Flask para esta petición.
        # Esto nos evita tener que pasar 'db_key' como argumento
        # desde el controlador al servicio.
        g.db_key = session.get('db_key')


    with app.app_context():
        # --- ¡NUEVO! Construir los Caches al inicio ---
        # (Llamamos a la función que ahora vive en el servicio)
        services.G_SENSOR_CACHE = services.build_global_cache()

        # Importar modelos (para que se registre el user_loader)
        from . import models

        # Importar controladores (Blueprints)
        from . import controllers
        from . import auth_controllers 

        # Registrar los blueprints
        app.register_blueprint(controllers.main_bp)
        app.register_blueprint(auth_controllers.auth_bp) 

        return app