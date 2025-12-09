from flask import Flask
from datetime import timedelta
from .extensions import db, login_manager
import config

def create_app():
    app = Flask(__name__, instance_relative_config=False)
    
    # Cargar config
    app.config['SECRET_KEY'] = config.SECRET_KEY
    app.config['SQLALCHEMY_DATABASE_URI'] = config.SQLALCHEMY_DATABASE_URI
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(minutes=30)

    # Inicializar extensiones
    db.init_app(app)
    login_manager.init_app(app)
    login_manager.login_view = 'auth.login'

    with app.app_context():
        # Importar partes de la app
        from . import models
        from . import controllers
        from . import auth_controllers
        
        # Registrar Blueprints
        app.register_blueprint(controllers.main_bp)
        app.register_blueprint(auth_controllers.auth_bp)
        
        # Contexto global para templates (Nombre DB actual)
        @app.context_processor
        def inject_context():
            return dict(current_db_name="PostgreSQL Unificada")

        # Crear tablas si no existen (opcional, útil la primera vez)
        db.create_all()

        return app