# app/extensions.py
# Este archivo es nuevo.
# Define las extensiones de Flask (como LoginManager)
# para que puedan ser importadas por otros archivos
# sin crear importaciones circulares.

from flask_login import LoginManager

# Definimos el login_manager aquí
login_manager = LoginManager()