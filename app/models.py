# Este archivo define la CLASE de Usuario.
# Ya NO es un modelo de base de datos, es una clase simple de Python
# que "finge" ser un usuario para que Flask-Login sea feliz.

from flask_login import UserMixin
from werkzeug.security import check_password_hash
import config # Importamos config para buscar los usuarios
from . import login_manager # Importamos el login_manager de __init__.py

class User(UserMixin):
    """
    Clase de Usuario simple que no depende de la base de datos.
    Usa UserMixin para ser compatible con Flask-Login.
    """
    def __init__(self, user_id, username, email, password_hash):
        self.id = user_id
        self.username = username
        self.email = email
        self.password_hash = password_hash

    def check_password(self, password):
        """Verifica si la contraseña coincide con el hash."""
        return check_password_hash(self.password_hash, password)

    def __repr__(self):
        return f'<User {self.username}>'

# --- Loader de Usuario para Flask-Login (MODIFICADO) ---
# Ahora, en lugar de buscar en la BD, busca en el
# diccionario APP_USERS de nuestro config.py

@login_manager.user_loader
def load_user(user_id):
    """Carga un usuario desde nuestro diccionario en config.py"""
    user_data = config.APP_USERS.get(str(user_id))
    
    if not user_data:
        return None
        
    # Crear una instancia de nuestra clase User
    return User(
        user_id=str(user_id),
        username=user_data.get('username'),
        email=user_data.get('email'),
        password_hash=user_data.get('password_hash')
    )