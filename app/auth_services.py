# Este archivo maneja la LÓGICA DE NEGOCIO de la autenticación.
# Es el "Servicio" de autenticación.

from . import auth_repositories
from werkzeug.security import generate_password_hash

def login_user_service(username, password):
    """
    Intenta loguear un usuario.
    Devuelve el objeto User si es exitoso, o None si falla.
    """
    # 1. Pedirle el usuario al REPOSITORIO
    user = auth_repositories.get_user_by_username_repo(username)
    
    # 2. Verificar la contraseña
    if user and user.check_password(password):
        # Si el usuario existe y la contraseña es correcta
        return user
    
    # Si no, devolvemos None
    return None

def logout_user_service():
    """Desloguea al usuario actual."""
    # En este caso, la librería 'flask_login' hace el trabajo
    from flask_login import logout_user
    logout_user()

def create_hash_service(password):
    """Genera un hash de contraseña seguro."""
    return generate_password_hash(password)