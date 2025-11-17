# app/auth_repositories.py
# Este archivo maneja el ACCESO A DATOS de los usuarios.
# Es el "Repositorio" de autenticación.
# Es el único que sabe DÓNDE están guardados los usuarios (en config.py)

from .models import User
import config

def get_user_by_username_repo(username):
    """
    Busca un usuario por su 'username' en el diccionario
    de config.py
    """
    # Recorremos el diccionario de usuarios en config.py
    for user_id, user_data in config.APP_USERS.items():
        if user_data['username'].lower() == username.lower():
            # Si encontramos el username, creamos un objeto User
            return User(
                user_id=str(user_id),
                username=user_data.get('username'),
                email=user_data.get('email'),
                password_hash=user_data.get('password_hash')
            )
            
    # Si no lo encontramos
    return None