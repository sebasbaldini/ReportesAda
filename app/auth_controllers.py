# Este archivo maneja las RUTAS de autenticación (login, logout, etc.)
# Es el "Controlador" de autenticación.

from flask import Blueprint, render_template, request, flash, redirect, url_for
from flask_login import login_user, logout_user, current_user
from . import auth_services

# Creamos un 'Blueprint' (un grupo de rutas) para la autenticación
auth_bp = Blueprint('auth', __name__)

# --- Ruta de Login ---
@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    # Si el usuario ya está logueado, lo mandamos al inicio
    if current_user.is_authenticated:
        return redirect(url_for('main.index'))

    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        # Llamamos al SERVICIO para que intente loguear al usuario
        user = auth_services.login_user_service(username, password)
        
        if user:
            # Si el servicio devuelve un usuario, lo logueamos
            login_user(user)
            # Redirigimos a la página principal
            return redirect(url_for('main.index'))
        else:
            # Si no, mostramos un error
            flash('Usuario o contraseña incorrectos.', 'danger')

    # Si es GET, solo mostramos la página de login
    return render_template('login.html')

# --- Ruta de Logout ---
@auth_bp.route('/logout')
def logout():
    # Llamamos al SERVICIO para desloguear
    auth_services.logout_user_service()
    flash('Sesión cerrada exitosamente.', 'success')
    return redirect(url_for('auth.login'))

# --- Ruta de Registro / Generador de Hash ---
@auth_bp.route('/register', methods=['GET', 'POST'])
def register():
    """
    Esta es una página de AYUDA para generar el HASH
    de la contraseña que tenés que pegar en config.py
    """
    generated_hash = None
    if request.method == 'POST':
        password = request.form.get('password')
        
        # Llamamos al SERVICIO para que genere el hash
        generated_hash = auth_services.create_hash_service(password)
        
        flash('Hash generado exitosamente. Copialo y pegalo en tu config.py', 'success')

    return render_template('register.html', generated_hash=generated_hash)