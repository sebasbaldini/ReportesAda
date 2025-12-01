# app/models.py (VERSIÓN DEFINITIVA)

from flask_login import UserMixin
from werkzeug.security import check_password_hash
import config
from . import login_manager

class User(UserMixin):
    def __init__(self, user_id, username, email, password_hash, role='admin'):
        self.id = user_id
        self.username = username
        self.email = email
        self.password_hash = password_hash
        # ESTA LÍNEA ES FUNDAMENTAL:
        self.role = role 

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

    def __repr__(self):
        return f'<User {self.username} ({self.role})>'

@login_manager.user_loader
def load_user(user_id):
    user_data = config.APP_USERS.get(str(user_id))
    
    if not user_data:
        return None
        
    return User(
        user_id=str(user_id),
        username=user_data.get('username'),
        email=user_data.get('email'),
        password_hash=user_data.get('password_hash'),
        # ACÁ LEEMOS EL ROL DESDE CONFIG:
        role=user_data.get('role', 'admin') 
    )