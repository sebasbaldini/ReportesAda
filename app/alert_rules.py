# app/alert_rules.py

# Diccionario de reglas.
# LA CLAVE (ej: 'FALBO') es la palabra clave que buscaremos en el NOMBRE de la estación.
# LOS VALORES son los umbrales.

ALERTA_CONFIG = {
    # Regla 1: Puente Falvo
    'FALBO': {
        'amarillo': 0.71,  # Mayor o igual a 0.71
        'naranja': 4.01,   # Mayor o igual a 4.01
        'rojo': 8.02       # Mayor o igual a 8.02
    },
    
    # EJEMPLO PARA EL FUTURO (Descomentar y editar cuando tengas otra)
    # 'ARROYO_SECO': {
    #     'amarillo': 1.50,
    #     'naranja': 2.80,
    #     'rojo': 3.50
    # },
}