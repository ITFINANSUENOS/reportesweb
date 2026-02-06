from fastapi import APIRouter
from src.api.v1.routes import reportes
from src.api.v1.routes import cartera_analytics
from src.api.v1.routes import busquedas

api_router = APIRouter()

# Aquí registramos el nuevo módulo de reportes
api_router.include_router(
    reportes.router, 
    prefix="/reportes", 
    tags=["Reportes y Archivos"]
)

# 2. Registra el módulo con un prefijo ordenado
api_router.include_router(
    cartera_analytics.router,
    prefix="/tableros/cartera",  
    tags=["Tablero Cartera"]
)

api_router.include_router(
    busquedas.router,
    prefix="/busquedas",
    tags=["Motor de Búsqueda"]
)

