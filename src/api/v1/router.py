from fastapi import APIRouter
from src.api.v1.routes import reportes_route
from src.api.v1.routes import cartera_analytics_route
from src.api.v1.routes import busquedas_route

api_router = APIRouter()

# Aquí registramos el nuevo módulo de reportes
api_router.include_router(
    reportes_route.router, 
    prefix="/reportes", 
    tags=["Reportes y Archivos"]
)

# 2. Registra el módulo con un prefijo ordenado
api_router.include_router(
    cartera_analytics_route.router,
    prefix="/tableros/cartera",  
    tags=["Tablero Cartera"]
)

api_router.include_router(
    busquedas_route.router,
    prefix="/busquedas",
    tags=["Motor de Búsqueda"]
)

