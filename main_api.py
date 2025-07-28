from fastapi import FastAPI
from src.api import datacredito_route # Importa tus rutas
# from src.api import cifin_routes # (A futuro)

app = FastAPI(
    title="API de Procesamiento de Reportes Financieros",
    description="Procesa archivos de Datacrédito y CIFIN."
)

# Incluye las rutas de Datacrédito en la aplicación principal
app.include_router(datacredito_route.router, prefix="/api/v1")

# (A futuro) Incluye las rutas de CIFIN
# app.include_router(cifin_routes.router, prefix="/api/v1")

@app.get("/", tags=["Root"])
def read_root():
    return {"message": "Bienvenido a la API de Procesamiento de Reportes"}