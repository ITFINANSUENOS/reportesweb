from fastapi import FastAPI
# 1. IMPORTAR EL MIDDLEWARE DE CORS
from fastapi.middleware.cors import CORSMiddleware 

from src.api.v1.router import api_router 
from src.api.procesamiento_route import router as procesamiento_router

app = FastAPI(
    title="API de Procesamiento de Reportes Financieros (Asíncrona)",
    description="Procesa archivos pesados de Datacrédito y CIFIN usando un flujo de S3 y tareas en segundo plano."
)

# ==========================================
# 2. CONFIGURACIÓN DE CORS (CRÍTICO) 🛡️
# ==========================================
# Esto le dice al navegador: "Deja que localhost:5173 hable conmigo"
app.add_middleware(
    CORSMiddleware,
    # Lista de dominios permitidos (El puerto de Vite es 5173)
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173", "*"], 
    allow_credentials=True,
    allow_methods=["*"],  # Permitir todos los métodos (GET, POST, PUT, DELETE)
    allow_headers=["*"],  # Permitir todos los headers (Authorization, Content-Type, etc.)
)
# ==========================================

# --- ZONA DE INCLUDES ---

# 3. Agrega el router que contiene REPORTES
app.include_router(api_router, prefix="/api/v1") 

# Mantén tus rutas antiguas de procesamiento si las sigues usando
app.include_router(procesamiento_router, prefix="/api/v1")

@app.get("/", tags=["Root"])
def read_root():
    """
    Endpoint de bienvenida.
    """
    return {"message": "Bienvenido a la API de Procesamiento de Reportes Asíncrona"}