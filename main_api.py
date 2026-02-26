from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware 
from fastapi.middleware.gzip import GZipMiddleware 

from src.api.v1.router import api_router 

app = FastAPI(
    title="API de Procesamiento de Reportes Financieros (Asíncrona)",
    description="Procesa archivos pesados de Datacrédito y CIFIN usando un flujo de S3 y tareas en segundo plano."
)

# 2. AGREGAR GZIP Justo debajo de la creación de 'app'
# Comprime cualquier respuesta que pese más de 500 bytes
app.add_middleware(GZipMiddleware, minimum_size=500)

# 2. CONFIGURACIÓN DE CORS
# Esto le dice al navegador: "Deja que localhost:5173 hable conmigo"
app.add_middleware(
    CORSMiddleware,
    # Lista de dominios permitidos (El puerto de Vite es 5173)
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173", "*"], 
    allow_credentials=True,
    allow_methods=["*"],  # Permitir todos los métodos (GET, POST, PUT, DELETE)
    allow_headers=["*"],  # Permitir todos los headers (Authorization, Content-Type, etc.)
)

# 3. Agrega el router que contiene REPORTES
app.include_router(api_router, prefix="/api/v1")