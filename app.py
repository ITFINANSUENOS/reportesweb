from fastapi import FastAPI
from src.api.v1.router import api_router

app = FastAPI(title="API Electrocréditos", version="1.0.0")

# Incluimos todas las rutas de la V1
app.include_router(api_router, prefix="/api/v1")

if __name__ == "__main__":
    import uvicorn
    # Ejecutar servidor
    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=True)