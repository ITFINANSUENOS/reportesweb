from pydantic import BaseModel
from typing import List, Any

# --- RESPUESTAS DE SUBIDA ---

class UrlSubidaResponse(BaseModel):
    upload_url: str
    file_key: str

class MensajeRespuesta(BaseModel):
    message: str
    status: str

class ProcesamientoResponse(MensajeRespuesta):
    job_id: str

# --- RESPUESTAS DE BÚSQUEDA ---

class BusquedaResponse(BaseModel):
    total_registros: int
    pagina_actual: int
    total_paginas: int
    data: List[Any] # Lista de filas