from pydantic import BaseModel
from typing import List, Optional

# --- MODELOS PARA REPORTES (INPUT) ---

class GenerarUrlRequest(BaseModel):
    filename: str
    content_type: str
    file_size: int = 0  # Opcional por compatibilidad

class IniciarProcesamientoRequest(BaseModel):
    file_key: str
    empresa: str
    tipo_reporte: str = "SEGUIMIENTOS"

# --- MODELOS PARA BÚSQUEDAS (INPUT) ---

class FiltrosTabla(BaseModel):
    job_id: str
    page: int = 1
    page_size: int = 20
    search_term: str = ""
    
    # Filtros Globales
    empresa: List[str] = []
    regional: List[str] = []
    zona: List[str] = []
    franja: List[str] = []
    call_center: List[str] = []
    novedades: List[str] = []
    
    # Filtros Locales (Seguimientos)
    estado_pago: Optional[List[str]] = None      
    estado_gestion: Optional[List[str]] = None   
    cargos: Optional[List[str]] = None           
    cargos_excluidos: Optional[List[str]] = None
    
    rodamiento: List[str] = []
    origen: str = "cartera"