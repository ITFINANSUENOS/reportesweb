from fastapi import APIRouter, HTTPException, Query, Body
from pydantic import BaseModel
from typing import List, Optional
import polars as pl
import boto3
import os
from src.core.config import settings

router = APIRouter()

s3_client = boto3.client(
    's3', 
    region_name=settings.AWS_REGION, 
    aws_access_key_id=settings.AWS_ACCESS_KEY_ID, 
    aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
)

CACHE_DATAFRAMES = {}

# --- MODELO ACTUALIZADO CON SEARCH_TERM ---
class FiltrosTabla(BaseModel):
    job_id: str
    empresa: Optional[List[str]] = []
    regional: Optional[List[str]] = []
    zona: Optional[List[str]] = []
    franja: Optional[List[str]] = []
    call_center: Optional[List[str]] = []
    page: int = 1
    page_size: int = 50
    search_term: Optional[str] = ""  # <--- NUEVO CAMPO PARA EL BUSCADOR

@router.post("/filtrar-tabla-detalle")
def filtrar_tabla_detalle(payload: FiltrosTabla):
    """
    Filtra, Busca y Pagina los datos del Parquet en memoria.
    """
    global CACHE_DATAFRAMES
    
    # 1. CARGAR DATOS (Igual que antes)
    if payload.job_id not in CACHE_DATAFRAMES:
        print(f"📥 Cache Miss Tabla: Descargando Job {payload.job_id}...")
        local_parquet = f"temp_search_{payload.job_id}.parquet"
        s3_key = f"data/{payload.job_id}.parquet"
        try:
            s3_client.download_file(settings.S3_BUCKET_NAME, s3_key, local_parquet)
            df = pl.read_parquet(local_parquet)
            CACHE_DATAFRAMES[payload.job_id] = df
            if os.path.exists(local_parquet): os.remove(local_parquet)
        except Exception as e:
            raise HTTPException(status_code=404, detail="Datos no encontrados.")
    
    df = CACHE_DATAFRAMES[payload.job_id]
    
    # 2. CONSTRUIR FILTROS CATEGÓRICOS (Igual que antes)
    condicion = pl.lit(True)
    
    if payload.empresa: condicion = condicion & pl.col("Empresa").is_in(payload.empresa)
    
    if payload.regional:
        if "Regional_Cobro" in df.columns: condicion = condicion & pl.col("Regional_Cobro").is_in(payload.regional)
        elif "Regional_Venta" in df.columns: condicion = condicion & pl.col("Regional_Venta").is_in(payload.regional)

    if payload.zona: condicion = condicion & pl.col("Zona").is_in(payload.zona)
    if payload.franja: condicion = condicion & pl.col("Franja_Cartera").is_in(payload.franja)
    if payload.call_center and "CALL_CENTER_FILTRO" in df.columns:
        condicion = condicion & pl.col("CALL_CENTER_FILTRO").is_in(payload.call_center)

    # 3. FILTRO DE BÚSQUEDA (GLOBAL)
    if payload.search_term and payload.search_term.strip():
        term = payload.search_term.strip().upper()
        # Búsqueda insensible a mayúsculas/minúsculas en múltiples columnas
        search_condition = (
            pl.col("Nombre_Cliente").fill_null("").str.to_uppercase().str.contains(term) |
            pl.col("Cedula_Cliente").fill_null("").str.contains(term) |
            pl.col("Credito").fill_null("").str.contains(term) |
            pl.col("Celular").fill_null("").str.contains(term)
        )
        condicion = condicion & search_condition

    # 4. APLICAR FILTROS
    df_filtrado = df.filter(condicion)
    
    # 5. PAGINACIÓN (Matemática Crítica)
    total_registros = df_filtrado.height
    total_paginas = (total_registros + payload.page_size - 1) // payload.page_size # División entera techo
    
    # Validar página actual
    pagina_actual = max(1, min(payload.page, total_paginas)) if total_paginas > 0 else 1
    offset = (pagina_actual - 1) * payload.page_size
    
    # Extraer slice
    data_pagina = df_filtrado.slice(offset, payload.page_size)
    
    print(f"🔍 Filtro: {payload.search_term} | Total: {total_registros} | Pág: {pagina_actual}/{total_paginas}")

    return {
        "total_registros": total_registros,
        "pagina_actual": pagina_actual,
        "total_paginas": total_paginas,
        "data": data_pagina.to_dicts()
    }