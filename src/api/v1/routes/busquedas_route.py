from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Any
import polars as pl
import os
import boto3 
from src.core.config import settings 

router = APIRouter()

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

# --- HELPER: DESCARGAR DE S3 ---
def garantizar_archivo_local(path_relativo: str):
    if os.path.exists(path_relativo): return True
    print(f"📥 Descargando {path_relativo} de S3...")
    try:
        os.makedirs(os.path.dirname(path_relativo), exist_ok=True)
        s3 = boto3.client('s3', region_name=settings.AWS_REGION, aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
        s3.download_file(settings.S3_BUCKET_NAME, path_relativo.replace("\\", "/"), path_relativo)
        return True
    except Exception as e:
        print(f"❌ Error S3: {e}")
        if os.path.exists(path_relativo): os.remove(path_relativo)
        return False

@router.post("/filtrar-tabla-detalle")
def filtrar_tabla_detalle(payload: FiltrosTabla):
    try:
        # 1. DETERMINAR RUTA
        if payload.origen == "novedades":
            parquet_path = f"data/novedades/{payload.job_id}.parquet"
        else:
            parquet_path = f"data/cartera/{payload.job_id}.parquet"

        # 2. VERIFICAR EXISTENCIA
        if not garantizar_archivo_local(parquet_path):
            # Fallback a ruta vieja
            parquet_path = f"data/{payload.job_id}.parquet"
            if not garantizar_archivo_local(parquet_path):
                return {"data": [], "total_registros": 0, "pagina_actual": 1, "total_paginas": 0}

        # 3. LEER EL PARQUET
        df = pl.read_parquet(parquet_path, memory_map=True)
        
        # 4. APLICAR BÚSQUEDA TEXTO
        if payload.search_term:
            term = payload.search_term.lower()
            cols_busqueda = [c for c in ["Nombre_Cliente", "Cedula_Cliente", "Credito", "Cargo_Usuario", "Novedad"] if c in df.columns]
            if cols_busqueda:
                filtro_texto = pl.lit(False)
                for col in cols_busqueda:
                    filtro_texto = filtro_texto | pl.col(col).str.to_lowercase().str.contains(term)
                df = df.filter(filtro_texto)

        # 5. APLICAR FILTROS GLOBALES Y LOCALES
        condicion = pl.lit(True)

        # A. EMPRESA
        if payload.empresa and "Empresa" in df.columns:
            condicion = condicion & pl.col("Empresa").is_in(payload.empresa)

        # B. REGIONAL (Soporte para ambos nombres)
        if payload.regional:
            if "Regional_Cobro" in df.columns:
                condicion = condicion & pl.col("Regional_Cobro").is_in(payload.regional)
            elif "Regional" in df.columns:
                condicion = condicion & pl.col("Regional").is_in(payload.regional)

        # C. ZONA
        if payload.zona and "Zona" in df.columns:
            condicion = condicion & pl.col("Zona").is_in(payload.zona)

        # D. FRANJA (Soporte para ambos nombres)
        if payload.franja:
            if "Franja_Cartera" in df.columns:
                condicion = condicion & pl.col("Franja_Cartera").is_in(payload.franja)
            elif "Franja" in df.columns:
                condicion = condicion & pl.col("Franja").is_in(payload.franja)

        # E. CALL CENTER
        if payload.call_center:
            # A veces viene como 'Call_Center', a veces como 'Nombre_Call_Center'
            col_cc = next((c for c in ["Call_Center", "Nombre_Call_Center", "CallCenter"] if c in df.columns), None)
            if col_cc:
                condicion = condicion & pl.col(col_cc).is_in(payload.call_center)

        # F. NOVEDADES (Nuevo Filtro)
        if payload.novedades:
            # Busca en 'Novedad' o 'Tipo_Novedad'
            col_nov = next((c for c in ["Novedad", "Tipo_Novedad", "Ultima_Novedad"] if c in df.columns), None)
            if col_nov:
                condicion = condicion & pl.col(col_nov).is_in(payload.novedades)

        # --- B. FILTROS LOCALES (SEGUIMIENTOS) ---
        
        if payload.estado_pago is not None and "Estado_Pago" in df.columns:
            condicion = condicion & pl.col("Estado_Pago").is_in(payload.estado_pago)
            
        if payload.estado_gestion is not None and "Estado_Gestion" in df.columns:
            condicion = condicion & pl.col("Estado_Gestion").is_in(payload.estado_gestion)
            
        if payload.cargos is not None and "Cargo_Usuario" in df.columns:
            if "SIN ASIGNAR" in payload.cargos:
                condicion = condicion & (
                    pl.col("Cargo_Usuario").is_in(payload.cargos) | 
                    pl.col("Cargo_Usuario").is_null() |
                    (pl.col("Cargo_Usuario") == "")
                )
            else:
                condicion = condicion & pl.col("Cargo_Usuario").is_in(payload.cargos)

        # Rodamientos
        if payload.rodamiento and "Rodamiento" in df.columns:
            condicion = condicion & pl.col("Rodamiento").is_in(payload.rodamiento)

        # APLICAR TODO EL FILTRADO
        df_filtrado = df.filter(condicion)

        # 6. PAGINACIÓN
        total_registros = df_filtrado.height
        if total_registros == 0:
            return {"data": [], "total_registros": 0, "pagina_actual": 1, "total_paginas": 0}

        total_paginas = (total_registros + payload.page_size - 1) // payload.page_size
        pagina_actual = max(1, min(payload.page, total_paginas))
        offset = (pagina_actual - 1) * payload.page_size
        
        data_pagina = df_filtrado.slice(offset, payload.page_size)
        
        return {
            "total_registros": total_registros,
            "pagina_actual": pagina_actual,
            "total_paginas": total_paginas,
            "data": data_pagina.to_dicts()
        }

    except Exception as e:
        print(f"❌ ERROR EN BUSCADOR: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))