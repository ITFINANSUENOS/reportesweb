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
def garantizar_archivo_local(s3_key: str, local_path: str):
    """
    Descarga un archivo de S3 si no existe localmente.
    Retorna True si el archivo está listo para usarse.
    """
    if os.path.exists(local_path): return True
    
    print(f"📥 Descargando {s3_key} de S3 a {local_path}...")
    try:
        # --- CORRECCIÓN AQUÍ ---
        # Solo creamos directorios si local_path tiene carpetas (ej: "temp/archivo.parquet")
        # Si es solo "archivo.parquet", directory será "" y saltamos este paso.
        directory = os.path.dirname(local_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        s3 = boto3.client(
            's3', 
            region_name=settings.AWS_REGION, 
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID, 
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
        )
        s3.download_file(settings.S3_BUCKET_NAME, s3_key, local_path)
        return True
    except Exception as e:
        print(f"❌ Error S3 ({s3_key}): {e}")
        # Limpieza si quedó corrupto
        if os.path.exists(local_path): os.remove(local_path)
        return False

@router.post("/filtrar-tabla-detalle")
def filtrar_tabla_detalle(payload: FiltrosTabla):
    try:
        job_id = payload.job_id
        origen = payload.origen
        
        # 1. MAPEO INTELIGENTE DE RUTAS
        MAPA_RUTAS = {
            "seguimientos_gestion": "data/seguimientos_gestion",
            "seguimientos_rodamientos": "data/seguimientos_rodamientos",
            "novedades": "data/seguimientos_gestion",
            "cartera": "data/seguimientos_rodamientos"
        }
        
        carpeta_s3 = MAPA_RUTAS.get(origen, f"data/{origen}")
        s3_key = f"{carpeta_s3}/{job_id}.parquet"
        
        # Nombre local
        local_path = f"temp_search_{job_id}_{origen}.parquet"

        # 2. VERIFICAR EXISTENCIA Y DESCARGAR
        if not garantizar_archivo_local(s3_key, local_path):
            print(f"⚠️ Ruta principal falló ({s3_key}), intentando fallback...")
            # Fallback a ruta vieja por si acaso
            fallback_key = f"data/{origen}/{job_id}.parquet"
            if not garantizar_archivo_local(fallback_key, local_path):
                 return {"data": [], "total_registros": 0, "pagina_actual": 1, "total_paginas": 0}

        # 3. LEER EL PARQUET
        try:
            # Usamos read_parquet por seguridad (memory_map=True es eficiente)
            df = pl.read_parquet(local_path, memory_map=True)
        except Exception as e:
            print(f"❌ Error leyendo Parquet corrupto: {e}")
            if os.path.exists(local_path): os.remove(local_path)
            return {"data": [], "total_registros": 0, "pagina_actual": 1, "total_paginas": 0}
        
        # 4. APLICAR BÚSQUEDA TEXTO
        if payload.search_term:
            term = payload.search_term.lower()
            cols_candidatas = ["Nombre_Cliente", "Cedula_Cliente", "Credito", "Cargo_Usuario", "Novedad", "Empresa"]
            cols_busqueda = [c for c in cols_candidatas if c in df.columns]
            
            if cols_busqueda:
                filtro_texto = pl.lit(False)
                for col in cols_busqueda:
                    filtro_texto = filtro_texto | pl.col(col).cast(pl.Utf8).str.to_lowercase().str.contains(term)
                df = df.filter(filtro_texto)

        # 5. CONSTRUCCIÓN DE FILTROS DINÁMICOS
        condicion = pl.lit(True)

        # --- A. FILTROS GLOBALES ---
        filtros_map = {
            "Empresa": payload.empresa,
            "Zona": payload.zona,
            "Regional_Cobro": payload.regional,
            "Regional": payload.regional,
            "Franja_Cartera": payload.franja,
            "Franja": payload.franja,
            "CALL_CENTER_FILTRO": payload.call_center,
            "Call_Center": payload.call_center,
            "Novedad": payload.novedades,
            "Tipo_Novedad": payload.novedades
        }

        for col_name, valores in filtros_map.items():
            if valores and col_name in df.columns:
                condicion = condicion & pl.col(col_name).is_in(valores)

        # --- B. FILTROS LOCALES ---
        if payload.estado_pago and "Estado_Pago" in df.columns:
            condicion = condicion & pl.col("Estado_Pago").is_in(payload.estado_pago)
            
        if payload.estado_gestion and "Estado_Gestion" in df.columns:
            condicion = condicion & pl.col("Estado_Gestion").is_in(payload.estado_gestion)
            
        if payload.cargos and "Cargo_Usuario" in df.columns:
            if "SIN ASIGNAR" in payload.cargos:
                condicion = condicion & (
                    pl.col("Cargo_Usuario").is_in(payload.cargos) | 
                    pl.col("Cargo_Usuario").is_null() |
                    (pl.col("Cargo_Usuario") == "")
                )
            else:
                condicion = condicion & pl.col("Cargo_Usuario").is_in(payload.cargos)

        if payload.rodamiento and "Rodamiento" in df.columns:
            condicion = condicion & pl.col("Rodamiento").is_in(payload.rodamiento)

        # 6. APLICAR FILTRADO FINAL
        df_filtrado = df.filter(condicion)

        # 7. PAGINACIÓN
        total_registros = df_filtrado.height
        
        if total_registros == 0:
            return {"data": [], "total_registros": 0, "pagina_actual": 1, "total_paginas": 0}

        total_paginas = (total_registros + payload.page_size - 1) // payload.page_size
        pagina_actual = max(1, min(payload.page, total_paginas))
        offset = (pagina_actual - 1) * payload.page_size
        
        data_pagina = df_filtrado.slice(offset, payload.page_size)
        resultado_final = data_pagina.to_dicts()

        return {
            "total_registros": total_registros,
            "pagina_actual": pagina_actual,
            "total_paginas": total_paginas,
            "data": resultado_final
        }

    except Exception as e:
        print(f"❌ ERROR EN BUSCADOR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))