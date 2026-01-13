from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Any
import polars as pl
import os
import boto3
import time
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

# --- 1. FUNCIÓN DE LIMPIEZA (CRÍTICA PARA AWS) ---
def limpiar_cache_antigua(directorio: str = "temp", max_archivos: int = 10):
    """
    Mantiene la carpeta temporal limpia. Si hay más de 'max_archivos',
    borra los más viejos para liberar espacio en disco.
    """
    try:
        if not os.path.exists(directorio): return

        archivos = [
            os.path.join(directorio, f) 
            for f in os.listdir(directorio) 
            if f.endswith('.parquet')
        ]
        
        if len(archivos) > max_archivos:
            # Ordenar por fecha de modificación (el más viejo primero)
            archivos.sort(key=os.path.getmtime)
            
            # Borrar los sobrantes
            archivos_a_borrar = archivos[:len(archivos) - max_archivos]
            for f in archivos_a_borrar:
                try:
                    os.remove(f)
                    print(f"🧹 Limpieza automática: Eliminado {f}")
                except OSError:
                    pass
    except Exception as e:
        print(f"⚠️ Warning limpieza caché: {e}")

# --- 2. HELPER DE DESCARGA ---
def garantizar_archivo_local(s3_key: str, local_path: str):
    """Descarga de S3 a local si no existe."""
    if os.path.exists(local_path): return True
    
    print(f"📥 Descargando {s3_key} de S3 a {local_path}...")
    try:
        # Aseguramos que la carpeta (temp/) exista
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
        if os.path.exists(local_path): os.remove(local_path)
        return False

@router.post("/filtrar-tabla-detalle")
def filtrar_tabla_detalle(payload: FiltrosTabla):
    try:
        # A. Limpieza preventiva antes de empezar
        limpiar_cache_antigua(directorio="temp", max_archivos=20)

        job_id = payload.job_id
        origen = payload.origen
        
        # B. Mapeo de Rutas (Frontend -> S3)
        MAPA_RUTAS = {
            # Rutas nuevas
            "seguimientos_gestion": "data/seguimientos_gestion",
            "seguimientos_rodamientos": "data/seguimientos_rodamientos",
            # Compatibilidad
            "novedades": "data/seguimientos_gestion",
            "cartera": "data/seguimientos_rodamientos"
        }
        
        carpeta_s3 = MAPA_RUTAS.get(origen, f"data/{origen}")
        s3_key = f"{carpeta_s3}/{job_id}.parquet"
        
        # C. Definir ruta local en carpeta 'temp'
        # Usamos os.path.join para evitar problemas de rutas
        nombre_archivo = f"search_{job_id}_{origen}.parquet"
        local_path = os.path.join("temp", nombre_archivo)

        # D. Descargar (con reintento de fallback si falla la ruta nueva)
        if not garantizar_archivo_local(s3_key, local_path):
            print(f"⚠️ Ruta {s3_key} falló, intentando ruta legacy...")
            fallback_key = f"data/{origen}/{job_id}.parquet"
            if not garantizar_archivo_local(fallback_key, local_path):
                 return {"data": [], "total_registros": 0, "pagina_actual": 1, "total_paginas": 0}

        # E. Leer Parquet (Optimizado)
        try:
            # memory_map=True es excelente para velocidad sin saturar RAM
            df = pl.read_parquet(local_path, memory_map=True)
        except Exception as e:
            print(f"❌ Archivo corrupto, eliminando: {e}")
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

        # --- Filtros Globales ---
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

        # --- Filtros Locales ---
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
        # Importante para debug en CloudWatch
        import traceback
        traceback.print_exc() 
        raise HTTPException(status_code=500, detail=str(e))