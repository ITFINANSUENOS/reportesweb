from fastapi import APIRouter, HTTPException
import polars as pl
import os
import boto3
from src.core.config import settings
from src.schemas.request_models import FiltrosTabla
from src.schemas.response_models import BusquedaResponse

router = APIRouter()

# --- Funciones Helpers (Caché y S3) ---
def limpiar_cache_antigua(directorio: str = "temp", max_archivos: int = 10):
    try:
        if not os.path.exists(directorio): return
        archivos = [os.path.join(directorio, f) for f in os.listdir(directorio) if f.endswith('.parquet')]
        if len(archivos) > max_archivos:
            archivos.sort(key=os.path.getmtime)
            for f in archivos[:len(archivos) - max_archivos]:
                try:
                    os.remove(f)
                    print(f"🧹 Limpieza automática: Eliminado {f}")
                except OSError:
                    pass
    except Exception as e:
        print(f"⚠️ Warning limpieza caché: {e}")

def garantizar_archivo_local(s3_key: str, local_path: str):
    if os.path.exists(local_path): return True
    print(f"📥 Descargando {s3_key} de S3 a {local_path}...")
    try:
        directory = os.path.dirname(local_path)
        if directory: os.makedirs(directory, exist_ok=True)
        s3 = boto3.client(
            's3', region_name=settings.AWS_REGION, 
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID, 
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
        )
        s3.download_file(settings.S3_BUCKET_NAME, s3_key, local_path)
        return True
    except Exception as e:
        print(f"❌ Error S3 ({s3_key}): {e}")
        if os.path.exists(local_path): os.remove(local_path)
        return False

# --- Endpoint ---
@router.post("/filtrar-tabla-detalle", response_model=BusquedaResponse)
def filtrar_tabla_detalle(payload: FiltrosTabla):
    try:
        limpiar_cache_antigua(directorio="temp", max_archivos=20)

        job_id = payload.job_id
        origen = payload.origen
        
        MAPA_RUTAS = {
            "seguimientos_gestion": "data/seguimientos_gestion",
            "seguimientos_rodamientos": "data/seguimientos_rodamientos",
            "novedades": "data/seguimientos_gestion",
            "cartera": "data/seguimientos_rodamientos"
        }
        
        carpeta_s3 = MAPA_RUTAS.get(origen, f"data/{origen}")
        s3_key = f"{carpeta_s3}/{job_id}.parquet"
        
        nombre_archivo = f"search_{job_id}_{origen}.parquet"
        local_path = os.path.join("temp", nombre_archivo)

        if not garantizar_archivo_local(s3_key, local_path):
            fallback_key = f"data/{origen}/{job_id}.parquet"
            if not garantizar_archivo_local(fallback_key, local_path):
                 return {"data": [], "total_registros": 0, "pagina_actual": 1, "total_paginas": 0}

        try:
            df = pl.read_parquet(local_path, memory_map=True)
        except Exception as e:
            print(f"❌ Archivo corrupto: {e}")
            if os.path.exists(local_path): os.remove(local_path)
            return {"data": [], "total_registros": 0, "pagina_actual": 1, "total_paginas": 0}
        
        # Búsqueda Texto
        if payload.search_term:
            term = payload.search_term.lower()
            cols_busqueda = [c for c in ["Nombre_Cliente", "Cedula_Cliente", "Credito", "Cargo_Usuario", "Novedad", "Empresa"] if c in df.columns]
            if cols_busqueda:
                filtro_texto = pl.lit(False)
                for col in cols_busqueda:
                    filtro_texto = filtro_texto | pl.col(col).cast(pl.Utf8).str.to_lowercase().str.contains(term)
                df = df.filter(filtro_texto)

        # Filtros Dinámicos
        condicion = pl.lit(True)
        filtros_map = {
            "Empresa": payload.empresa, "Zona": payload.zona, "Regional_Cobro": payload.regional,
            "Regional": payload.regional, "Franja_Cartera": payload.franja, "Franja": payload.franja,
            "CALL_CENTER_FILTRO": payload.call_center, "Call_Center": payload.call_center,
            "Novedad": payload.novedades, "Tipo_Novedad": payload.novedades
        }

        for col_name, valores in filtros_map.items():
            if valores and col_name in df.columns:
                condicion = condicion & pl.col(col_name).is_in(valores)

        if payload.estado_pago and "Estado_Pago" in df.columns:
            condicion = condicion & pl.col("Estado_Pago").is_in(payload.estado_pago)
        if payload.estado_gestion and "Estado_Gestion" in df.columns:
            condicion = condicion & pl.col("Estado_Gestion").is_in(payload.estado_gestion)
        if payload.rodamiento and "Rodamiento" in df.columns:
            condicion = condicion & pl.col("Rodamiento").is_in(payload.rodamiento)
            
        if payload.cargos and "Cargo_Usuario" in df.columns:
            if "SIN ASIGNAR" in payload.cargos:
                condicion = condicion & (pl.col("Cargo_Usuario").is_in(payload.cargos) | pl.col("Cargo_Usuario").is_null() | (pl.col("Cargo_Usuario") == ""))
            else:
                condicion = condicion & pl.col("Cargo_Usuario").is_in(payload.cargos)

        df_filtrado = df.filter(condicion)

        # Paginación
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
        print(f"❌ ERROR BUSCADOR: {str(e)}")
        import traceback
        traceback.print_exc() 
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/consultar-clientes")
def consultar_clientes(
    job_id: str,
    q: str,
    limit: int = 20,
    origen: str = "cartera"
):
    try:
        # 1. Definimos rutas (Misma lógica que arriba)
        MAPA_RUTAS = {
            "seguimientos_gestion": "data/seguimientos_gestion",
            "seguimientos_rodamientos": "data/seguimientos_rodamientos",
            "novedades": "data/seguimientos_gestion",
            "cartera": "data/seguimientos_rodamientos"
        }
        carpeta_s3 = MAPA_RUTAS.get(origen, f"data/{origen}")
        s3_key = f"{carpeta_s3}/{job_id}.parquet"
        nombre_archivo = f"search_{job_id}_{origen}.parquet"
        local_path = os.path.join("temp", nombre_archivo)

        # 2. Descargamos (Si el POST ya lo bajó, esto no tarda nada)
        if not garantizar_archivo_local(s3_key, local_path):
             # Fallback
             fallback_key = f"data/{origen}/{job_id}.parquet"
             if not garantizar_archivo_local(fallback_key, local_path):
                 return []

        # 3. Lectura ULTRA RÁPIDA (Solo columnas necesarias)
        try:
            # Solo leemos Nombre, Cédula y Crédito para que sea veloz
            cols_necesarias = ["Nombre_Cliente", "Cedula_Cliente", "Credito"]
            
            # Truco: Ver qué columnas existen antes de leer
            esquema = pl.scan_parquet(local_path).collect_schema()
            cols_existentes = [c for c in cols_necesarias if c in esquema.names()]
            
            if not cols_existentes: return []

            # memory_map=True es clave para velocidad
            df = pl.read_parquet(local_path, columns=cols_existentes, memory_map=True)
            
        except Exception:
            return []

        # 4. Buscamos (Nombre O Cédula O Crédito)
        term = q.lower()
        filtro = pl.lit(False)
        
        if "Nombre_Cliente" in df.columns:
            filtro = filtro | pl.col("Nombre_Cliente").cast(pl.Utf8).str.to_lowercase().str.contains(term)
        if "Cedula_Cliente" in df.columns:
            filtro = filtro | pl.col("Cedula_Cliente").cast(pl.Utf8).str.contains(term)
        if "Credito" in df.columns:
             filtro = filtro | pl.col("Credito").cast(pl.Utf8).str.contains(term)

        # 5. Retornamos solo los primeros 'limit' resultados
        return df.filter(filtro).head(limit).to_dicts()

    except Exception as e:
        print(f"❌ Error buscador rápido: {e}")
        return []    