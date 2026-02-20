from fastapi import APIRouter, HTTPException
import polars as pl
import os
import boto3
from src.core.config import settings
from src.schemas.request_models import FiltrosTabla, ConsultaRelacionada
from src.schemas.response_models import BusquedaResponse
from src.services.analytics.call_center import CallCenterAnalyticsService   

router = APIRouter()

# --- Funciones Helpers (Caché y S3) ---
def limpiar_cache_antigua(directorio: str = "temp", max_archivos: int = 10):
    try:
        if not os.path.exists(directorio): return
        archivos = [os.path.join(directorio, f) for f in os.listdir(directorio) if f.endswith('.parquet')]
        if len(archivos) > max_archivos:
            archivos.sort(key=os.path.getmtime)
            for f in archivos[:len(archivos) - max_archivos]:
                try: os.remove(f)
                except OSError: pass
    except Exception as e:
        print(f"⚠️ Warning limpieza caché: {e}")

def garantizar_archivo_local(s3_key: str, local_path: str):
    if os.path.exists(local_path): return True
    try:
        directory = os.path.dirname(local_path)
        if directory: os.makedirs(directory, exist_ok=True)
        s3 = boto3.client('s3', region_name=settings.AWS_REGION, aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
        s3.download_file(settings.S3_BUCKET_NAME, s3_key, local_path)
        return True
    except Exception as e:
        print(f"❌ Error S3 ({s3_key}): {e}")
        if os.path.exists(local_path): os.remove(local_path)
        return False

# --- Endpoint Principal ---
@router.post("/filtrar-tabla-detalle", response_model=BusquedaResponse)
def filtrar_tabla_detalle(payload: FiltrosTabla):
    try:
        limpiar_cache_antigua(directorio="temp", max_archivos=20)
        job_id = payload.job_id
        origen = payload.origen
        
        # 1. DEFINICIÓN DE RUTAS
        carpeta_s3 = f"data/{origen}"
        nombre_archivo_s3 = f"{job_id}.parquet"

        # A. Módulo COMERCIAL
        if origen == "comercial_fnz":
            carpeta_s3 = "data/comercial"
            nombre_archivo_s3 = f"fnz_{job_id}.parquet"
        elif origen == "comercial_retanqueos":
            carpeta_s3 = "data/comercial"
            nombre_archivo_s3 = f"retanqueos_{job_id}.parquet"
        elif origen.startswith("comercial_cosechas"): # Detecta s1, s2, s3
            carpeta_s3 = "data/comercial"
            nombre_archivo_s3 = f"cosechas_{job_id}.parquet"

        # B. Módulos TRADICIONALES
        else:
            MAPA_RUTAS = {
                "seguimientos_gestion": "data/seguimientos_gestion",
                "seguimientos_rodamientos": "data/seguimientos_rodamientos",
                "detallados_cartera": "data/detallados_cartera",
                "detallados_novedades": "data/detallados_novedades",
                "detallados_call_center": "data/detallados_call_center",
                "novedades": "data/seguimientos_gestion",
                "cartera": "data/seguimientos_rodamientos"
            }
            if origen in MAPA_RUTAS:
                carpeta_s3 = MAPA_RUTAS[origen]

        s3_key = f"{carpeta_s3}/{nombre_archivo_s3}"
        nombre_archivo_local = f"search_{job_id}_{origen}.parquet"
        local_path = os.path.join("temp", nombre_archivo_local)

        # 2. DESCARGA / CACHÉ
        if not garantizar_archivo_local(s3_key, local_path):
            if "detallados" in origen:
                fallback_origen = "seguimientos_rodamientos" if "cartera" in origen else "seguimientos_gestion"
                fallback_key = f"data/{fallback_origen}/{job_id}.parquet"
                if not garantizar_archivo_local(fallback_key, local_path):
                     return {"data": [], "total_registros": 0, "pagina_actual": 1, "total_paginas": 0}
            else:
                 return {"data": [], "total_registros": 0, "pagina_actual": 1, "total_paginas": 0}
             
        # 3. LECTURA Y FILTRADO (Polars)
        try:
            df = pl.read_parquet(local_path, memory_map=True)
            
            # --- FILTROS AUTOMÁTICOS POR SUB-ORIGEN (COSECHAS) ---
            if origen == "comercial_cosechas_s1":
                df = df.filter(pl.col("Grupo_Seguimiento") == "SECCION_1_SIN_PAGO")
            elif origen == "comercial_cosechas_s2":
                df = df.filter(pl.col("Grupo_Seguimiento") == "SECCION_2_FALLO_2DA")
            elif origen == "comercial_cosechas_s3":
                df = df.filter(pl.col("Grupo_Seguimiento") == "SECCION_3_FALLO_3RA_PLUS")
                
        except Exception:
            if os.path.exists(local_path): os.remove(local_path)
            return {"data": [], "total_registros": 0, "pagina_actual": 1, "total_paginas": 0}
        
        # A. Filtro Texto
        if payload.search_term:
            term = payload.search_term.lower()
            posibles_cols = ["Nombre_Cliente", "Cedula_Cliente", "Credito", "Cargo_Usuario", "Novedad", "Empresa", "Nombre_Vendedor", "Regional_Venta"]
            cols_busqueda = [c for c in posibles_cols if c in df.columns]
            if cols_busqueda:
                filtro_texto = pl.lit(False)
                for col in cols_busqueda:
                    filtro_texto = filtro_texto | pl.col(col).cast(pl.Utf8).str.to_lowercase().str.contains(term)
                df = df.filter(filtro_texto)

        # B. Filtros Dinámicos
        condicion = pl.lit(True)
        filtros_map = {
            "Empresa": payload.empresa, "Zona": payload.zona, "Regional_Cobro": payload.regional,
            "Regional": payload.regional, "Regional_Venta": payload.regional,
            "Franja_Cartera": payload.franja, "Franja": payload.franja,
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

        # 4. PAGINACIÓN
        total_registros = df_filtrado.height
        if total_registros == 0:
            return {"data": [], "total_registros": 0, "pagina_actual": 1, "total_paginas": 0}

        total_paginas = (total_registros + payload.page_size - 1) // payload.page_size
        pagina_actual = max(1, min(payload.page, total_paginas))
        offset = (pagina_actual - 1) * payload.page_size
        
        data_pagina = df_filtrado.slice(offset, payload.page_size)
        if not data_pagina.is_empty():
             data_pagina = data_pagina.with_columns(pl.col(pl.Date).dt.to_string("%Y-%m-%d"))

        return {
            "total_registros": total_registros,
            "pagina_actual": pagina_actual,
            "total_paginas": total_paginas,
            "data": data_pagina.to_dicts()
        }

    except Exception as e:
        import traceback
        traceback.print_exc() 
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/consultar-clientes")
def consultar_clientes(
    job_id: str, q: str, limit: int = 20, origen: str = "cartera"
):
    try:
        # Mapeo idéntico para el buscador rápido
        carpeta_s3 = f"data/{origen}"
        nombre_archivo_s3 = f"{job_id}.parquet"
        if origen == "comercial_fnz":
            carpeta_s3 = "data/comercial"
            nombre_archivo_s3 = f"fnz_{job_id}.parquet"
        elif origen == "comercial_retanqueos":
             carpeta_s3 = "data/comercial"
             nombre_archivo_s3 = f"retanqueos_{job_id}.parquet"
        elif origen == "detallados_cartera": 
             carpeta_s3 = "data/detallados_cartera"
        elif origen == "cartera":
             carpeta_s3 = "data/seguimientos_rodamientos"

        s3_key = f"{carpeta_s3}/{nombre_archivo_s3}"
        nombre_archivo = f"search_{job_id}_{origen}.parquet"
        local_path = os.path.join("temp", nombre_archivo)

        if not garantizar_archivo_local(s3_key, local_path): return []

        try:
            cols_necesarias = ["Nombre_Cliente", "Cedula_Cliente", "Credito"]
            esquema = pl.scan_parquet(local_path).collect_schema()
            cols_existentes = [c for c in cols_necesarias if c in esquema.names()]
            if not cols_existentes: return []
            df = pl.read_parquet(local_path, columns=cols_existentes, memory_map=True)
        except Exception:
            return []

        term = q.lower()
        filtro = pl.lit(False)
        if "Nombre_Cliente" in df.columns:
            filtro = filtro | pl.col("Nombre_Cliente").cast(pl.Utf8).str.to_lowercase().str.contains(term)
        if "Cedula_Cliente" in df.columns:
            filtro = filtro | pl.col("Cedula_Cliente").cast(pl.Utf8).str.contains(term)
        if "Credito" in df.columns:
             filtro = filtro | pl.col("Credito").cast(pl.Utf8).str.contains(term)

        return df.filter(filtro).head(limit).to_dicts()
    except Exception:
        return []
    
@router.post("/consultar-relacionados")
def consultar_relacionados(payload: ConsultaRelacionada):
    try:
        job_id = payload.job_id
        origen = payload.origen_destino
        
        # 1. Resolver ruta del archivo destino
        # Reutilizamos tu lógica de mapeo
        carpeta_s3 = f"data/{origen}"
        if origen == "detallados_cartera": 
             carpeta_s3 = "data/detallados_cartera"
        elif origen == "detallados_novedades": 
             carpeta_s3 = "data/detallados_novedades"
        # ... puedes agregar los comerciales si quisieras cruzar con ellos también
        
        nombre_archivo_s3 = f"{job_id}.parquet"
        s3_key = f"{carpeta_s3}/{nombre_archivo_s3}"
        
        # Usamos un nombre local distinto para no chocar con la búsqueda principal
        nombre_archivo_local = f"rel_{job_id}_{origen}.parquet"
        local_path = os.path.join("temp", nombre_archivo_local)

        # 2. Descargar si no existe
        # Nota: Aquí podríamos usar el mismo archivo de 'search_' si quisiéramos ahorrar espacio,
        # pero separarlos evita conflictos de lectura/escritura si hay concurrencia simple.
        if not garantizar_archivo_local(s3_key, local_path):
            return []

        # 3. Lectura y Filtrado Exacto
        try:
            # Lazy scan es más rápido para filtrar una sola cédula en un archivo grande
            lf = pl.scan_parquet(local_path)
            
            # Filtro: Columna == Valor
            # IMPORTANTE: Casteamos a String ambos lados para evitar errores de tipo (Int vs Str)
            valor = str(payload.valor_clave).strip()
            col = payload.columna_clave
            
            # Buscamos coincidencias exactas
            resultado = lf.filter(
                pl.col(col).cast(pl.Utf8).str.strip_chars() == valor
            ).collect()
            
            # Limpiamos fechas para JSON
            if not resultado.is_empty():
                 resultado = resultado.with_columns(pl.col(pl.Date).dt.to_string("%Y-%m-%d"))
            
            return resultado.to_dicts()

        except Exception as e:
            print(f"Error leyendo parquet relacionado: {e}")
            return []

    except Exception as e:
        print(f"Error endpoint relacionados: {e}")
        raise HTTPException(status_code=500, detail=str(e))  
    
@router.post("/metricas/call-center")
def obtener_metricas_call_center(payload: FiltrosTabla):
    try:
        job_id = payload.job_id
        
        # 1. Definir Rutas de Archivos Base
        # Usamos 'detallados_call_center' porque YA tiene la lógica de cascada aplicada
        path_cartera = os.path.join("temp", f"search_{job_id}_detallados_call_center.parquet")
        s3_cartera = f"data/detallados_call_center/{job_id}.parquet"
        
        path_llamadas = os.path.join("temp", f"calls_{job_id}.parquet")
        s3_llamadas = f"data/llamadas/{job_id}.parquet"
        
        path_mensajes = os.path.join("temp", f"msgs_{job_id}.parquet")
        s3_mensajes = f"data/mensajes/{job_id}.parquet"
        
        path_novedades = os.path.join("temp", f"novs_{job_id}.parquet")
        s3_novedades = f"data/seguimientos_gestion/{job_id}.parquet" # Novedades suele estar aquí

        # 2. Descargar si no existen
        if not garantizar_archivo_local(s3_cartera, path_cartera): return {}
        garantizar_archivo_local(s3_llamadas, path_llamadas)
        garantizar_archivo_local(s3_mensajes, path_mensajes)
        garantizar_archivo_local(s3_novedades, path_novedades)

        # 3. Cargar DataFrames
        df_cartera = pl.read_parquet(path_cartera)
        df_llamadas = pl.read_parquet(path_llamadas) if os.path.exists(path_llamadas) else pl.DataFrame()
        df_mensajes = pl.read_parquet(path_mensajes) if os.path.exists(path_mensajes) else pl.DataFrame()
        df_novedades = pl.read_parquet(path_novedades) if os.path.exists(path_novedades) else pl.DataFrame()

        # 4. APLICAR FILTROS GLOBALES A LA CARTERA
        condicion = pl.lit(True)
        filtros_map = {
            "Empresa": payload.empresa, "Zona": payload.zona, "Regional_Cobro": payload.regional,
            "Regional": payload.regional, "Franja_Cartera": payload.franja, 
            "CALL_CENTER_FILTRO": payload.call_center,
        }
        for col, val in filtros_map.items():
            if val and col in df_cartera.columns:
                condicion = condicion & pl.col(col).is_in(val)
        
        if payload.rodamiento and "Rodamiento" in df_cartera.columns:
            condicion = condicion & pl.col("Rodamiento").is_in(payload.rodamiento)

        df_cartera_filtrada = df_cartera.filter(condicion)

        # 5. FILTRAR AUXILIARES (Para que las gráficas de abajo coincidan con los filtros)
        if not df_cartera_filtrada.is_empty():
            # Intentamos filtrar llamadas/mensajes por las cédulas resultantes
            cedulas_filtradas = df_cartera_filtrada["Cedula_Cliente"].unique()
            
            if "Cedula_Cliente" in df_novedades.columns:
                df_novedades = df_novedades.filter(pl.col("Cedula_Cliente").is_in(cedulas_filtradas))
            
            # Si hay filtro explícito de Call Center, lo aplicamos a llamadas también
            if payload.call_center and "Call_Center" in df_llamadas.columns:
                 df_llamadas = df_llamadas.filter(pl.col("Call_Center").is_in(payload.call_center))

        # 6. RECALCULAR MÉTRICAS
        cc_service = CallCenterAnalyticsService()
        resultados = cc_service.calcular_metricas_call_center(
            df_cartera=df_cartera_filtrada,
            df_novedades=df_novedades,
            df_llamadas=df_llamadas,
            df_mensajeria=df_mensajes
        )
        
        # Eliminamos el parquet interno para no enviarlo por la red
        if "df_parquet_detalle" in resultados:
            del resultados["df_parquet_detalle"]

        return resultados

    except Exception as e:
        print(f"Error calculando métricas dinámicas: {e}")
        import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))      