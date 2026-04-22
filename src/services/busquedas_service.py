import polars as pl
import os
import io
import logging
from src.core.config import settings
from src.services.storage.s3_service import S3Service

class BusquedasService:
    def __init__(self):
        self.MAPA_RUTAS = {
            "seguimientos_gestion": "data/seguimientos_gestion",
            "seguimientos_rodamientos": "data/seguimientos_rodamientos",
            "detallados_cartera": "data/detallados_cartera",
            "detallados_novedades": "data/detallados_novedades",
            "detallados_call_center": "data/detallados_call_center",
            "novedades": "data/seguimientos_gestion",
            "cartera": "data/seguimientos_rodamientos",
            "comercial_fnz": "data/comercial",
            "comercial_retanqueos": "data/comercial"
        }
        # Instanciamos el servicio de S3
        self.s3_service = S3Service()

    # --- UTILIDADES INTERNAS ---
    def _limpiar_cache_antigua(self, directorio: str = "temp", max_archivos: int = 10):
        try:
            if not os.path.exists(directorio): return
            archivos = [os.path.join(directorio, f) for f in os.listdir(directorio) if f.endswith('.parquet')]
            if len(archivos) > max_archivos:
                archivos.sort(key=os.path.getmtime)
                for f in archivos[:len(archivos) - max_archivos]:
                    try: os.remove(f)
                    except OSError: pass
        except Exception as e:
            logging.warning(f"⚠️ Warning limpieza caché: {e}")

    def _garantizar_archivo_local(self, s3_key: str, local_path: str) -> bool:
        """Usa el S3Service para descargar, evitando la fuga de abstracción."""
        if os.path.exists(local_path): return True
        try:
            # Delegamos toda la lógica al S3Service
            ruta = self.s3_service.descargar_archivo(s3_key, local_path)
            # Si ruta es un string válido, fue exitoso. Si es vacío ("") o None, falló.
            return bool(ruta)
        except Exception as e:
            logging.error(f"❌ Error garantizando archivo ({s3_key}): {e}", exc_info=True)
            if os.path.exists(local_path): os.remove(local_path)
            return False

    def _resolver_rutas(self, origen: str, job_id: str):
        """Resuelve de qué carpeta de S3 y qué archivo se debe descargar."""
        carpeta_s3 = f"data/{origen}"
        nombre_archivo_s3 = f"{job_id}.parquet"

        if origen == "comercial_fnz":
            carpeta_s3 = "data/comercial"
            nombre_archivo_s3 = f"fnz_{job_id}.parquet"
        elif origen == "comercial_retanqueos":
            carpeta_s3 = "data/comercial"
            nombre_archivo_s3 = f"retanqueos_{job_id}.parquet"
        elif origen.startswith("comercial_cosechas"):
            carpeta_s3 = "data/comercial"
            nombre_archivo_s3 = f"cosechas_{job_id}.parquet"
        elif origen in self.MAPA_RUTAS:
            carpeta_s3 = self.MAPA_RUTAS[origen]

        s3_key = f"{carpeta_s3}/{nombre_archivo_s3}"
        local_path = os.path.join("temp", f"search_{job_id}_{origen}.parquet")
        
        return s3_key, local_path

    def _aplicar_filtros_comunes(self, lf: pl.LazyFrame, payload) -> pl.LazyFrame:
        """Aplica la lógica de filtrado dinámico sobre un LazyFrame (Optimizado en Memoria)."""
        origen = payload.origen
        
        # Obtenemos las columnas disponibles sin cargar datos
        columnas = lf.collect_schema().names()
        
        # Filtros de Cosechas
        if origen == "comercial_cosechas_s1": lf = lf.filter(pl.col("Grupo_Seguimiento") == "SECCION_1_SIN_PAGO")
        elif origen == "comercial_cosechas_s2": lf = lf.filter(pl.col("Grupo_Seguimiento") == "SECCION_2_FALLO_2DA")
        elif origen == "comercial_cosechas_s3": lf = lf.filter(pl.col("Grupo_Seguimiento") == "SECCION_3_FALLO_3RA_PLUS")
        
        # Filtro Texto
        if payload.search_term:
            term = payload.search_term.lower()
            posibles_cols = ["Nombre_Cliente", "Cedula_Cliente", "Credito", "Cargo_Usuario", "Novedad", "Empresa", "Nombre_Vendedor", "Regional_Venta"]
            cols_busqueda = [c for c in posibles_cols if c in columnas]
            if cols_busqueda:
                filtro_texto = pl.lit(False)
                for col in cols_busqueda:
                    filtro_texto = filtro_texto | pl.col(col).cast(pl.Utf8).str.to_lowercase().str.contains(term)
                lf = lf.filter(filtro_texto)

        # Filtros Dinámicos
        condicion = pl.lit(True)
        filtros_map = {
            "Empresa": payload.empresa, "Zona": payload.zona, "Regional_Cobro": payload.regional,
            "Regional": payload.regional, "Regional_Venta": payload.regional,
            "Franja_Cartera": payload.franja, "Franja": payload.franja,
            "CALL_CENTER_FILTRO": payload.call_center, "Call_Center": payload.call_center,
            "Tipo_Novedad": payload.novedades if all(n in ["COMPROMISO", "AUSENCIA", "PAGO", "NOVEDAD"] for n in payload.novedades) else None
        }
        for col_name, valores in filtros_map.items():
            if valores and col_name in columnas:
                condicion = condicion & pl.col(col_name).is_in(valores)

        # Filtro de Novedades
        if payload.novedades and len(payload.novedades) > 0:
            if "Cantidad_Novedades" in columnas:
                if "Con Novedades" in payload.novedades and "Sin Novedades" not in payload.novedades:
                    condicion = condicion & (pl.col("Cantidad_Novedades") > 0)
                elif "Sin Novedades" in payload.novedades and "Con Novedades" not in payload.novedades:
                    condicion = condicion & (pl.col("Cantidad_Novedades") == 0)
            elif "Estado_Gestion" in columnas:
                if "Con Novedades" in payload.novedades and "Sin Novedades" not in payload.novedades:
                    condicion = condicion & (pl.col("Estado_Gestion") == "CON GESTIÓN")
                elif "Sin Novedades" in payload.novedades and "Con Novedades" not in payload.novedades:
                    condicion = condicion & (pl.col("Estado_Gestion") == "SIN GESTIÓN")
        
        if payload.estado_pago and "Estado_Pago" in columnas: condicion = condicion & pl.col("Estado_Pago").is_in(payload.estado_pago)
        if payload.estado_gestion and "Estado_Gestion" in columnas: condicion = condicion & pl.col("Estado_Gestion").is_in(payload.estado_gestion)
        if payload.rodamiento and "Rodamiento" in columnas: condicion = condicion & pl.col("Rodamiento").is_in(payload.rodamiento)
        if payload.Regional_Venta and "Regional_Venta" in columnas: condicion = condicion & pl.col("Regional_Venta").is_in(payload.Regional_Venta)
        if payload.Vendedor_Activo and "Vendedor_Activo" in columnas: condicion = condicion & pl.col("Vendedor_Activo").is_in(payload.Vendedor_Activo)
        if payload.Nombre_Vendedor and "Nombre_Vendedor" in columnas: condicion = condicion & pl.col("Nombre_Vendedor").is_in(payload.Nombre_Vendedor)
        
        # Filtrar por vigencia - Cruce con cartera optimizado
        if payload.vigencia and len(payload.vigencia) > 0 and "Cedula_Cliente" in columnas:
            origen_cartera = "seguimientos_rodamientos"
            if origen == "detallados_call_center": origen_cartera = "detallados_call_center"
            elif origen == "detallados_cartera": origen_cartera = "detallados_cartera"
            
            try:
                s3_key_cartera, local_path_cartera = self._resolver_rutas(origen_cartera, payload.job_id)
                if self._garantizar_archivo_local(s3_key_cartera, local_path_cartera):
                    # Usamos scan_parquet para no cargar toda la cartera
                    df_cartera = pl.scan_parquet(local_path_cartera).select(["Cedula_Cliente", "Estado_Vigencia"]).collect()
                    if "Estado_Vigencia" in df_cartera.columns:
                        cedulas_vigencia = df_cartera.filter(pl.col("Estado_Vigencia").is_in(payload.vigencia))["Cedula_Cliente"].unique()
                        condicion = condicion & pl.col("Cedula_Cliente").is_in(cedulas_vigencia)
            except Exception as e:
                logging.warning(f"⚠️ Warning: No se pudo aplicar filtro vigencia: {e}")

        if payload.cargos and "Cargo_Usuario" in columnas:
             if "SIN ASIGNAR" in payload.cargos:
                condicion = condicion & (pl.col("Cargo_Usuario").is_in(payload.cargos) | pl.col("Cargo_Usuario").is_null() | (pl.col("Cargo_Usuario") == ""))
             else:
                condicion = condicion & pl.col("Cargo_Usuario").is_in(payload.cargos)

        return lf.filter(condicion)

    # --- MÉTODOS PÚBLICOS DEL SERVICIO ---
    
    def filtrar_tabla(self, payload) -> dict:
        self._limpiar_cache_antigua(directorio="temp", max_archivos=20)
        s3_key, local_path = self._resolver_rutas(payload.origen, payload.job_id)

        if not self._garantizar_archivo_local(s3_key, local_path):
            return {"data": [], "total_registros": 0, "pagina_actual": 1, "total_paginas": 0}

        try:
            lf = pl.scan_parquet(local_path)
            lf_filtrado = self._aplicar_filtros_comunes(lf, payload)

            # Materializamos a un DataFrame solo con los datos ya filtrados
            df_filtrado = lf_filtrado.collect()

            total_registros = df_filtrado.height
            if total_registros == 0: return {"data": [], "total_registros": 0, "pagina_actual": 1, "total_paginas": 0}

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
            logging.error(f"Error procesando filtrado: {e}", exc_info=True)
            raise Exception(f"Error procesando filtrado: {str(e)}")

    def exportar_excel(self, payload) -> io.BytesIO:
        s3_key, local_path = self._resolver_rutas(payload.origen, payload.job_id)
        
        if not self._garantizar_archivo_local(s3_key, local_path):
            raise FileNotFoundError("Archivo no encontrado para exportar")

        #  Evaluación perezosa para exportaciones masivas
        lf = pl.scan_parquet(local_path)
        lf_filtrado = self._aplicar_filtros_comunes(lf, payload)

        if hasattr(payload, 'columnas_visibles') and payload.columnas_visibles:
            cols_validas = [col for col in payload.columnas_visibles if col in lf_filtrado.collect_schema().names()]
            if cols_validas:
                lf_filtrado = lf_filtrado.select(cols_validas)

        df_final = lf_filtrado.collect()

        buffer = io.BytesIO()
        df_final.write_excel(buffer)
        buffer.seek(0)
        return buffer

    def consultar_clientes(self, job_id: str, q: str, limit: int, origen: str) -> list:
        s3_key, local_path = self._resolver_rutas(origen, job_id)
        if not self._garantizar_archivo_local(s3_key, local_path): return []

        try:
            cols_necesarias = ["Nombre_Cliente", "Cedula_Cliente", "Credito"]
            esquema = pl.scan_parquet(local_path).collect_schema()
            cols_existentes = [c for c in cols_necesarias if c in esquema.names()]
            if not cols_existentes: return []
            
            term = q.lower()
            filtro = pl.lit(False)
            if "Nombre_Cliente" in cols_existentes: filtro = filtro | pl.col("Nombre_Cliente").cast(pl.Utf8).str.to_lowercase().str.contains(term)
            if "Cedula_Cliente" in cols_existentes: filtro = filtro | pl.col("Cedula_Cliente").cast(pl.Utf8).str.contains(term)
            if "Credito" in cols_existentes: filtro = filtro | pl.col("Credito").cast(pl.Utf8).str.contains(term)

            df = pl.scan_parquet(local_path).select(cols_existentes).filter(filtro).head(limit).collect()
            return df.to_dicts()
        except Exception as e:
            logging.error(f"Error en consulta rápida: {e}")
            return []

    def consultar_relacionados(self, payload) -> list:
        s3_key, local_path = self._resolver_rutas(payload.origen_destino, payload.job_id)
        local_path = local_path.replace("search_", "rel_") 

        if not self._garantizar_archivo_local(s3_key, local_path): return []

        try:
            lf = pl.scan_parquet(local_path)
            valor = str(payload.valor_clave).strip()
            col = payload.columna_clave
            
            resultado = lf.filter(pl.col(col).cast(pl.Utf8).str.strip_chars() == valor).collect()
            
            if not resultado.is_empty():
                 resultado = resultado.with_columns(pl.col(pl.Date).dt.to_string("%Y-%m-%d"))
            return resultado.to_dicts()
        except Exception as e:
            logging.error(f"Error consultando relacionados: {e}")
            return []
            
    def descargar_dependencias_metricas(self, job_id: str):
        """Descarga todos los parquets necesarios para calcular métricas"""
        archivos = {
            "cartera": (f"data/detallados_call_center/{job_id}.parquet", os.path.join("temp", f"search_{job_id}_detallados_call_center.parquet")),
            "llamadas": (f"data/llamadas/{job_id}.parquet", os.path.join("temp", f"calls_{job_id}.parquet")),
            "mensajes": (f"data/mensajes/{job_id}.parquet", os.path.join("temp", f"msgs_{job_id}.parquet")),
            "novedades": (f"data/seguimientos_gestion/{job_id}.parquet", os.path.join("temp", f"novs_{job_id}.parquet"))
        }
        
        if not self._garantizar_archivo_local(archivos["cartera"][0], archivos["cartera"][1]):
            return None
            
        self._garantizar_archivo_local(archivos["llamadas"][0], archivos["llamadas"][1])
        self._garantizar_archivo_local(archivos["mensajes"][0], archivos["mensajes"][1])
        self._garantizar_archivo_local(archivos["novedades"][0], archivos["novedades"][1])
        
        return archivos