import os
import traceback
import logging
import polars as pl
import concurrent.futures

from src.core.constants import (
    COLS_TABLA_NOVEDADES, COLS_TABLA_RODAMIENTOS,
    COLS_MASTER_CARTERA, COLS_MASTER_NOVEDADES)

from src.services.storage.s3_service import S3Service
from src.services.storage.excel_loader import ExcelLoaderService
from src.services.analytics.cartera import CarteraAnalyticsService
from src.services.analytics.seguimientos import SeguimientosAnalyticsService
from src.services.analytics.resultados import ResultadosAnalyticsService
from src.services.analytics.comercial import ComercialAnalyticsService
from src.services.analytics.call_center import CallCenterAnalyticsService

class ReportesOrchestrator:
    def __init__(self):
        self.storage = S3Service()
        self.loader = ExcelLoaderService()
        
        self.cartera_service = CarteraAnalyticsService()
        self.seguimientos_service = SeguimientosAnalyticsService()
        self.resultados_service = ResultadosAnalyticsService()
        self.comercial_service = ComercialAnalyticsService()
        self.cc_service = CallCenterAnalyticsService()

    def procesar_excel_multi_modulo(self, file_path: str, job_id: str, empresa: str) -> dict:
        resultados_modulos = {}
        logging.info("⏳ ORCHESTRATOR: Cargando hojas de Excel...")
        
        # 1. CARGA DE DATOS
        df_cartera = self.loader.cargar_cartera(file_path)
        df_novedades = self.loader.cargar_novedades(file_path)
        
        df_llamadas = self.loader.cargar_llamadas(file_path)
        if not df_llamadas.is_empty():
            self.storage.guardar_parquet(df_llamadas, f"data/llamadas/{job_id}.parquet")

        df_mensajeria = self.loader.cargar_mensajeria(file_path)
        if not df_mensajeria.is_empty():
            self.storage.guardar_parquet(df_mensajeria, f"data/mensajes/{job_id}.parquet")
            
        df_fnz = self.loader.cargar_fnz(file_path)
        if not df_fnz.is_empty():
            self.storage.guardar_parquet(df_fnz, f"data/fnz/{job_id}.parquet")

        # 2. PROCESAMIENTO ANALÍTICO
        df_cartera_save = None 
        df_novedades_save = None

        df_cartera = self.cartera_service.enriquecer_datos_base(df_cartera)

        if not df_cartera.is_empty():
            metadata_base = {"job_id": job_id, "empresa": empresa}

            # --- DEFINICIÓN DE TAREAS PARA LOS HILOS ---
            def tarea_cartera():
                try:
                    metrics = self.cartera_service.calcular_metricas_tablero_principal(df_cartera)
                    self.storage.guardar_json(metrics, f"graficos/cartera/{job_id}.json", {**metadata_base, "modulo": "cartera"})
                    return ("cartera", metrics, df_cartera)
                except Exception as e:
                    logging.error(f"❌ Error Cartera: {e}", exc_info=True)
                    return ("cartera", None, None)

            def tarea_seguimientos_y_resultados():
                # Resultados depende de Seguimientos, así que van en el mismo hilo secuencial
                try:
                    res_seg = self.seguimientos_service.calcular_metricas_seguimientos(df_cartera, df_novedades)
                    df_nov_full = res_seg.pop("_df_novedades_full", None)
                    df_cart_base = res_seg.pop("_df_cartera_base", df_cartera)
                    
                    self.storage.guardar_json(res_seg, f"graficos/seguimientos/{job_id}.json", {**metadata_base, "modulo": "seguimientos"})
                    
                    res_res = self.resultados_service.calcular_metricas_resultados(df_cart_base)
                    self.storage.guardar_json(res_res, f"graficos/resultados/{job_id}.json", {**metadata_base, "modulo": "resultados"})
                    
                    return ("seg_res", {"seguimientos": res_seg, "resultados": res_res}, df_cart_base, df_nov_full)
                except Exception as e:
                    logging.error(f"❌ Error Seguimientos/Resultados: {e}", exc_info=True)
                    return ("seg_res", None, None, None)

            def tarea_comercial():
                try:
                    logging.info("📊 Procesando Módulo Comercial...")
                    df_fnz_input = df_fnz if not df_fnz.is_empty() else None
                    df_ret = self.comercial_service.calcular_df_retanqueos(df_cartera)
                    df_cos = self.comercial_service.calcular_df_cosechas(df_cartera)

                    if df_fnz_input is not None: self.storage.guardar_parquet(df_fnz_input, f"data/comercial/fnz_{job_id}.parquet")
                    if df_ret is not None: self.storage.guardar_parquet(df_ret, f"data/comercial/retanqueos_{job_id}.parquet")
                    if df_cos is not None: self.storage.guardar_parquet(df_cos, f"data/comercial/cosechas_{job_id}.parquet")

                    json_comercial = self.comercial_service.generar_json_comercial(df_fnz=df_fnz_input, df_retanqueos=df_ret, df_cosechas=df_cos)
                    self.storage.guardar_json(json_comercial, f"graficos/comercial/{job_id}.json", {"job_id": job_id, "modulo": "comercial"})
                    return ("comercial", True)
                except Exception as e:
                    logging.error(f"❌ Error Comercial: {e}", exc_info=True)
                    return ("comercial", False)

            def tarea_call_center():
                try:
                    logging.info("📞 Procesando Módulo Call Center...")
                    res_cc = self.cc_service.calcular_metricas_call_center(
                        df_cartera=df_cartera, df_novedades=df_novedades,
                        df_llamadas=df_llamadas, df_mensajeria=df_mensajeria
                    )
                    if "df_parquet_detalle" in res_cc:
                        df_detalle_cc = res_cc.pop("df_parquet_detalle") 
                        self.storage.guardar_parquet(df_detalle_cc, f"data/detallados_call_center/{job_id}.parquet")

                    self.storage.guardar_json(res_cc, f"graficos/call_center/{job_id}.json", {"job_id": job_id, "modulo": "call_center", "empresa": empresa})
                    return ("call_center", True)
                except Exception as e:
                    logging.error(f"❌ Error Call Center: {e}", exc_info=True)
                    return ("call_center", False)

            # --- EJECUCIÓN MULTIHILO ---
            logging.info("🚀 Iniciando procesamiento paralelo de módulos...")
            
            # Lanzamos las 4 tareas al mismo tiempo
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                futuros = [
                    executor.submit(tarea_cartera),
                    executor.submit(tarea_seguimientos_y_resultados),
                    executor.submit(tarea_comercial),
                    executor.submit(tarea_call_center)
                ]
                
                # Recogemos los resultados conforme van terminando
                for futuro in concurrent.futures.as_completed(futuros):
                    resultado = futuro.result()
                    nombre_tarea = resultado[0]
                    
                    if nombre_tarea == "cartera" and resultado[1]:
                        resultados_modulos["cartera"] = resultado[1]
                        df_cartera_save = resultado[2]
                    elif nombre_tarea == "seg_res" and resultado[1]:
                        resultados_modulos["seguimientos"] = resultado[1]["seguimientos"]
                        resultados_modulos["resultados"] = resultado[1]["resultados"]
                        df_cartera_save = resultado[2]  # Actualizamos con las modificaciones de seguimientos
                        df_novedades_save = resultado[3]
                    elif nombre_tarea in ["comercial", "call_center"] and resultado[1]:
                        resultados_modulos[nombre_tarea] = True

        # 3. GUARDADO FINAL DE TABLAS MASTER
        if df_cartera_save is not None:
            path_rodamientos = f"data/seguimientos_rodamientos/{job_id}.parquet"
            cols_optimizadas = [c for c in COLS_TABLA_RODAMIENTOS if c in df_cartera_save.columns]
            self.storage.guardar_parquet(df_cartera_save, path_rodamientos, cols_optimizadas)

            path_master_cartera = f"data/detallados_cartera/{job_id}.parquet"
            cols_master = [c for c in COLS_MASTER_CARTERA if c in df_cartera_save.columns]
            if not cols_master: cols_master = None 
            if self.storage.guardar_parquet(df_cartera_save, path_master_cartera, cols_master):
                resultados_modulos["_archivo_cartera_master"] = path_master_cartera

        df_nov_final = df_novedades if not df_novedades.is_empty() else df_novedades_save
        if df_nov_final is not None and not df_nov_final.is_empty():
            if df_novedades_save is not None:
                path_gestion = f"data/seguimientos_gestion/{job_id}.parquet"
                cols_optimizadas_nov = [c for c in COLS_TABLA_NOVEDADES if c in df_novedades_save.columns]
                self.storage.guardar_parquet(df_novedades_save, path_gestion, cols_optimizadas_nov)

            path_master_nov = f"data/detallados_novedades/{job_id}.parquet"
            cols_master_nov = [c for c in COLS_MASTER_NOVEDADES if c in df_nov_final.columns]
            if not cols_master_nov: cols_master_nov = None
            if self.storage.guardar_parquet(df_nov_final, path_master_nov, cols_master_nov):
                resultados_modulos["_archivo_novedades_master"] = path_master_nov

        return resultados_modulos

    def ejecutar_pipeline(self, job_id: str, file_key: str, empresa: str, tipo_reporte: str = None) -> dict:
        logging.info(f"📥 ORCHESTRATOR: Descargando archivo {file_key} de S3...")
        local_path = self.storage.descargar_archivo(file_key, f"/tmp/{job_id}_input.xlsx")
        
        if not local_path:
            raise ValueError(f"No se pudo descargar el archivo: {file_key}")
        
        try:
            return self.procesar_excel_multi_modulo(local_path, job_id, empresa)
        finally:
            if os.path.exists(local_path):
                os.remove(local_path)