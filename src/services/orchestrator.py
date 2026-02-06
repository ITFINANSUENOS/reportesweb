from src.core.constants import (
    COLS_TABLA_NOVEDADES, COLS_TABLA_RODAMIENTOS,
    COLS_MASTER_CARTERA, COLS_MASTER_NOVEDADES)

from src.services.storage.s3_service import S3Service
from src.services.storage.excel_loader import ExcelLoaderService
from src.services.analytics.cartera import CarteraAnalyticsService
from src.services.analytics.seguimientos import SeguimientosAnalyticsService
from src.services.analytics.resultados import ResultadosAnalyticsService
from src.services.analytics.comercial import ComercialAnalyticsService

class ReportesOrchestrator:
    def __init__(self):
        self.storage = S3Service()
        self.loader = ExcelLoaderService()

    def procesar_excel_multi_modulo(self, file_path: str, job_id: str, empresa: str) -> dict:
        resultados_modulos = {}
        print("⏳ ORCHESTRATOR: Cargando hojas de Excel...")
        
        # 1. CARGA DE DATOS
        df_cartera = self.loader.cargar_cartera(file_path)
        df_novedades = self.loader.cargar_novedades(file_path)
        
        # Archivos soporte (Llamadas, Mensajes, FNZ)
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

        if not df_cartera.is_empty():
            metadata_base = {"job_id": job_id, "empresa": empresa}

            # A. CARTERA (Analytics)
            try:
                metrics_cartera = CarteraAnalyticsService().calcular_metricas_tablero_principal(df_cartera)
                resultados_modulos["cartera"] = metrics_cartera
                self.storage.guardar_json(metrics_cartera, f"graficos/cartera/{job_id}.json", {**metadata_base, "modulo": "cartera"})
                df_cartera_save = df_cartera 
            except Exception as e:
                print(f"❌ Error Cartera: {e}")

            # B. SEGUIMIENTOS (Analytics y Cruces)
            try:
                res_seg = SeguimientosAnalyticsService().calcular_metricas_seguimientos(df_cartera, df_novedades)
                if "_df_novedades_full" in res_seg: df_novedades_save = res_seg.pop("_df_novedades_full")
                if "_df_cartera_base" in res_seg: df_cartera_save = res_seg.pop("_df_cartera_base")
                resultados_modulos["seguimientos"] = res_seg
                self.storage.guardar_json(res_seg, f"graficos/seguimientos/{job_id}.json", {**metadata_base, "modulo": "seguimientos"})
            except Exception as e:
                print(f"❌ Error Seguimientos: {e}")

            # C. RESULTADOS (KPIs)
            try:
                df_input_res = df_cartera_save if df_cartera_save is not None else df_cartera
                res_resultados = ResultadosAnalyticsService().calcular_metricas_resultados(df_input_res)
                resultados_modulos["resultados"] = res_resultados
                self.storage.guardar_json(res_resultados, f"graficos/resultados/{job_id}.json", {**metadata_base, "modulo": "resultados"})
            except Exception as e:
                print(f"❌ Error Resultados: {e}")
                
        # D. MÓDULO COMERCIAL
        try:
            print("📊 ANALYTICS: Procesando Módulo Comercial...")
            comercial_service = ComercialAnalyticsService()
            
            # 1. PREPARACIÓN DE DATOS
            df_fnz_input = df_fnz if not df_fnz.is_empty() else None
            df_retanqueos = comercial_service.calcular_df_retanqueos(df_cartera)
            df_cosechas = comercial_service.calcular_df_cosechas(df_cartera)

            # 2. GUARDADO DE PARQUETS (Capa de Detalle)
            if df_fnz_input is not None:
                self.storage.guardar_parquet(df_fnz_input, f"data/comercial/fnz_{job_id}.parquet")
                
            if df_retanqueos is not None:
                self.storage.guardar_parquet(df_retanqueos, f"data/comercial/retanqueos_{job_id}.parquet")
                
            if df_cosechas is not None:
                self.storage.guardar_parquet(df_cosechas, f"data/comercial/cosechas_{job_id}.parquet")

            # 3. GUARDADO DE JSON (Capa de Visualización)
            # Pasamos todos los DF para generar contadores, aunque solo FNZ se exporta en detalle
            json_comercial = comercial_service.generar_json_comercial(
                df_fnz=df_fnz_input,
                df_retanqueos=df_retanqueos,
                df_cosechas=df_cosechas
            )
            
            self.storage.guardar_json(
                json_comercial, 
                f"graficos/comercial/{job_id}.json", 
                {"job_id": job_id, "modulo": "comercial"}
            )
            
            resultados_modulos["comercial"] = True
            print("✅ Comercial: 3 Parquets y 1 JSON generados correctamente.")

        except Exception as e:
            print(f"❌ Error Módulo Comercial: {e}")
            import traceback
            traceback.print_exc()        

        # 3. GUARDADO FINAL DE TABLAS 
        # --- A. CARTERA ---
        if df_cartera_save is not None:
            # Versión Optimizada
            path_rodamientos = f"data/seguimientos_rodamientos/{job_id}.parquet"
            cols_optimizadas = [c for c in COLS_TABLA_RODAMIENTOS if c in df_cartera_save.columns]
            self.storage.guardar_parquet(df_cartera_save, path_rodamientos, cols_optimizadas)

            # Versión MASTER
            path_master_cartera = f"data/detallados_cartera/{job_id}.parquet"
            cols_master = [c for c in COLS_MASTER_CARTERA if c in df_cartera_save.columns]
            if not cols_master: cols_master = None 
            if self.storage.guardar_parquet(df_cartera_save, path_master_cartera, cols_master):
                resultados_modulos["_archivo_cartera_master"] = path_master_cartera

        # --- B. NOVEDADES ---
        df_nov_final = df_novedades if not df_novedades.is_empty() else df_novedades_save

        if df_nov_final is not None and not df_nov_final.is_empty():
            # Versión Optimizada
            if df_novedades_save is not None:
                path_gestion = f"data/seguimientos_gestion/{job_id}.parquet"
                cols_optimizadas_nov = [c for c in COLS_TABLA_NOVEDADES if c in df_novedades_save.columns]
                self.storage.guardar_parquet(df_novedades_save, path_gestion, cols_optimizadas_nov)

            # Versión MASTER
            path_master_nov = f"data/detallados_novedades/{job_id}.parquet"
            cols_master_nov = [c for c in COLS_MASTER_NOVEDADES if c in df_nov_final.columns]
            if not cols_master_nov: cols_master_nov = None
            if self.storage.guardar_parquet(df_nov_final, path_master_nov, cols_master_nov):
                resultados_modulos["_archivo_novedades_master"] = path_master_nov

        return resultados_modulos