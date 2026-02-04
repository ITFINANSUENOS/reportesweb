from src.core.constants import COLS_TABLA_NOVEDADES, COLS_TABLA_RODAMIENTOS

# Nuevos Servicios
from src.services.storage.s3_service import S3Service
from src.services.storage.excel_loader import ExcelLoaderService

from src.services.analytics.cartera import CarteraAnalyticsService
from src.services.analytics.seguimientos import SeguimientosAnalyticsService
from src.services.analytics.resultados import ResultadosAnalyticsService

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
        
        # Archivos soporte (Se cargan y se suben directamente)
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

            # A. CARTERA
            try:
                metrics_cartera = CarteraAnalyticsService().calcular_metricas_tablero_principal(df_cartera)
                resultados_modulos["cartera"] = metrics_cartera
                self.storage.guardar_json(metrics_cartera, f"graficos/cartera/{job_id}.json", {**metadata_base, "modulo": "cartera"})
                df_cartera_save = df_cartera 
            except Exception as e:
                print(f"❌ Error Cartera: {e}")

            # B. SEGUIMIENTOS
            try:
                res_seg = SeguimientosAnalyticsService().calcular_metricas_seguimientos(df_cartera, df_novedades)
                # Extraemos los dataframes procesados
                if "_df_novedades_full" in res_seg: df_novedades_save = res_seg.pop("_df_novedades_full")
                if "_df_cartera_base" in res_seg: df_cartera_save = res_seg.pop("_df_cartera_base")
                
                resultados_modulos["seguimientos"] = res_seg
                self.storage.guardar_json(res_seg, f"graficos/seguimientos/{job_id}.json", {**metadata_base, "modulo": "seguimientos"})
            except Exception as e:
                print(f"❌ Error Seguimientos: {e}")

            # C. RESULTADOS
            try:
                df_input_res = df_cartera_save if df_cartera_save is not None else df_cartera
                res_resultados = ResultadosAnalyticsService().calcular_metricas_resultados(df_input_res)
                resultados_modulos["resultados"] = res_resultados
                self.storage.guardar_json(res_resultados, f"graficos/resultados/{job_id}.json", {**metadata_base, "modulo": "resultados"})
            except Exception as e:
                print(f"❌ Error Resultados: {e}")

        # 3. GUARDADO FINAL DE TABLAS (Data Lake)
        if df_cartera_save is not None:
            path_cartera = f"data/seguimientos_rodamientos/{job_id}.parquet"
            cols_validas = [c for c in COLS_TABLA_RODAMIENTOS if c in df_cartera_save.columns]
            if self.storage.guardar_parquet(df_cartera_save, path_cartera, cols_validas):
                resultados_modulos["_archivo_cartera"] = path_cartera

        if df_novedades_save is not None:
            path_nov = f"data/seguimientos_gestion/{job_id}.parquet"
            cols_validas = [c for c in COLS_TABLA_NOVEDADES if c in df_novedades_save.columns]
            if self.storage.guardar_parquet(df_novedades_save, path_nov, cols_validas):
                resultados_modulos["_archivo_novedades"] = path_nov

        return resultados_modulos