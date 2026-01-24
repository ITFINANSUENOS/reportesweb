import polars as pl
import io
import json
import boto3
import os
from datetime import datetime
from src.core.config import settings
from src.core.columns_config import (
    COLS_CARTERA, COLS_NOVEDADES, COLS_LLAMADAS, COLS_MENSAJERIA, MAPA_FNZ,
    COLS_TABLA_NOVEDADES, COLS_TABLA_RODAMIENTOS
)
from src.utils.polars_utils import leer_hoja_excel, guardar_parquet, limpiar_texto_lote, parsear_fechas

# IMPORTS DE SERVICIOS ANALÍTICOS
from src.services.tableros.cartera.cartera_analytics_service import CarteraAnalyticsService
from src.services.tableros.seguimientos.seguimientos_analytics_service import SeguimientosAnalyticsService
from src.services.tableros.resultados.resultados_analytics_service import ResultadosAnalyticsService # <--- IMPORTANTE

class DataProcessorService:
    def __init__(self):
        self.s3 = boto3.client(
            's3',
            region_name=settings.AWS_REGION,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
        )

    def _guardar_json_s3(self, data_contenido: dict, key_s3: str, job_id: str, empresa: str, modulo: str):
        """Helper para subir JSONs de gráficos a S3 con metadata."""
        try:
            payload_final = {
                "metadata": {
                    "job_id": job_id,
                    "empresa": empresa,
                    "modulo": modulo,
                    "fecha_generacion": datetime.now().isoformat()
                },
                "data": data_contenido
            }

            json_str = json.dumps(payload_final, ensure_ascii=False)
            
            self.s3.put_object(
                Bucket=settings.S3_BUCKET_NAME,
                Key=key_s3,
                Body=json_str,
                ContentType='application/json'
            )
            print(f"✅ JSON guardado en S3: {key_s3}")
        except Exception as e:
            print(f"❌ Error guardando JSON {key_s3}: {e}")

    def _guardar_parquet_s3(self, df: pl.DataFrame, key_s3: str, columnas_validas: list = None):
        """Helper CRÍTICO: Guarda Parquet local y sube a S3."""
        try:
            nombre_local = key_s3.replace("/", "_")
            df_final = df.select(columnas_validas) if columnas_validas else df
            
            guardar_parquet(df_final, nombre_local)
            
            print(f"☁️ Subiendo Parquet a S3: {key_s3}...")
            self.s3.upload_file(nombre_local, settings.S3_BUCKET_NAME, key_s3)
            
            if os.path.exists(nombre_local):
                os.remove(nombre_local)
                
            return True
        except Exception as e:
            print(f"❌ Error subiendo Parquet {key_s3}: {e}")
            return False

    def _preprocesar_cartera(self, df_cartera: pl.DataFrame) -> pl.DataFrame:
        try:
            defaults = {"Zona": "SIN ZONA", "Call_Center_Apoyo": "SIN APOYO"}
            for col, val in defaults.items():
                if col not in df_cartera.columns:
                    df_cartera = df_cartera.with_columns(pl.lit(val).alias(col))

            df_cartera = df_cartera.with_columns([
                pl.col("Zona").cast(pl.Utf8).str.replace_all(" ", "").str.strip_chars().str.to_uppercase(),
                pl.col("Call_Center_Apoyo").cast(pl.Utf8).str.replace_all(" ", "").str.strip_chars().str.to_uppercase()
            ])

            zonas_cl = ['CL1', 'CL2', 'CL3', 'CL4']
            apoyo_cl = ['CL5', 'CL6', 'CL7', 'CL8', 'CL9']

            df_cartera = df_cartera.with_columns(
                pl.when(pl.col("Zona").is_in(zonas_cl)).then(pl.col("Zona"))
                .when(pl.col("Call_Center_Apoyo").is_in(apoyo_cl)).then(pl.col("Call_Center_Apoyo"))
                .otherwise(pl.lit("SIN CALL CENTER")).alias("CALL_CENTER_FILTRO"),
                
                pl.col("Regional_Cobro").fill_null("OTRAS ZONAS")
            )
            return df_cartera
        except Exception as e:
            print(f"❌ Error preprocesando cartera: {e}")
            return df_cartera

    def procesar_excel_multi_modulo(self, file_path: str, job_id: str, empresa: str) -> dict:
        resultados_modulos = {}

        # 1. CARGA CARTERA
        overrides_cartera = {
            "Valor_Desembolso": pl.Float64, "Valor_Cuota": pl.Float64, "Valor_Cuota_Atraso": pl.Float64,
            "Valor_Cuota_Vigente": pl.Utf8, "Total_Recaudo": pl.Float64, "Valor_Vencido": pl.Float64,
            "Cedula_Cliente": pl.Utf8, "Celular": pl.Utf8, "Cobrador":pl.Utf8, "Telefono_Cobrador": pl.Utf8, "Telefono_Gestor": pl.Utf8,
            "Telefono_Codeudor1": pl.Utf8, "Telefono_Codeudor2": pl.Utf8, "Credito": pl.Utf8, "Movil_Lider": pl.Utf8,
            "Cantidad_Novedades": pl.Float64, "Meta_General": pl.Float64, "Meta_$": pl.Float64, 
            "Meta_Saldo": pl.Float64, "Recaudo_Meta": pl.Float64, "Meta_Intereses": pl.Float64,
            # Campos extra necesarios para RESULTADOS
            "Meta_T.R_$": pl.Float64, "Total_Recaudo_Sin_Anti": pl.Float64 
        }
        df_cartera = leer_hoja_excel(file_path, "Analisis_de_Cartera", COLS_CARTERA, overrides_cartera)

        if not df_cartera.is_empty():
            if "Valor_Cuota_Vigente" in df_cartera.columns:
                df_cartera = df_cartera.with_columns(
                    pl.when(pl.col("Valor_Cuota_Vigente").str.to_uppercase().str.contains("ANTICIPADO"))
                    .then(pl.lit("ANTICIPADO")).otherwise(pl.lit("NORMAL")).alias("Tipo_Vigencia_Temp")
                )
                df_cartera = df_cartera.with_columns(
                    pl.col("Valor_Cuota_Vigente").str.replace("(?i)ANTICIPADO", "0")
                      .str.replace(",", "").cast(pl.Float64, strict=False).fill_null(0)
                )
            else:
                df_cartera = df_cartera.with_columns(pl.lit("NORMAL").alias("Tipo_Vigencia_Temp"))
            
            df_cartera = parsear_fechas(df_cartera, ["Fecha_Desembolso", "Fecha_Ultima_Novedad", "Fecha_Cuota_Atraso", "Fecha_Cuota_Vigente"])
            df_cartera = limpiar_texto_lote(df_cartera, ["Empresa", "Regional_Venta", "Nombre_Ciudad", "Nombre_Vendedor", "Franja_Meta", "Rodamiento", "Regional_Cobro", "Zona", "Cedula_Cliente"])
            if "Cantidad_Novedades" in df_cartera.columns:
                df_cartera = df_cartera.with_columns(pl.col("Cantidad_Novedades").fill_null(0))
            
            df_cartera = self._preprocesar_cartera(df_cartera)

        # 2. CARGA NOVEDADES
        overrides_nov = {"Celular_Cliente": pl.Utf8, "Telefono_Cliente": pl.Utf8, "Cedula_Cliente": pl.Utf8, "Valor": pl.Float64, "Novedad": pl.Utf8}
        df_novedades = leer_hoja_excel(file_path, "Detalle_Novedades", COLS_NOVEDADES, overrides_nov)

        if not df_novedades.is_empty():
            if "Celular_Cliente" in df_novedades.columns: 
                df_novedades = df_novedades.with_columns(pl.col("Celular_Cliente").str.replace(r"\.$", ""))
            df_novedades = parsear_fechas(df_novedades, ["Fecha_Novedad", "Fecha_Compromiso"])
            df_novedades = limpiar_texto_lote(df_novedades, ["Cedula_Cliente"])
            
        # 3. ARCHIVOS SOPORTE
        df_llamadas = leer_hoja_excel(file_path, "Reporte_Llamadas", COLS_LLAMADAS, {"Destino_Llamada": pl.Utf8, "Extension_Llamada": pl.Utf8, "Codigo_Llamada": pl.Utf8})
        if not df_llamadas.is_empty():
            df_llamadas = parsear_fechas(df_llamadas, ["Fecha_Llamada"])
            self._guardar_parquet_s3(df_llamadas, f"data/llamadas/{job_id}.parquet")

        df_mensajeria = leer_hoja_excel(file_path, "Reporte_Mensajes", COLS_MENSAJERIA, {"Fecha_Llamada": pl.Utf8, "Numero_Telefono": pl.Utf8})
        if not df_mensajeria.is_empty():
            df_mensajeria = parsear_fechas(df_mensajeria, ["Fecha_Llamada"])
            self._guardar_parquet_s3(df_mensajeria, f"data/mensajes/{job_id}.parquet")
            
        df_fnz = leer_hoja_excel(file_path, "FNZ007", list(MAPA_FNZ.keys()), {"PAGARE": pl.Utf8, "CEDULA": pl.Utf8, "TELEFONO1": pl.Utf8, "MOVIL": pl.Utf8, "VALOR_TOTA": pl.Float64, "DESEMBOLSO": pl.Utf8})
        if not df_fnz.is_empty():
            df_fnz = df_fnz.rename({k:v for k,v in MAPA_FNZ.items() if k in df_fnz.columns})
            df_fnz = parsear_fechas(df_fnz, ["Fecha_Nacimiento"])
            self._guardar_parquet_s3(df_fnz, f"data/fnz/{job_id}.parquet")

        # 4. ANALÍTICA Y GUARDADO
        df_cartera_save = None
        df_novedades_save = None

        if not df_cartera.is_empty():
            # A. CARTERA
            try:
                metrics_cartera = CarteraAnalyticsService().calcular_metricas_tablero_principal(df_cartera)
                resultados_modulos["cartera"] = metrics_cartera
                self._guardar_json_s3(metrics_cartera, f"graficos/cartera/{job_id}.json", job_id, empresa, "cartera")
                df_cartera_save = df_cartera 
            except Exception as e:
                print(f"❌ Error Cartera: {e}")

            # B. SEGUIMIENTOS
            try:
                res_seg = SeguimientosAnalyticsService().calcular_metricas_seguimientos(df_cartera, df_novedades)
                if "_df_novedades_full" in res_seg: df_novedades_save = res_seg.pop("_df_novedades_full")
                if "_df_cartera_base" in res_seg: df_cartera_save = res_seg.pop("_df_cartera_base")
                resultados_modulos["seguimientos"] = res_seg
                self._guardar_json_s3(res_seg, f"graficos/seguimientos/{job_id}.json", job_id, empresa, "seguimientos")
            except Exception as e:
                print(f"❌ Error Seguimientos: {e}")

            # --- C. RESULTADOS (ESTO ES LO QUE FALTABA) ---
            try:
                # Usamos df_cartera_save porque ya tiene limpiezas de Seguimientos si aplicara, o df_cartera
                df_input_res = df_cartera_save if df_cartera_save is not None else df_cartera
                
                res_resultados = ResultadosAnalyticsService().calcular_metricas_resultados(df_input_res)
                resultados_modulos["resultados"] = res_resultados
                
                self._guardar_json_s3(
                    data_contenido=res_resultados, 
                    key_s3=f"graficos/resultados/{job_id}.json", 
                    job_id=job_id, 
                    empresa=empresa, 
                    modulo="resultados"
                )
            except Exception as e:
                print(f"❌ Error Resultados: {e}")

        # 5. GUARDADO DE PARQUETS FINALES
        if df_cartera_save is not None:
            path_cartera = f"data/seguimientos_rodamientos/{job_id}.parquet"
            cols_validas = [c for c in COLS_TABLA_RODAMIENTOS if c in df_cartera_save.columns]
            if self._guardar_parquet_s3(df_cartera_save, path_cartera, cols_validas):
                resultados_modulos["_archivo_cartera"] = path_cartera

        if df_novedades_save is not None:
            path_nov = f"data/seguimientos_gestion/{job_id}.parquet"
            cols_validas = [c for c in COLS_TABLA_NOVEDADES if c in df_novedades_save.columns]
            if self._guardar_parquet_s3(df_novedades_save, path_nov, cols_validas):
                resultados_modulos["_archivo_novedades"] = path_nov

        return resultados_modulos