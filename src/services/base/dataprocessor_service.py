import polars as pl
from src.core.columns_config import (
    COLS_CARTERA, COLS_NOVEDADES, COLS_LLAMADAS, COLS_MENSAJERIA, MAPA_FNZ,
    COLS_TABLA_NOVEDADES, COLS_TABLA_RODAMIENTOS
)
from src.utils.polars_utils import leer_hoja_excel, guardar_parquet, guardar_json, limpiar_texto_lote, parsear_fechas
from src.services.tableros.cartera.cartera_analytics_service import CarteraAnalyticsService
from src.services.tableros.seguimientos.seguimientos_analytics_service import SeguimientosAnalyticsService

class DataProcessorService:

    def _preprocesar_cartera(self, df_cartera: pl.DataFrame) -> pl.DataFrame:
        """Aplica lógica de negocio específica para Cartera (Limpieza y Normalización)."""
        try:
            # 1. Asegurar columnas de negocio básicas (Zona, Call Center) para filtros
            defaults = {"Zona": "SIN ZONA", "Call_Center_Apoyo": "SIN APOYO"}
            for col, val in defaults.items():
                if col not in df_cartera.columns:
                    df_cartera = df_cartera.with_columns(pl.lit(val).alias(col))

            # 2. Normalización de Textos (Mayúsculas y sin espacios)
            df_cartera = df_cartera.with_columns([
                pl.col("Zona").cast(pl.Utf8).str.replace_all(" ", "").str.strip_chars().str.to_uppercase(),
                pl.col("Call_Center_Apoyo").cast(pl.Utf8).str.replace_all(" ", "").str.strip_chars().str.to_uppercase()
            ])

            # 3. Lógica para columna calculada 'CALL_CENTER_FILTRO'
            # (Esto sí es necesario para los filtros del frontend)
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
            print(f"❌ Error en lógica de negocio Cartera: {e}")
            return df_cartera

    def procesar_excel_multi_modulo(self, file_path: str, job_id: str = "temp_job") -> dict:
        resultados_modulos = {}
        # 1. CARTERA
        overrides_cartera = {
            "Valor_Desembolso": pl.Float64, "Valor_Cuota": pl.Float64, "Valor_Cuota_Atraso": pl.Float64,
            "Valor_Cuota_Vigente": pl.Utf8, "Total_Recaudo": pl.Float64, "Valor_Vencido": pl.Float64,
            "Cedula_Cliente": pl.Utf8, "Celular": pl.Utf8, "Telefono_Cobrador": pl.Utf8, "Telefono_Gestor": pl.Utf8,
            "Telefono_Codeudor1": pl.Utf8, "Telefono_Codeudor2": pl.Utf8, "Credito": pl.Utf8,"Movil_Lider": pl.Utf8,
            "Cantidad_Novedades": pl.Float64, "Meta_General": pl.Float64, "Meta_$": pl.Float64, 
            "Meta_Saldo": pl.Float64, "Recaudo_Meta": pl.Float64, "Meta_Intereses": pl.Float64
        }
        df_cartera = leer_hoja_excel(file_path, "Analisis_de_Cartera", COLS_CARTERA, overrides_cartera)

        if not df_cartera.is_empty():
            # --- DETECCIÓN DE ANTICIPADOS ---
            # Se hace antes de limpiar los números para no perder la palabra "ANTICIPADO"
            if "Valor_Cuota_Vigente" in df_cartera.columns:
                df_cartera = df_cartera.with_columns(
                    pl.when(pl.col("Valor_Cuota_Vigente").str.to_uppercase().str.contains("ANTICIPADO"))
                    .then(pl.lit("ANTICIPADO"))
                    .otherwise(pl.lit("NORMAL"))
                    .alias("Tipo_Vigencia_Temp")
                )
                # Ahora sí convertimos a numérico reemplazando texto por 0
                df_cartera = df_cartera.with_columns(
                    pl.col("Valor_Cuota_Vigente").str.replace("(?i)ANTICIPADO", "0").str.replace(",", "").cast(pl.Float64, strict=False).fill_null(0)
                )
            else:
                df_cartera = df_cartera.with_columns(pl.lit("NORMAL").alias("Tipo_Vigencia_Temp"))
            
            # Parseo de fechas y limpieza general
            df_cartera = parsear_fechas(df_cartera, ["Fecha_Desembolso", "Fecha_Ultima_Novedad", "Fecha_Cuota_Atraso", "Fecha_Cuota_Vigente"])
            df_cartera = limpiar_texto_lote(df_cartera, ["Empresa", "Regional_Venta", "Nombre_Ciudad", "Nombre_Vendedor", "Franja_Meta", "Rodamiento", "Regional_Cobro", "Zona", "Cedula_Cliente"])
            
            if "Cantidad_Novedades" in df_cartera.columns:
                df_cartera = df_cartera.with_columns(pl.col("Cantidad_Novedades").fill_null(0))
            
            # Ejecutar preprocesamiento (Zonas, Call Center)
            df_cartera = self._preprocesar_cartera(df_cartera)

        # 2. NOVEDADES
        overrides_nov = {
            "Celular_Cliente": pl.Utf8, "Telefono_Cliente": pl.Utf8, "Cedula_Cliente": pl.Utf8, 
            "Valor": pl.Float64, "Novedad": pl.Utf8
        }
        df_novedades = leer_hoja_excel(file_path, "Detalle_Novedades", COLS_NOVEDADES, overrides_nov)

        if not df_novedades.is_empty():
            if "Celular_Cliente" in df_novedades.columns: 
                df_novedades = df_novedades.with_columns(pl.col("Celular_Cliente").str.replace(r"\.$", ""))
            df_novedades = parsear_fechas(df_novedades, ["Fecha_Novedad", "Fecha_Compromiso"])
            df_novedades = limpiar_texto_lote(df_novedades, ["Cedula_Cliente"])
            
        # 3. HOJAS OPCIONALES
        # Llamadas
        df_llamadas = leer_hoja_excel(file_path, "Reporte_Llamadas", COLS_LLAMADAS, 
            {"Destino_Llamada": pl.Utf8, "Extension_Llamada": pl.Utf8, "Codigo_Llamada": pl.Utf8})
        if not df_llamadas.is_empty():
            df_llamadas = parsear_fechas(df_llamadas, ["Fecha_Llamada"])
            guardar_parquet(df_llamadas, f"data/llamadas/{job_id}.parquet")

        # Mensajeria
        df_mensajeria = leer_hoja_excel(file_path, "Reporte_Mensajes", COLS_MENSAJERIA, 
            {"Fecha_Llamada": pl.Utf8, "Numero_Telefono": pl.Utf8})
        if not df_mensajeria.is_empty():
            df_mensajeria = parsear_fechas(df_mensajeria, ["Fecha_Llamada"])
            guardar_parquet(df_mensajeria, f"data/mensajes/{job_id}.parquet")

        # FNZ007
        df_fnz = leer_hoja_excel(file_path, "FNZ007", list(MAPA_FNZ.keys()), 
            {"PAGARE": pl.Utf8, "CEDULA": pl.Utf8, "TELEFONO1": pl.Utf8, "MOVIL": pl.Utf8, "VALOR_TOTA": pl.Float64, "DESEMBOLSO": pl.Utf8})
        if not df_fnz.is_empty():
            df_fnz = df_fnz.rename({k:v for k,v in MAPA_FNZ.items() if k in df_fnz.columns})
            df_fnz = parsear_fechas(df_fnz, ["Fecha_Nacimiento"])
            guardar_parquet(df_fnz, f"data/fnz/{job_id}.parquet")

        # 4. EJECUCIÓN ANALÍTICA
        df_cartera_save = None
        df_novedades_save = None

        if not df_cartera.is_empty():
            try:
                resultados_modulos["cartera"] = CarteraAnalyticsService().calcular_metricas_tablero_principal(df_cartera)
                # Este 'df_cartera_save' es la base, pero será reemplazado si 'seguimientos' retorna uno enriquecido
                df_cartera_save = df_cartera 
            except Exception as e:
                resultados_modulos["cartera"] = {"error": str(e)}

            try:
                res_seg = SeguimientosAnalyticsService().calcular_metricas_seguimientos(df_cartera, df_novedades)
                # A. Extraemos el DF Full para la TABLA DE GESTIÓN (cruzado)
                if "_df_novedades_full" in res_seg:
                    df_novedades_save = res_seg.pop("_df_novedades_full")
                # B. Extraemos el DF Base para la TABLA DE RODAMIENTOS (cartera única + estados)
                if "_df_cartera_base" in res_seg:
                    # Sobrescribimos df_cartera_save con este porque ya tiene Estado_Pago y Estado_Gestion calculados
                    df_cartera_save = res_seg.pop("_df_cartera_base") 
                resultados_modulos["seguimientos"] = res_seg
            except Exception as e:
                resultados_modulos["seguimientos"] = {"error": str(e)}
                
        # 5. GUARDADO FINAL DIFERENCIADO
        # A. TABLA RODAMIENTOS (Data financiera única por crédito)
        if df_cartera_save is not None:
            # 'seguimientos_rodamientos'
            path_cartera = f"data/seguimientos_rodamientos/{job_id}.parquet"
            
            guardar_parquet(df_cartera_save, path_cartera, cols_especificas=COLS_TABLA_RODAMIENTOS)
            resultados_modulos["_archivo_cartera"] = path_cartera

        # B. TABLA GESTIÓN (Data detallada con novedades)
        if df_novedades_save is not None:
            # 'seguimientos_gestion'
            path_nov = f"data/seguimientos_gestion/{job_id}.parquet"
            guardar_parquet(df_novedades_save, path_nov, cols_especificas=COLS_TABLA_NOVEDADES)
            resultados_modulos["_archivo_novedades"] = path_nov
        return resultados_modulos
    
    def guardar_json_resultado(self, data: dict, output_path: str):
        return guardar_json(data, output_path)
