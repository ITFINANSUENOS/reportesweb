import polars as pl
from src.core.columns_config import COLS_CARTERA, COLS_NOVEDADES, COLS_LLAMADAS, COLS_MENSAJERIA, MAPA_FNZ
from src.utils.polars_utils import leer_hoja_excel, guardar_parquet, guardar_json, limpiar_texto_lote, parsear_fechas
from src.services.tableros.cartera.cartera_analytics_service import CarteraAnalyticsService
from src.services.tableros.seguimientos.seguimientos_analytics_service import SeguimientosAnalyticsService

class DataProcessorService:

    def _preprocesar_cartera(self, df_cartera: pl.DataFrame) -> pl.DataFrame:
        """Aplica lógica de negocio específica para Cartera."""
        try:
            # 1. Asegurar columnas para evitar crash
            cols_requeridas = ["Zona", "Call_Center_Apoyo", "Saldo_Capital", "Saldo_Interes_Corriente", "Saldo_Avales"]
            defaults = {"Zona": "SIN ZONA", "Call_Center_Apoyo": "SIN APOYO"}
            
            for col in cols_requeridas:
                if col not in df_cartera.columns:
                    val = defaults.get(col, 0.0)
                    df_cartera = df_cartera.with_columns(pl.lit(val).alias(col))

            # 2. Normalización de Textos Clave
            df_cartera = df_cartera.with_columns([
                pl.col("Zona").cast(pl.Utf8).str.replace_all(" ", "").str.strip_chars().str.to_uppercase(),
                pl.col("Call_Center_Apoyo").cast(pl.Utf8).str.replace_all(" ", "").str.strip_chars().str.to_uppercase()
            ])

            # 3. Lógica Zonas y Saldos
            zonas_cl = ['CL1', 'CL2', 'CL3', 'CL4']
            apoyo_cl = ['CL5', 'CL6', 'CL7', 'CL8', 'CL9']

            df_cartera = df_cartera.with_columns(
                pl.when(pl.col("Zona").is_in(zonas_cl)).then(pl.col("Zona"))
                .when(pl.col("Call_Center_Apoyo").is_in(apoyo_cl)).then(pl.col("Call_Center_Apoyo"))
                .otherwise(pl.lit("SIN CALL CENTER")).alias("CALL_CENTER_FILTRO"),
                
                pl.col("Regional_Cobro").fill_null("OTRAS ZONAS"),
                
                (pl.col("Saldo_Capital").fill_null(0) + 
                 pl.col("Saldo_Interes_Corriente").fill_null(0) + 
                 pl.col("Saldo_Avales").fill_null(0)).alias("Valor_Saldo")
            )
            return df_cartera
        except Exception as e:
            print(f"❌ Error en lógica de negocio Cartera: {e}")
            return df_cartera

    def procesar_excel_multi_modulo(self, file_path: str, job_id: str = "temp_job") -> dict:
        resultados_modulos = {}

        # ---------------------------------------------------------
        # 1. CARTERA
        # ---------------------------------------------------------
        overrides_cartera = {
            "Valor_Desembolso": pl.Float64, "Valor_Cuota": pl.Float64, "Valor_Cuota_Atraso": pl.Float64,
            "Valor_Cuota_Vigente": pl.Utf8, "Total_Recaudo": pl.Float64, "Valor_Vencido": pl.Float64,
            "Cedula_Cliente": pl.Utf8, "Celular": pl.Utf8, "Telefono_Cobrador": pl.Utf8, "Telefono_Gestor": pl.Utf8,
            "Telefono_Codeudor1": pl.Utf8, "Telefono_Codeudor2": pl.Utf8, "Credito": pl.Utf8, 
            "Cantidad_Novedades": pl.Float64, "Meta_General": pl.Float64, "Meta_$": pl.Float64, 
            "Meta_Saldo": pl.Float64, "Recaudo_Meta": pl.Float64, "Meta_Intereses": pl.Float64, 
            "Saldo_Capital": pl.Float64, "Saldo_Interes_Corriente": pl.Float64, "Saldo_Avales": pl.Float64
        }
        df_cartera = leer_hoja_excel(file_path, "Analisis_de_Cartera", COLS_CARTERA, overrides_cartera)

        if not df_cartera.is_empty():
            # Limpieza "ANTICIPADO" y Fechas
            if "Valor_Cuota_Vigente" in df_cartera.columns:
                df_cartera = df_cartera.with_columns(
                    pl.col("Valor_Cuota_Vigente").str.replace("(?i)ANTICIPADO", "0").str.replace(",", "").cast(pl.Float64, strict=False).fill_null(0)
                )
            
            df_cartera = parsear_fechas(df_cartera, ["Fecha_Desembolso", "Fecha_Ultima_Novedad", "Fecha_Cuota_Atraso", "Fecha_Cuota_Vigente"])
            df_cartera = limpiar_texto_lote(df_cartera, ["Empresa", "Regional_Venta", "Nombre_Ciudad", "Nombre_Vendedor", "Franja_Meta", "Rodamiento", "Regional_Cobro", "Zona", "Cedula_Cliente"])
            
            if "Cantidad_Novedades" in df_cartera.columns:
                df_cartera = df_cartera.with_columns(pl.col("Cantidad_Novedades").fill_null(0))
            
            # Aplicar Lógica de Negocio
            df_cartera = self._preprocesar_cartera(df_cartera)

        # ---------------------------------------------------------
        # 2. NOVEDADES
        # ---------------------------------------------------------
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

        # ---------------------------------------------------------
        # 3. HOJAS OPCIONALES (Lectura y Guardado)
        # ---------------------------------------------------------
        
        # Llamadas
        df_llamadas = leer_hoja_excel(file_path, "Reporte_Llamadas", COLS_LLAMADAS, 
            {"Destino_Llamada": pl.Utf8, "Extension_Llamada": pl.Utf8, "Codigo_Llamada": pl.Utf8})
        if not df_llamadas.is_empty():
            if "Fecha_Llamada" in df_llamadas.columns:
                df_llamadas = df_llamadas.with_columns(pl.col("Fecha_Llamada").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False))
            guardar_parquet(df_llamadas, f"data/llamadas/{job_id}.parquet")

        # Mensajeria
        df_mensajeria = leer_hoja_excel(file_path, "Reporte_Mensajes", COLS_MENSAJERIA, 
            {"Fecha_Llamada": pl.Utf8, "Numero_Telefono": pl.Utf8})
        if not df_mensajeria.is_empty():
            if "Fecha_Llamada" in df_mensajeria.columns:
                df_mensajeria = df_mensajeria.with_columns(pl.col("Fecha_Llamada").str.to_date(strict=False))
            guardar_parquet(df_mensajeria, f"data/mensajes/{job_id}.parquet")

        # FNZ007
        df_fnz = leer_hoja_excel(file_path, "FNZ007", list(MAPA_FNZ.keys()), 
            {"PAGARE": pl.Utf8, "CEDULA": pl.Utf8, "TELEFONO1": pl.Utf8, "MOVIL": pl.Utf8, "VALOR_TOTA": pl.Float64, "DESEMBOLSO": pl.Utf8})
        if not df_fnz.is_empty():
            df_fnz = df_fnz.rename({k:v for k,v in MAPA_FNZ.items() if k in df_fnz.columns})
            if "Fecha_Nacimiento" in df_fnz.columns:
                df_fnz = df_fnz.with_columns(pl.col("Fecha_Nacimiento").cast(pl.Utf8).str.to_date(strict=False))
            guardar_parquet(df_fnz, f"data/fnz/{job_id}.parquet")


        # ---------------------------------------------------------
        # 4. EJECUCIÓN ANALÍTICA Y SALIDAS
        # ---------------------------------------------------------
        df_cartera_save = None
        df_novedades_save = None

        if not df_cartera.is_empty():
            # Módulo Cartera
            try:
                resultados_modulos["cartera"] = CarteraAnalyticsService().calcular_metricas_tablero_principal(df_cartera)
                df_cartera_save = df_cartera 
            except Exception as e:
                resultados_modulos["cartera"] = {"error": str(e)}

            # Módulo Seguimientos
            try:
                res_seg = SeguimientosAnalyticsService().calcular_metricas_seguimientos(df_cartera, df_novedades)
                
                # Separación de datos: Extraemos DataFrames para Parquet
                if "_df_novedades_full" in res_seg:
                    df_novedades_save = res_seg.pop("_df_novedades_full")
                if "_df_cartera_base" in res_seg:
                    res_seg.pop("_df_cartera_base") 

                resultados_modulos["seguimientos"] = res_seg
            except Exception as e:
                resultados_modulos["seguimientos"] = {"error": str(e)}

        # ---------------------------------------------------------
        # 5. GUARDADO FINAL DE PARQUETS MAESTROS
        # ---------------------------------------------------------
        if df_cartera_save is not None:
            guardar_parquet(df_cartera_save, f"data/cartera/{job_id}.parquet")
            resultados_modulos["_archivo_cartera"] = f"data/cartera/{job_id}.parquet"

        if df_novedades_save is not None:
            path_nov = f"data/novedades/{job_id}.parquet"
            # Unimos todas las columnas disponibles para el frontend
            todas_cols = list(set(COLS_CARTERA + COLS_NOVEDADES + 
                                ['Estado_Pago', 'Estado_Gestion', 'Novedades_Por_Cargo', 'Tipo_Vigencia_Temp', 'Valor_Saldo', 'CALL_CENTER_FILTRO']))
            
            guardar_parquet(df_novedades_save, path_nov, cols_especificas=todas_cols)
            resultados_modulos["_archivo_novedades"] = path_nov

        # Nota: El guardado de JSONs (guardar_json_resultado) ahora se delega 
        # al controlador (reportes_controller.py) que usará el utilitario de utils.
        # Debes asegurarte de exponer 'guardar_json' en el servicio si el controlador lo llama a través de la instancia,
        # O actualizar el controlador para importar 'guardar_json' desde utils.
        
        return resultados_modulos
    
    # Wrapper por si tu controlador llama a processor.guardar_json_resultado
    def guardar_json_resultado(self, data: dict, output_path: str):
        return guardar_json(data, output_path)