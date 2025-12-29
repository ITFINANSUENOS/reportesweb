import polars as pl
import json
import os
from datetime import datetime, date
from src.services.tableros.cartera.cartera_analytics_service import CarteraAnalyticsService
from src.services.tableros.seguimientos.seguimientos_analytics_service import SeguimientosAnalyticsService

class DataProcessorService:
    
    def guardar_parquet_optimizado(self, df: pl.DataFrame, output_path: str):
        """
        Guarda una versión comprimida y ligera para el motor de búsqueda.
        """
        try:
            # Selecciona SOLO las columnas que el usuario va a buscar o ver en la tabla de resultados
            cols_deseadas = [
                "Credito", "Cedula_Cliente", "Nombre_Cliente", "Celular", 
                "Empresa", "Regional_Venta", "Regional_Cobro", "Zona", 
                "Total_Recaudo", "Dias_Atraso_Final", "Valor_Vencido", 
                "Nombre_Ciudad", "Gestor", "Valor_Saldo", "Franja_Cartera",
                "CALL_CENTER_FILTRO", "Call_Center_Apoyo"
            ]
            
            # Intersección de columnas (solo las que existen)
            cols_finales = [c for c in cols_deseadas if c in df.columns]
            
            if not cols_finales:
                return False

            # Guardar Parquet comprimido (Snappy es rápido para leer en AWS)
            df.select(cols_finales).write_parquet(output_path, compression="snappy")
            print(f"💾 Parquet de búsqueda generado: {output_path}")
            return True
        except Exception as e:
            print(f"❌ Error guardando Parquet: {e}")
            return False
        
    def _calcular_columna_call_center_filtro(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Calcula la lógica de negocio de Call Center para filtros.
        VERSIÓN SEGURA: Maneja casos donde no existan las columnas Zona o Apoyo.
        """
        if "Zona" not in df.columns:
            df = df.with_columns(pl.lit("SIN ZONA").alias("Zona"))
            
        if "Call_Center_Apoyo" not in df.columns:
            df = df.with_columns(pl.lit("SIN APOYO").alias("Call_Center_Apoyo"))

        df = df.with_columns([
            pl.col("Zona").fill_null("").cast(pl.Utf8).str.replace_all(" ", "").str.strip_chars().str.to_uppercase(),
            pl.col("Call_Center_Apoyo").fill_null("").cast(pl.Utf8).str.replace_all(" ", "").str.strip_chars().str.to_uppercase()
        ])

        zonas_cl = ['CL1', 'CL2', 'CL3', 'CL4']
        apoyo_cl = ['CL5', 'CL6', 'CL7', 'CL8', 'CL9']

        df = df.with_columns(
            pl.when(pl.col("Zona").is_in(zonas_cl))
            .then(pl.col("Zona"))
            .when(pl.col("Call_Center_Apoyo").is_in(apoyo_cl))
            .then(pl.col("Call_Center_Apoyo"))
            .otherwise(pl.lit("SIN CALL CENTER"))
            .alias("CALL_CENTER_FILTRO")
        )
        
        return df

    def procesar_excel_multi_modulo(self, file_path: str) -> dict:
        resultados_modulos = {}
        df_cartera = pl.DataFrame()
        df_novedades = pl.DataFrame()
        
        common_options = {
            "infer_schema_length": 10000,
            # REMOVED "ANTICIPADO" from null_values so it is read as a valid string
            "null_values": [
                "NO APLICA", "No Aplica", "no aplica", "NO ASIGNADO", "No Asignado",
                "NA", "N/A", "SIN DATO", "null", "nan", "NaN", 
                "VIGENCIA EXPIRADA", "Vigencia Expirada"
            ]
        }

        # --- 1. LEER CARTERA ---
        try:
             # READ EVERYTHING AS UTF8 FIRST to catch "ANTICIPADO"
             df_cartera = pl.read_excel(
                file_path, 
                sheet_name="Analisis_de_Cartera",
                engine="xlsx2csv",
                read_csv_options={
                    **common_options,
                    "schema_overrides": {
                        "Cedula_Cliente": pl.Utf8,
                        "Credito": pl.Utf8,
                        "Fecha_Cuota_Vigente": pl.Utf8, # Important: Read as String
                        "Celular": pl.Utf8,
                        "Valor_Desembolso": pl.Float64,
                        "Valor_Cuota_Vigente": pl.Utf8,
                        "Valor_Cuota": pl.Float64,
                        "Valor_Cuota_Atraso": pl.Float64,
                        "Total_Recaudo": pl.Float64,
                        "Cantidad_Novedades": pl.Float64,
                        "Meta_General": pl.Float64,
                        "Valor_Vencido": pl.Float64,
                        "Meta_$": pl.Float64,
                        "Meta_Saldo": pl.Float64,
                        "Saldo_Capital": pl.Float64,
                        "Saldo_Interes_Corriente": pl.Float64,
                        "Saldo_Avales": pl.Float64,
                        "Dias_Atraso_Final": pl.Float64
                    }
                }
            )
             
             cols_moneda_texto = ["Valor_Cuota_Vigente"]
             
             for col in cols_moneda_texto:
                 if col in df_cartera.columns:
                     df_cartera = df_cartera.with_columns(
                         pl.col(col)
                           .str.replace("ANTICIPADO", "0") # Si dice ANTICIPADO, vale 0 pesos
                           .str.replace("anticipado", "0")
                           .cast(pl.Float64, strict=False) # Ahora sí, convertir a número
                           .fill_null(0) # Cualquier otro error se vuelve 0
                     )

             # --- 1.2 LÓGICA DE VIGENCIA (Recuperar etiqueta) ---
             df_cartera = df_cartera.with_columns(
                 pl.when(pl.col("Fecha_Cuota_Vigente").str.to_uppercase().str.contains("ANTICIPADO"))
                 .then(pl.lit("ANTICIPADO"))
                 .otherwise(pl.lit("FECHA"))
                 .alias("Tipo_Vigencia_Temp")
             )

             # --- 1.3 CONVERSIÓN DE FECHAS ---
             df_cartera = df_cartera.with_columns(
                 pl.col("Fecha_Cuota_Vigente")
                   .str.strptime(pl.Date, "%Y-%m-%d", strict=False) 
                   .alias("Fecha_Cuota_Vigente") 
             )
             
             if "Fecha_Desembolso" in df_cartera.columns:
                 df_cartera = df_cartera.with_columns(pl.col("Fecha_Desembolso").str.strptime(pl.Date, "%Y-%m-%d", strict=False))

        except Exception as e:
             print(f"⚠️ Error leyendo Cartera: {e}")

        # --- 2. LEER NOVEDADES ---
        try:
            df_novedades = pl.read_excel(
                file_path, 
                sheet_name="Detalle_Novedades",
                engine="xlsx2csv",
                read_csv_options={
                    **common_options,
                    "schema_overrides": {
                        "Cedula_Cliente": pl.Utf8,
                        "Cargo_Usuario": pl.Utf8,
                        "Tipo_Novedad": pl.Utf8,
                        "Novedad": pl.Utf8,
                        "Valor": pl.Float64,
                        "Celular_Cliente": pl.Utf8
                    }
                }
            )
        except Exception as e:
            print(f"⚠️ Error leyendo Novedades: {e}")

        # --- 3. PRE-PROCESAMIENTO GLOBAL ---
        if not df_cartera.is_empty():
            try:
                df_cartera = self._calcular_columna_call_center_filtro(df_cartera)
                
                if "Regional_Cobro" in df_cartera.columns:
                    df_cartera = df_cartera.with_columns(
                        pl.col("Regional_Cobro").fill_null("OTRAS ZONAS")
                    )

                df_cartera = df_cartera.with_columns(
                    (
                        pl.col("Saldo_Capital").fill_null(0) + 
                        pl.col("Saldo_Interes_Corriente").fill_null(0) + 
                        pl.col("Saldo_Avales").fill_null(0)
                    ).alias("Valor_Saldo")
                )

            except Exception as e:
                print(f"❌ Error crítico en pre-procesamiento de Cartera: {e}")

        # --- 4. EJECUCIÓN DE MÓDULOS ---
        
        if not df_cartera.is_empty():
            try:
                print("📊 ANALYTICS: Calculando métricas de Cartera...")
                analytics_cartera = CarteraAnalyticsService()
                resultados_modulos["cartera"] = analytics_cartera.calcular_metricas_tablero_principal(df_cartera)
            except Exception as e:
                print(f"❌ ERROR EN MODULO CARTERA: {e}")
                resultados_modulos["cartera"] = {"error": str(e)}

        if not df_cartera.is_empty():
            try:
                print("📊 ANALYTICS: Calculando métricas de Seguimientos...")
                analytics_seg = SeguimientosAnalyticsService()
                resultados_modulos["seguimientos"] = analytics_seg.calcular_metricas_seguimientos(df_cartera, df_novedades)
            except Exception as e:
                print(f"❌ ERROR EN MODULO SEGUIMIENTOS: {e}")
                resultados_modulos["seguimientos"] = {"error": str(e)}

        # --- 5. GENERAR BASE DE DATOS PARA BUSCADOR ---
        if not df_cartera.is_empty():
            try:
                parquet_filename = "temp_data_searchable.parquet"
                if self.guardar_parquet_optimizado(df_cartera, parquet_filename):
                    resultados_modulos["_archivo_parquet"] = parquet_filename
            except Exception as e:
                print(f"⚠️ No se pudo generar archivo de búsqueda: {e}")

        return resultados_modulos

    def guardar_json_resultado(self, data: dict, output_path: str):
        def json_serial(obj):
            if isinstance(obj, (datetime, date)):
                return obj.isoformat()
            raise TypeError (f"Type {type(obj)} not serializable")

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4, default=json_serial)