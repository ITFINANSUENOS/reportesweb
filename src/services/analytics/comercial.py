import polars as pl
from datetime import date, timedelta

class ComercialAnalyticsService:
    
    # 1. LÓGICA DE NEGOCIO (Generación de DataFrames para Parquets)
    def calcular_df_retanqueos(self, df_cartera: pl.DataFrame) -> pl.DataFrame:
        """Filtra la cartera para encontrar candidatos a Retanqueo."""
        if df_cartera is None or df_cartera.is_empty(): return None

        # Filtros base
        df_pot = (
            df_cartera
            .filter(pl.col("Dias_Atraso_Final") <= 30)
            .filter(pl.col("Total_Cuotas") >= 6)
            .with_columns((pl.col("Total_Cuotas") - pl.col("Cuotas_Pagadas")).alias("Cuotas_Restantes"))
        )
        
        # Reglas de negocio complejas
        cond_a = (pl.col("Total_Cuotas").is_between(6, 8) & pl.col("Cuotas_Restantes").is_between(1, 2))
        cond_b = ((pl.col("Total_Cuotas") > 8) & pl.col("Cuotas_Restantes").is_between(1, 4))
        
        # Retorna el DataFrame filtrado (listo para guardar como Parquet)
        return df_pot.filter(cond_a | cond_b)

    def calcular_df_cosechas(self, df_cartera: pl.DataFrame) -> pl.DataFrame:
        """Filtra la cartera para encontrar riesgo temprano (Cosechas < 6 meses)."""
        if df_cartera is None or df_cartera.is_empty(): return None
        if "Fecha_Desembolso" not in df_cartera.columns: return None

        fecha_corte = date.today() - timedelta(days=180)
        df_recent = df_cartera.filter(pl.col("Fecha_Desembolso") >= fecha_corte)
        
        if df_recent.is_empty(): return None

        # Retorna el DataFrame clasificado (listo para guardar como Parquet)
        return df_recent.filter(pl.col("Dias_Atraso_Final") > 0).with_columns(
            pl.when(pl.col("Cuotas_Pagadas") == 0).then(pl.lit("SECCION_1_SIN_PAGO"))
            .when(pl.col("Cuotas_Pagadas") == 1).then(pl.lit("SECCION_2_FALLO_2DA"))
            .when(pl.col("Cuotas_Pagadas").is_between(2, 5)).then(pl.lit("SECCION_3_FALLO_3RA_PLUS"))
            .otherwise(pl.lit("OK")).alias("Grupo_Seguimiento")
        ).filter(pl.col("Grupo_Seguimiento") != "OK")

    # 2. GENERACIÓN DE JSON LIGERO
    def generar_json_comercial(self, df_fnz: pl.DataFrame, df_retanqueos: pl.DataFrame = None, df_cosechas: pl.DataFrame = None) -> dict:
        """
        Genera SOLO el resumen necesario para visualización y contadores META.
        """
        
        # Calculamos contadores específicos para Cosechas
        meta_cosechas = { "s1": 0, "s2": 0, "s3": 0 }
        
        if df_cosechas is not None and not df_cosechas.is_empty():
            # Hacemos un group_by rápido para contar
            conteos = df_cosechas.group_by("Grupo_Seguimiento").len().to_dicts()
            for c in conteos:
                if c["Grupo_Seguimiento"] == "SECCION_1_SIN_PAGO": meta_cosechas["s1"] = c["len"]
                elif c["Grupo_Seguimiento"] == "SECCION_2_FALLO_2DA": meta_cosechas["s2"] = c["len"]
                elif c["Grupo_Seguimiento"] == "SECCION_3_FALLO_3RA_PLUS": meta_cosechas["s3"] = c["len"]

        return {
            # Solo enviamos esto para que el Front haga la matriz y los filtros
            "fnz_resumen": self._procesar_resumen_fnz(df_fnz),
            
            # Metadata ligera para los badges de los títulos
            "meta": {
                "total_retanqueos": df_retanqueos.height if df_retanqueos is not None else 0,
                "total_cosechas": df_cosechas.height if df_cosechas is not None else 0,
                "cosechas_detalle": meta_cosechas
            }
        }

    def _procesar_resumen_fnz(self, df_fnz: pl.DataFrame) -> list:
        """
        Selecciona estrictamente lo necesario para el gráfico de barras y la tabla pivote.
        """
        if df_fnz is None or df_fnz.is_empty(): return []

        cols_necesarias = [
            'Nombre_Vendedor',  # Eje Y (Filas)
            'Estado',           # Eje X (Columnas)
            'Regional_Venta',   # Filtro Global
            'Analista_Asociado' # Filtro Global
        ]
        
        cols_finales = [c for c in cols_necesarias if c in df_fnz.columns]
        
        # Convertimos fechas a string para evitar el error JSON (aunque aquí no hay fechas seleccionadas, es buena práctica)
        return self._exportar_a_json(df_fnz.select(cols_finales))

    # --- HELPER: FORMATO JSON SEGURO (FECHAS) ---
    def _exportar_a_json(self, df: pl.DataFrame) -> list:
        if df is None or df.is_empty(): return []
        # Convierte todas las columnas tipo Date a String YYYY-MM-DD
        df_clean = df.with_columns(pl.col(pl.Date).dt.to_string("%Y-%m-%d"))
        return df_clean.to_dicts()