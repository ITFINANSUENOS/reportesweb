import polars as pl
from datetime import datetime
from src.core.constants import ZONA_COBRO_MAP

class CarteraAnalyticsService:
    
    # Extrae los valores únicos para los filtros en milisegundos
    def _extraer_opciones_filtros(self, df: pl.DataFrame) -> dict:
        """Extrae los valores únicos de las columnas para los selects del Frontend"""
        def obtener_unicos(nombres_posibles):
            # Busca la primera columna que exista en el DataFrame
            for col in nombres_posibles:
                if col in df.columns:
                    # Saca únicos, quita nulos y ordena alfabéticamente
                    return df.select(pl.col(col).drop_nulls().unique().sort()).to_series().to_list()
            return []

        return {
            "empresas": obtener_unicos(["Empresa"]),
            "zonas": obtener_unicos(["Zona"]),
            "regionales": obtener_unicos(["Regional_Cobro", "Regional_Venta", "Regional"]),
            "call_centers": obtener_unicos(["CALL_CENTER_FILTRO", "CALL_CENTER", "Call_Center"]),
            "franjas": obtener_unicos(["Franja_Cartera", "Franja_Meta", "Franja"]),
            "estados_vigencia": ["vigente", "vencido", "anticipado"]
        }

    def calcular_metricas_tablero_principal(self, df: pl.DataFrame) -> dict:
        if df.is_empty():
            return {}

        print("📊 ANALYTICS: Generando Cubos de Datos para el Tablero Principal...")

        # 1. SACAR FILTROS RÁPIDOS ANTES DE AGRUPAR
        filtros_limpios = self._extraer_opciones_filtros(df)

        posibles_filtros = [
            "Empresa", "Regional_Cobro", "Zona", "Franja_Cartera", 
            "CALL_CENTER_FILTRO", "Call_Center_Apoyo", "Regional_Venta",
            "Estado_Vigencia"
        ]
        cols_filtro = [c for c in posibles_filtros if c in df.columns]

        # Agregar Estado_Vigencia si no existe
        if "Estado_Vigencia" not in df.columns:
            df = self.agregar_estado_vigencia(df)

        # 2. CUBO GENERAL
        agg_regional = (
            df.group_by(cols_filtro + ["Franja_Meta"]) 
            .agg([
                pl.len().alias("count"),
                pl.col("Total_Recaudo").sum().alias("Total_Recaudo"), 
            ])
        )

        # 3. CUBO COBRO
        agg_cobro = self._calcular_cobro(df, cols_filtro)

        # 4. CUBO DESEMBOLSO
        agg_desembolso = self._calcular_desembolso(df, cols_filtro)

        # 5. CUBO VIGENCIA
        agg_vigencia = self._calcular_vigencia(df, cols_filtro)

        # RETORNO INTACTO Solo agregamos la llave de filtros_disponibles
        return {
            "filtros_disponibles": filtros_limpios,
            "cubo_regional": agg_regional.to_dicts(),
            "cubo_cobro": agg_cobro.to_dicts() if agg_cobro is not None else [],
            "cubo_desembolso": agg_desembolso.to_dicts() if agg_desembolso is not None else [],
            "cubo_vigencia": agg_vigencia.to_dicts() if agg_vigencia is not None else []
        }

    def _calcular_cobro(self, df: pl.DataFrame, cols_filtro: list):
        if "Zona_Cobro" not in df.columns: return None

        df_temp = (
            df.with_columns(
                pl.col("Zona_Cobro").replace(ZONA_COBRO_MAP, default=None).alias("Zona_Mapeada")
            )
            .with_columns(
                pl.coalesce(["Zona_Mapeada", "Regional_Cobro"]).alias("Eje_X_Cobro")
            )
            .filter(pl.col("Eje_X_Cobro").is_not_null())
        )

        return (
            df_temp.group_by(cols_filtro + ["Eje_X_Cobro", "Franja_Meta"])
            .len()
            .rename({"len": "count"})
        )

    def _calcular_desembolso(self, df: pl.DataFrame, cols_filtro: list):
        if "Fecha_Desembolso" not in df.columns: return None
            
        current_year = datetime.now().year
        
        return (
            df.filter(pl.col("Fecha_Desembolso").is_not_null())
            .with_columns(pl.col("Fecha_Desembolso").dt.year().alias("Año_Desembolso"))
            .filter(pl.col("Año_Desembolso").is_between(2018, current_year))
            .group_by(cols_filtro + ["Año_Desembolso", "Franja_Meta"])
            .agg(pl.col("Valor_Desembolso").sum())
            .with_columns(pl.col("Valor_Desembolso").round(0).cast(pl.Int64))
        )

    def _calcular_vigencia(self, df: pl.DataFrame, cols_filtro: list):
        if "Tipo_Vigencia_Temp" not in df.columns: 
            if "Fecha_Cuota_Vigente" not in df.columns: return None
            df = df.with_columns(pl.lit("FECHA").alias("Tipo_Vigencia_Temp"))

        return (
            df.with_columns(
                pl.when(pl.col("Tipo_Vigencia_Temp") == "ANTICIPADO")
                .then(pl.lit("anticipado"))
                .when(pl.col("Fecha_Cuota_Vigente").is_not_null())
                .then(pl.lit("vigente"))
                .otherwise(pl.lit("vencido"))
                .alias("Estado_Vigencia_Agrupado")
            )
            .with_columns(
                pl.when(pl.col("Estado_Vigencia_Agrupado") == "vigente")
                .then(
                    pl.when(pl.col("Fecha_Cuota_Vigente").dt.day() <= 10).then(pl.lit("Días 1-10"))
                    .when(pl.col("Fecha_Cuota_Vigente").dt.day() <= 20).then(pl.lit("Días 11-20"))
                    .otherwise(pl.lit("Días 21+"))
                )
                .otherwise(pl.lit(None))
                .alias("Sub_Estado_Vigencia")
            )
            .group_by(cols_filtro + ["Estado_Vigencia_Agrupado", "Sub_Estado_Vigencia"])
            .len()
            .rename({"len": "count"})
        )

    def agregar_estado_vigencia(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Agrega columna 'Estado_Vigencia' al DataFrame para Parquets y filtros globales.
        Retorna el DataFrame con la nueva columna.
        """
        if df.is_empty():
            return df

        if "Tipo_Vigencia_Temp" not in df.columns:
            if "Fecha_Cuota_Vigente" not in df.columns:
                return df.with_columns(pl.lit(None).alias("Estado_Vigencia"))
            df = df.with_columns(pl.lit("FECHA").alias("Tipo_Vigencia_Temp"))

        return df.with_columns(
            pl.when(pl.col("Tipo_Vigencia_Temp") == "ANTICIPADO")
            .then(pl.lit("anticipado"))
            .when(pl.col("Fecha_Cuota_Vigente").is_not_null())
            .then(pl.lit("vigente"))
            .otherwise(pl.lit("vencido"))
            .alias("Estado_Vigencia")
        )