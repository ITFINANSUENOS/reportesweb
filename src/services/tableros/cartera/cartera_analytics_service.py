import polars as pl
from datetime import datetime
from src.services.tableros.cartera.cartera_constants import ZONA_COBRO_MAP

class CarteraAnalyticsService:
    
    def calcular_metricas_tablero_principal(self, df: pl.DataFrame) -> dict:
        if df.is_empty():
            return {}

        print("📊 ANALYTICS: Generando Cubos de Datos para el Tablero Principal...")

        posibles_filtros = [
            "Empresa", "Regional_Cobro", "Zona", "Franja_Cartera", 
            "CALL_CENTER_FILTRO", "Call_Center_Apoyo", "Regional_Venta"
        ]
        cols_filtro = [c for c in posibles_filtros if c in df.columns]

        # 2. CUBO GENERAL
        agg_regional = (
            df.group_by(cols_filtro + ["Franja_Meta"]) 
            .agg([
                pl.len().alias("count"),
                pl.col("Total_Recaudo").sum().alias("Total_Recaudo"), 
                pl.col("Valor_Saldo").sum().alias("Saldo_Total")
            ])
        )

        # 3. CUBO COBRO
        agg_cobro = self._calcular_cobro(df, cols_filtro)

        # 4. CUBO DESEMBOLSO
        agg_desembolso = self._calcular_desembolso(df, cols_filtro)

        # 5. CUBO VIGENCIA
        agg_vigencia = self._calcular_vigencia(df, cols_filtro)

        return {
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
        # We need Tipo_Vigencia_Temp which we created in the processor service
        if "Tipo_Vigencia_Temp" not in df.columns: 
            # Fallback if column is missing (should not happen with correct processor)
            if "Fecha_Cuota_Vigente" not in df.columns: return None
            df = df.with_columns(pl.lit("FECHA").alias("Tipo_Vigencia_Temp"))

        return (
            df.with_columns(
                # 1. Logic for Parent State
                pl.when(pl.col("Tipo_Vigencia_Temp") == "ANTICIPADO")
                .then(pl.lit("ANTICIPADO"))
                .when(pl.col("Fecha_Cuota_Vigente").is_not_null())
                .then(pl.lit("VIGENTES"))
                .otherwise(pl.lit("VIGENCIA EXPIRADA"))
                .alias("Estado_Vigencia_Agrupado")
            )
            .with_columns(
                # 2. Logic for Child State (RANGES for VIGENTES)
                pl.when(pl.col("Estado_Vigencia_Agrupado") == "VIGENTES")
                .then(
                    pl.when(pl.col("Fecha_Cuota_Vigente").dt.day() <= 10).then(pl.lit("Días 1-10"))
                    .when(pl.col("Fecha_Cuota_Vigente").dt.day() <= 20).then(pl.lit("Días 11-20"))
                    .otherwise(pl.lit("Días 21+"))
                )
                .otherwise(pl.lit(None)) # Anticipado and Expirada have no children
                .alias("Sub_Estado_Vigencia")
            )
            .group_by(cols_filtro + ["Estado_Vigencia_Agrupado", "Sub_Estado_Vigencia"])
            .len()
            .rename({"len": "count"})
        )