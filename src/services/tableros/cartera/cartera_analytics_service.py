import polars as pl
import io
import os
import boto3
from datetime import datetime
from dotenv import load_dotenv
from src.services.tableros.cartera.cartera_constants import ZONA_COBRO_MAP

load_dotenv()

class CarteraAnalyticsService:
    def __init__(self):
        self.bucket_name = os.getenv('S3_BUCKET_NAME')
        self.s3_client = boto3.client('s3', region_name=os.getenv('AWS_REGION'))

    def _leer_parquet(self, file_key: str) -> pl.DataFrame:
        print(f"📊 Descargando y leyendo: {file_key}")
        # Leemos el objeto desde S3 directo a memoria RAM
        response = self.s3_client.get_object(Bucket=self.bucket_name, Key=file_key)
        # Retornamos el DataFrame de Polars
        return pl.read_parquet(io.BytesIO(response['Body'].read()))

    def generar_data_tablero(self, file_key: str):
        """
        Orquestador: Trae el DF una vez y calcula las 4 vistas.
        """
        df = self._leer_parquet(file_key)
        
        return {
            "regional": self._analisis_regional(df),
            "cobro": self._analisis_cobro(df),
            "desembolso": self._analisis_desembolso(df),
            "vigencia": self._analisis_vigencia(df)
        }

    # 1. GRÁFICA REGIONAL
    def _analisis_regional(self, df: pl.DataFrame):
        q = (
            df.lazy()
            .group_by(["Regional_Venta", "Franja_Meta"])
            .len()
            .collect()
        )
        return q.to_dicts()

    # 2. GRÁFICA DE COBRO (Manejo de Nulos y Mapeo)
    def _analisis_cobro(self, df: pl.DataFrame):
        # Validación defensiva por si cambia el nombre de columnas en el Excel
        if "Regional_Cobro" not in df.columns or "Zona_Cobro" not in df.columns:
            return []

        q = (
            df.lazy()
            .with_columns([
                # 💡 replace es estricto. Si no encuentra la llave, deja el valor original.
                # Es importante asegurar que Zona_Cobro sea string.
                pl.col("Zona_Cobro").cast(pl.Utf8).replace(ZONA_COBRO_MAP).alias("Zona_Mapeada")
            ])
            .with_columns([
                # Coalesce: Toma el primer valor no nulo de la lista
                pl.coalesce([pl.col("Regional_Cobro"), pl.col("Zona_Mapeada")]).alias("Eje_X_Cobro")
            ])
            .filter(pl.col("Eje_X_Cobro").is_not_null())
            .group_by(["Eje_X_Cobro", "Franja_Meta"])
            .len()
            .collect()
        )
        return q.to_dicts()

    # 3. GRÁFICA DESEMBOLSOS (Histórico)
    def _analisis_desembolso(self, df: pl.DataFrame):
        if "Fecha_Desembolso" not in df.columns:
            return []

        current_year = datetime.now().year
        
        q = (
            df.lazy()
            .filter(pl.col("Fecha_Desembolso").is_not_null())
            .with_columns(pl.col("Fecha_Desembolso").dt.year().alias("Año_Desembolso"))
            .filter(
                (pl.col("Año_Desembolso") >= 2018) & 
                (pl.col("Año_Desembolso") <= current_year)
            )
            .group_by(["Año_Desembolso", "Franja_Meta"])
            .agg(pl.col("Valor_Desembolso").sum()) # Sumamos el dinero
            .sort("Año_Desembolso")
            .collect()
        )
        return q.to_dicts()

    # 4. GRÁFICA VIGENCIA (Sunburst / Drilldown)
    def _analisis_vigencia(self, df: pl.DataFrame):
        """
        Lógica:
        - Si es una fecha (ej: 2025-01-15) -> Agrupar como "VIGENTES" -> Subgrupo: "Día 15"
        - Si es texto (ej: CASTIGADA, PREJURIDICO) -> Agrupar como ese texto -> Subgrupo: ""
        """
        if "Fecha_Cuota_Vigente" not in df.columns:
            return []
            
        q = (
            df.lazy()
            # 💡 Paso 1: Convertir todo a String para analizar sin errores de tipo
            .with_columns(pl.col("Fecha_Cuota_Vigente").cast(pl.Utf8).alias("Temp_Str"))
            
            # 💡 Paso 2: Clasificar (Padre)
            # Detectamos si parece una fecha (contiene guion y empieza por 20..)
            .with_columns(
                pl.when(pl.col("Temp_Str").str.contains(r"^\d{4}-")) 
                .then(pl.lit("VIGENTES"))
                .otherwise(pl.col("Temp_Str"))
                .alias("Estado_Padre")
            )
            
            # 💡 Paso 3: Extraer Día (Hijo) solo para las vigentes
            .with_columns(
                pl.when(pl.col("Estado_Padre") == "VIGENTES")
                .then(
                    # Intentamos parsear la fecha y sacar el día
                    pl.format("Día {}", 
                        pl.col("Temp_Str")
                        .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
                        .dt.day()
                    )
                )
                .otherwise(pl.lit("General")) # Para Castigada, etc, el hijo es "General"
                .alias("Estado_Hijo")
            )
            # Filtramos basura (nulos o vacíos)
            .filter(pl.col("Estado_Padre").is_not_null())
            .group_by(["Estado_Padre", "Estado_Hijo"])
            .len()
            .collect()
        )
        return q.to_dicts()