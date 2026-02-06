import polars as pl
import io

class DetalladosAnalyticsService:
    """
    Servicio encargado de la lógica del módulo 'Explorador de Datos'.
    Principalmente utilidades de exportación, ya que la visualización
    se maneja vía Parquet/Buscador.
    """
    
    def generar_excel_descarga(self, df: pl.DataFrame, sheet_name: str = "Datos") -> bytes:
        """
        Convierte un DataFrame de Polars a un archivo Excel binario (.xlsx)
        listo para ser retornado por un endpoint de descarga.
        
        Args:
            df: DataFrame filtrado (desde el buscador o crudo).
            sheet_name: Nombre de la hoja en el Excel.
        """
        output = io.BytesIO()
        try:
            # Polars escribe Excel nativamente mucho más rápido que Pandas
            df.write_excel(output, worksheet=sheet_name)
            output.seek(0)
            return output.getvalue()
        except Exception as e:
            print(f"❌ Error generando Excel en DetalladosService: {e}")
            return b""