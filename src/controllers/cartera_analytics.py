from fastapi import HTTPException
from src.services.analytics.cartera import CarteraAnalyticsService
from src.services.storage.s3_service import S3Service

class CarteraAnalyticsController:
    def __init__(self):
        self.service = CarteraAnalyticsService()
        self.s3 = S3Service()

    async def get_tablero_principal(self, file_key: str):
        if not file_key.endswith(".parquet"):
             raise HTTPException(status_code=400, detail="Archivo incorrecto")
        
        try:
            df = self.s3.descargar_parquet(file_key)
            data = self.service.calcular_metricas_tablero_principal(df)
            return {
                "status": "success",
                "data": data
            }
        except Exception as e:
            print(f"Error Analytics: {e}")
            raise HTTPException(status_code=500, detail=str(e))