from fastapi import HTTPException
from src.services.tableros.cartera.cartera_analytics_service import CarteraAnalyticsService

class CarteraAnalyticsController:
    def __init__(self):
        self.service = CarteraAnalyticsService()

    async def get_tablero_principal(self, file_key: str):
        if not file_key.endswith(".parquet"):
             raise HTTPException(status_code=400, detail="Archivo incorrecto")
        
        try:
            # Llamamos al método maestro que trae los 4 datasets
            data = self.service.generar_data_tablero(file_key)
            return {
                "status": "success",
                "data": data
            }
        except Exception as e:
            print(f"Error Analytics: {e}")
            raise HTTPException(status_code=500, detail=str(e))