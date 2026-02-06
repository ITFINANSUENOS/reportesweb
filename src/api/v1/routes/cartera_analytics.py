from fastapi import APIRouter, Query
from src.controllers.cartera_analytics import CarteraAnalyticsController

router = APIRouter()
controller = CarteraAnalyticsController()

@router.get("/dashboard-principal")
async def obtener_dashboard(file_key: str = Query(..., description="Key del archivo S3")):
    """
    Retorna toda la data agregada para los 4 gráficos del Tablero de Cartera.
    """
    return await controller.get_tablero_principal(file_key)