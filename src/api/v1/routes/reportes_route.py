from fastapi import APIRouter, UploadFile, File
from src.controllers.api.reportes_controller import ReportesController

router = APIRouter()
controller = ReportesController()

@router.post("/cargar-general", summary="Cargar y procesar Excel General")
async def cargar_reporte_general(file: UploadFile = File(...)):
    """
    Recibe el Excel Operativo, lo valida, separa las hojas (Cartera, Novedades, etc.),
    las convierte a Parquet y las sube a S3.
    """
    return await controller.cargar_reporte_general(file)