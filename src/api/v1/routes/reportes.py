from fastapi import APIRouter, HTTPException, BackgroundTasks
import boto3
import json
from src.core.config import settings
from src.controllers.reportes import ReportesController
from src.schemas.request_models import GenerarUrlRequest, IniciarProcesamientoRequest
from src.schemas.response_models import UrlSubidaResponse, ProcesamientoResponse

router = APIRouter()
controller = ReportesController()

@router.get("/activo")
def obtener_reporte_activo():
    """Retorna la configuración del último reporte procesado."""
    s3 = boto3.client(
        's3', 
        region_name=settings.AWS_REGION, 
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID, 
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
    )
    try:
        response = s3.get_object(Bucket=settings.S3_BUCKET_NAME, Key="config/reporte_activo.json")
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)
    except s3.exceptions.NoSuchKey:
        raise HTTPException(status_code=404, detail="No hay reportes activos todavía.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo configuración: {str(e)}")

# 1. Generar URL de Subida
@router.post("/generar-url-subida", response_model=UrlSubidaResponse)
def generar_url_subida(payload: GenerarUrlRequest):
    return controller.generar_url_subida(
        payload.filename, 
        payload.content_type, 
        payload.file_size
    )

# 2. Iniciar Procesamiento
@router.post("/iniciar-procesamiento", response_model=ProcesamientoResponse)
async def iniciar_procesamiento(payload: IniciarProcesamientoRequest, background_tasks: BackgroundTasks):
    return await controller.iniciar_procesamiento_async(
        payload.file_key, 
        payload.empresa, 
        payload.tipo_reporte, 
        background_tasks
    )

@router.get("/contenido/{job_id}/{modulo}")
def obtener_contenido_grafico(job_id: str, modulo: str):
    return controller.obtener_json_graficos(job_id, modulo)