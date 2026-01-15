from fastapi import APIRouter, HTTPException,BackgroundTasks
import boto3
import json
from src.core.config import settings
from src.controllers.api.reportes_controller import ReportesController

router = APIRouter()
controller = ReportesController()

@router.get("/activo")
def obtener_reporte_activo():
    """
    Retorna el job_id del último reporte procesado correctamente.
    Los usuarios 'no-admin' usarán esto para saber qué cargar.
    """
    s3 = boto3.client(
        's3', 
        region_name=settings.AWS_REGION, 
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID, 
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
    )
    
    try:
        # Leemos el archivo fijo
        response = s3.get_object(Bucket=settings.S3_BUCKET_NAME, Key="config/reporte_activo.json")
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)
        
        return data 
        
    except s3.exceptions.NoSuchKey:
        # Caso: Es la primera vez y nunca se ha subido nada
        raise HTTPException(status_code=404, detail="No hay reportes activos todavía.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo configuración: {str(e)}")

# 1. Endpoint para pedir permiso de subida (Generar URL)
@router.post("/generar-url-subida")
def generar_url_subida(data: dict):
    filename = data.get("filename")
    content_type = data.get("content_type")
    if not filename or not content_type:
        raise HTTPException(status_code=400, detail="Faltan datos")
    return controller.generar_url_subida(filename, content_type)

# 2. Endpoint para activar el Worker (Iniciar procesamiento)
@router.post("/iniciar-procesamiento")
async def iniciar_procesamiento(data: dict, background_tasks: BackgroundTasks): # <--- AGREGAR EL PARÁMETRO
    # Esperamos {"file_key": "uploads/...", "empresa": "..."}
    file_key = data.get("file_key")
    empresa = data.get("empresa")
    # Capturamos el tipo, por defecto SEGUIMIENTOS
    tipo_reporte = data.get("tipo_reporte", "SEGUIMIENTOS") 
    
    if not file_key or not empresa:
        raise HTTPException(status_code=400, detail="Faltan datos")
    # Pasamos background_tasks al controlador
    return await controller.iniciar_procesamiento_async(
        file_key, 
        empresa, 
        tipo_reporte, 
        background_tasks
    )

@router.get("/contenido/{job_id}/{modulo}")
def obtener_contenido_grafico(job_id: str, modulo: str):
    return controller.obtener_json_graficos(job_id, modulo)
