from fastapi import APIRouter, BackgroundTasks
from src.controllers.api.datacredito_controller import DataCreditoController

router = APIRouter()
controller = DataCreditoController()

@router.post("/generar_urls_subida", summary="Obtener URLs firmadas para S3")
def generar_urls(data: dict):
    return controller.generar_urls_subida(data)

@router.post("/iniciar_procesamiento", summary="Iniciar cruce DataCredito")
def iniciar_proceso(data: dict, background_tasks: BackgroundTasks):
    return controller.iniciar_procesamiento(data, background_tasks)

@router.get("/estado_procesamiento", summary="Consultar si terminó el proceso")
def consultar_estado(key: str):
    return controller.consultar_estado(key)