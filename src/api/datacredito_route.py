# src/api/datacredito_route.py

import os
import shutil
import uuid
# Importa 'Form' para recibir 'empresa'
from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks, Form
from fastapi.responses import FileResponse
# ¡CAMBIO IMPORTANTE! Importamos nuestro nuevo servicio de API
from src.api.datacredito_service import DataCreditoApiService

router = APIRouter()
# ¡CAMBIO IMPORTANTE! Creamos una instancia de nuestro servicio de API
api_service = DataCreditoApiService()

def cleanup_temp_folder(folder_path: str):
    """
    Elimina de forma segura la carpeta temporal y su contenido.
    """
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)
        print(f"Limpieza: Carpeta temporal {folder_path} eliminada.")

@router.post("/datacredito/process", tags=["Datacrédito"])
async def procesar_reporte_datacredito(
    background_tasks: BackgroundTasks,
    # Añadimos 'empresa' como un campo de formulario obligatorio
    empresa: str = Form(...),
    archivo_plano: UploadFile = File(...),
    archivo_correcciones: UploadFile = File(...)
):
    """
    Recibe los archivos y el tipo de empresa, los pasa al servicio de la API
    para su procesamiento y devuelve el reporte final.
    """
    temp_dir = f"temp_{uuid.uuid4().hex}"
    os.makedirs(temp_dir)

    try:
        # Guarda los archivos subidos
        plano_path = os.path.join(temp_dir, archivo_plano.filename)
        corrections_path = os.path.join(temp_dir, archivo_correcciones.filename)
        output_filename = f"Resultado_{empresa}_{os.path.splitext(archivo_plano.filename)[0]}.xlsx"
        output_path = os.path.join(temp_dir, output_filename)

        with open(plano_path, "wb") as f:
            shutil.copyfileobj(archivo_plano.file, f)
        with open(corrections_path, "wb") as f:
            shutil.copyfileobj(archivo_correcciones.file, f)

        # ¡CAMBIO IMPORTANTE! La ruta ahora llama al método del servicio de API
        api_service.process_files_for_api(
            plano_path=plano_path,
            correcciones_path=corrections_path,
            output_path=output_path,
            empresa=empresa
        )

        # Registra la tarea de limpieza para que se ejecute después de la respuesta
        background_tasks.add_task(cleanup_temp_folder, temp_dir)

        # Devuelve el resultado
        return FileResponse(
            path=output_path,
            filename=output_filename,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except Exception as e:
        # Si algo falla, limpia y lanza el error HTTP
        cleanup_temp_folder(temp_dir)
        raise HTTPException(status_code=500, detail=f"Ocurrió un error interno en el servidor: {e}")