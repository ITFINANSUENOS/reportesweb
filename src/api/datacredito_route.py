import os
import shutil
import uuid
from fastapi import APIRouter, UploadFile, File, HTTPException , BackgroundTasks
from fastapi.responses import FileResponse
from src.controllers.datacredito_controller import DataCreditoController

# 1. Crea un "router", que es como un mini-aplicativo para las rutas de Datacrédito
router = APIRouter()
# 2. Crea una única instancia del controlador que se usará para estas rutas
datacredito_controller = DataCreditoController()


def cleanup_temp_folder(folder_path: str):
    """
    Elimina de forma segura la carpeta temporal y su contenido.
    """
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)
        print(f"Limpieza: Carpeta temporal {folder_path} eliminada.")

@router.post("/datacredito/process", tags=["Datacrédito"])
async def procesar_reporte_datacredito(
    # 2. Añade BackgroundTasks como un parámetro a la función
    background_tasks: BackgroundTasks,
    archivo_plano: UploadFile = File(...),
    archivo_correcciones: UploadFile = File(...)
):
    """
    Recibe los archivos, los pasa al controlador para su procesamiento,
    y devuelve el reporte final.
    """
    temp_dir = f"temp_{uuid.uuid4().hex}"
    os.makedirs(temp_dir)

    try:
        # Guarda los archivos subidos
        plano_path = os.path.join(temp_dir, archivo_plano.filename)
        corrections_path = os.path.join(temp_dir, archivo_correcciones.filename)
        output_filename = f"Resultado_{os.path.splitext(archivo_plano.filename)[0]}.xlsx"
        output_path = os.path.join(temp_dir, output_filename)

        with open(plano_path, "wb") as f:
            shutil.copyfileobj(archivo_plano.file, f)
        with open(corrections_path, "wb") as f:
            shutil.copyfileobj(archivo_correcciones.file, f)

        # La ruta llama al controlador para que haga el trabajo pesado
        datacredito_controller.process_files(plano_path, corrections_path, output_path)

        # 3. Registra la tarea de limpieza para que se ejecute DESPUÉS de enviar el archivo
        background_tasks.add_task(cleanup_temp_folder, temp_dir)

        # 4. Devuelve el resultado
        return FileResponse(
            path=output_path,
            filename=output_filename,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        # Si algo falla, limpia la carpeta y luego lanza el error
        cleanup_temp_folder(temp_dir)
        raise HTTPException(status_code=500, detail=str(e))