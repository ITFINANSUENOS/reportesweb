import boto3
import uuid
import os
import shutil
import uuid
from fastapi import APIRouter, BackgroundTasks, HTTPException
from src.services.datacredito.datacredito_api_service import DataCreditoApiService 
# --- Configuración (¡Ajusta esto!) ---
router = APIRouter()
s3_client = boto3.client('s3', region_name='us-east-1') # Región de App Runner y S3
BUCKET_NAME = 'finansuenos-reportes-privados' # El nombre de tu bucket S3

def limpiar_reportes_antiguos_s3(prefijo: str, max_archivos: int = 3):
    """
    Mantiene solo los 'max_archivos' más recientes en un prefijo de S3,
    basado en la fecha de última modificación.
    """
    try:
        # 1. Listar todos los objetos en el prefijo (directorio "resultados/")
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefijo)
        
        if 'Contents' not in response:
            print(f"S3_CLEANUP: No hay archivos en '{prefijo}'. No se borra nada.", flush=True)
            return

        # 2. Filtrar solo archivos (excluir "carpetas" vacías si las hubiera)
        archivos_en_s3 = [obj for obj in response['Contents'] if obj['Size'] > 0]

        # 3. Ordenarlos por fecha de modificación (antiguos primero)
        archivos_ordenados = sorted(
            archivos_en_s3, 
            key=lambda obj: obj['LastModified']
        )
        
        # 4. Calcular cuántos archivos hay que borrar
        archivos_a_borrar_count = len(archivos_ordenados) - max_archivos

        if archivos_a_borrar_count <= 0:
            print(f"S3_CLEANUP: Hay {len(archivos_ordenados)} archivos. Límite es {max_archivos}. No se borra nada.", flush=True)
            return

        # 5. Obtener las 'Keys' (nombres) de los archivos más antiguos
        archivos_a_borrar = archivos_ordenados[:archivos_a_borrar_count]
        
        # 6. Preparar la lista para el borrado en lote (batch delete)
        objetos_para_borrar = [{'Key': obj['Key']} for obj in archivos_a_borrar]
        
        print(f"S3_CLEANUP: Hay {len(archivos_ordenados)} archivos. Borrando {len(objetos_para_borrar)} archivos antiguos...", flush=True)

        # 7. Ejecutar el borrado
        s3_client.delete_objects(
            Bucket=BUCKET_NAME,
            Delete={'Objects': objetos_para_borrar}
        )
        
        print(f"S3_CLEANUP: Borrado de {len(objetos_para_borrar)} archivos completado.", flush=True)

    except Exception as e:
        # ¡Importante! Si la limpieza falla, no debe romper la tarea principal.
        print(f"S3_CLEANUP_ERROR: No se pudo limpiar los archivos antiguos. Error: {e}", flush=True)

# ESTA ES LA TAREA PESADA (en segundo plano)
def procesar_archivos_en_segundo_plano(
    plano_key: str, 
    correcciones_key: str, 
    empresa: str,
    output_key: str
): 
    """
    Esta función se ejecuta en segundo plano. NO sufre de timeouts de API.
    1. Descarga los archivos de S3.
    2. Llama a tu servicio de procesamiento (que ahora es optimizado).
    3. Sube el resultado a S3.
    4. Limpia los archivos temporales.
    """
    # Creamos una instancia de tu servicio de lógica de negocio
    service_api = DataCreditoApiService()
    
    # /tmp/ es el único directorio escribible en App Runner
    temp_dir = f"/tmp/{uuid.uuid4().hex}" 
    os.makedirs(temp_dir, exist_ok=True)
    
    plano_path = os.path.join(temp_dir, os.path.basename(plano_key))
    correcciones_path = os.path.join(temp_dir, os.path.basename(correcciones_key))
    output_path = os.path.join(temp_dir, output_key)

    try:
        # 1. Descargar archivos de S3
        print(f"BG_TASK: Descargando archivos a {temp_dir}...", flush=True)
        s3_client.download_file(BUCKET_NAME, plano_key, plano_path)
        s3_client.download_file(BUCKET_NAME, correcciones_key, correcciones_path)
        print("BG_TASK: Archivos descargados.", flush=True)

        # 2. Llamar a tu lógica de servicio (tu código)
        print(f"BG_TASK: Iniciando procesamiento para la empresa {empresa}...", flush=True)
        # Esta función ahora usa el modelo optimizado (chunks)
        service_api.process_files_for_api(
            plano_path=plano_path,
            correcciones_path=correcciones_path,
            output_path=output_path,
            empresa=empresa
        )
        print("BG_TASK: Procesamiento completado.", flush=True)

        # 3. Subir el resultado a S3
        # El archivo de resultado se crea en 'output_path' por tu servicio
        print(f"BG_TASK: Subiendo resultado a S3 en 'resultados/{output_key}'...", flush=True)
        s3_client.upload_file(output_path, BUCKET_NAME, f"resultados/{output_key}")
        print(f"BG_TASK: Resultado subido.", flush=True)
        # Limpiar reportes antiguos en S3
        print(f"BG_TASK: Verificando reportes antiguos en S3 (límite: 3)...", flush=True)
        limpiar_reportes_antiguos_s3(prefijo="resultados/", max_archivos=3)

    except Exception as e:
        print(f"BG_TASK_ERROR: Falló el procesamiento. Error: {e}", flush=True)
    
    finally:
        # 4. Limpiar carpeta temporal
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            print(f"BG_TASK: Carpeta temporal {temp_dir} eliminada.", flush=True)


# --- ENDPOINT 1: Generar URLs de Subida (CORREGIDO) ---
@router.post("/generar_urls_subida", tags=["Procesamiento"])
def generar_urls_subida(data: dict):
    """
    PASO 1: Pide permiso para subir.
    ¡CORREGIDO! Ahora también recibe el 'content_type' del cliente
    para que la firma (signature) coincida.
    """
    plano_filename = data.get("plano_filename")
    correcciones_filename = data.get("correcciones_filename")
    
    # --- ¡NUEVO! ---
    # El cliente (frontend/Postman) AHORA DEBE ENVIAR esto.
    plano_content_type = data.get("plano_content_type")
    correcciones_content_type = data.get("correcciones_content_type")

    # Validación actualizada
    if not all([plano_filename, correcciones_filename, plano_content_type, correcciones_content_type]):
        raise HTTPException(status_code=400, detail="Se requieren plano_filename, correcciones_filename, plano_content_type y correcciones_content_type")

    plano_key = f"uploads/{uuid.uuid4().hex}-{plano_filename}"
    correcciones_key = f"uploads/{uuid.uuid4().hex}-{correcciones_filename}"

    try:
        # --- ¡CORREGIDO! ---
        # Añadimos 'ContentType' a los Parámetros.
        # Boto3 ahora incluirá esto en la firma criptográfica.
        url_plano = s3_client.generate_presigned_url(
            'put_object',
            Params={
                'Bucket': BUCKET_NAME, 
                'Key': plano_key,
                'ContentType': plano_content_type  # <-- Esta es la corrección
            },
            ExpiresIn=3600
        )
        url_correcciones = s3_client.generate_presigned_url(
            'put_object',
            Params={
                'Bucket': BUCKET_NAME, 
                'Key': correcciones_key,
                'ContentType': correcciones_content_type # <-- Esta es la corrección
            },
            ExpiresIn=3600
        )
        
        print(f"INFO: URLs de subida generadas (con ContentType) para {plano_key}", flush=True)
        return {
            "plano": {"upload_url": url_plano, "key": plano_key},
            "correcciones": {"upload_url": url_correcciones, "key": correcciones_key}
        }
    except Exception as e:
        print(f"ERROR: No se pudo generar la URL. Error: {e}", flush=True)
        raise HTTPException(status_code=500, detail=f"Error al generar URLs de S3: {e}")


# --- ENDPOINT 2: Iniciar Procesamiento ---
@router.post("/iniciar_procesamiento_datacredito", tags=["Procesamiento"])
def iniciar_procesamiento_datacredito(
    data: dict, 
    background_tasks: BackgroundTasks
):
    """
    PASO 3: Inicia el procesamiento.
    ...
    """
    plano_key = data.get("plano_key")
    correcciones_key = data.get("correcciones_key")
    empresa = data.get("empresa")
    
    if not all([plano_key, correcciones_key, empresa]):
        raise HTTPException(status_code=400, detail="Se requieren plano_key, correcciones_key y empresa")

    # --- Nombres de archivos de salida 
    base_name_plano = os.path.basename(plano_key).split('-', 1)[-1]
    base_name_sin_ext = os.path.splitext(base_name_plano)[0]
    
    # 2. Creamos un identificador único para ESTE trabajo
    job_id_unico = uuid.uuid4().hex[:8] # Genera un ID corto como 'a1b2c3d4'
    
    # 3. Lo añadimos al nombre del archivo de salida
    output_filename = f"Resultado_{empresa}_{base_name_sin_ext}_{job_id_unico}.xlsx"



    # 1. Añade el trabajo pesado a la cola de fondo
    background_tasks.add_task(
        procesar_archivos_en_segundo_plano, 
        plano_key, 
        correcciones_key, 
        empresa, 
        output_filename # <--- Ya lleva el nombre único
    )
    
    # 2. Responde INMEDIATAMENTE
    print(f"INFO: Trabajo para {plano_key} encolado. Respondiendo 202.", flush=True)
    return {
        "status": "accepted",
        "message": "El procesamiento ha comenzado en segundo plano.",
        # ¡Devolvemos el NUEVO nombre único!
        "output_key": f"resultados/{output_filename}"
    }

@router.get("/estado_procesamiento", tags=["Procesamiento"])
def estado_procesamiento(key: str):
    """
    PASO 4: El cliente "pregunta" (hace 'polling') si el trabajo ya terminó.
    
    Recibe el 'output_key' que le dimos en el PASO 3.
    (Ej: 'resultados/Resultado_Empresa_miarchivo.xlsx')
    """
    
    if not key:
        raise HTTPException(status_code=400, detail="Se requiere el 'key' del archivo de resultado.")

    try:
        # 1. Intentamos "ver" (head_object) si el archivo ya existe en S3.
        # head_object es más rápido y barato que get_object o list_objects.
        s3_client.head_object(Bucket=BUCKET_NAME, Key=key)
        
        # 2. Si EXISTE (no hubo error), generamos una URL de DESCARGA
        print(f"INFO: El trabajo {key} está completado. Generando URL de descarga.", flush=True)
        
        download_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': BUCKET_NAME, 'Key': key},
            ExpiresIn=3600 # La URL de descarga dura 1 hora
        )
        
        return {
            "status": "completed",
            "download_url": download_url,
            "key": key
        }

    except s3_client.exceptions.ClientError as e:
        # 3. Si Boto3 da un error, lo revisamos
        error_code = e.response.get('Error', {}).get('Code')
        
        if error_code == '404' or error_code == 'NoSuchKey':
            # Código 404 (Not Found) significa que el archivo AÚN NO EXISTE.
            # Esto es normal, el trabajo sigue en proceso.
            print(f"INFO: El trabajo {key} aún está en proceso.", flush=True)
            return {
                "status": "processing",
                "message": "El archivo aún no está listo. Intente de nuevo en unos segundos."
            }
        else:
            # Otro error de S3 (ej. 403 Forbidden, permisos incorrectos)
            print(f"ERROR: Error de S3 al verificar {key}: {e}", flush=True)
            raise HTTPException(status_code=500, detail=f"Error de S3: {e}")
            
    except Exception as e:
        # Otro error inesperado
        print(f"ERROR: Error inesperado al verificar {key}: {e}", flush=True)
        raise HTTPException(status_code=500, detail=f"Error inesperado: {e}")    
    
    