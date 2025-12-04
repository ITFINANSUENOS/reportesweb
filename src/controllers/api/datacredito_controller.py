import boto3
import uuid
import os
import shutil
from fastapi import HTTPException, BackgroundTasks
from dotenv import load_dotenv

# Importamos el servicio con la ruta completa
from src.services.datacredito.datacredito_api_service import DataCreditoApiService

load_dotenv()

class DataCreditoController:
    def __init__(self):
        # Configuración de AWS
        self.bucket_name = os.getenv('S3_BUCKET_NAME')
        self.region = os.getenv('AWS_REGION', 'us-east-1')
        self.s3_client = boto3.client('s3', region_name=self.region)
        
        # Instanciamos el servicio de lógica de negocio
        self.service_api = DataCreditoApiService()

    # --- MÉTODOS PRIVADOS / UTILITARIOS ---

    def _limpiar_reportes_antiguos_s3(self, prefijo: str, max_archivos: int = 3):
        """Mantiene solo los archivos más recientes en S3."""
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefijo)
            if 'Contents' not in response: return

            archivos = [obj for obj in response['Contents'] if obj['Size'] > 0]
            archivos_ordenados = sorted(archivos, key=lambda x: x['LastModified'])
            
            a_borrar_count = len(archivos_ordenados) - max_archivos
            if a_borrar_count <= 0: return

            archivos_a_borrar = archivos_ordenados[:a_borrar_count]
            objetos = [{'Key': obj['Key']} for obj in archivos_a_borrar]
            
            print(f"S3_CLEANUP: Borrando {len(objetos)} archivos antiguos...", flush=True)
            self.s3_client.delete_objects(Bucket=self.bucket_name, Delete={'Objects': objetos})
            
        except Exception as e:
            print(f"S3_CLEANUP_ERROR: {e}", flush=True)

    def _tarea_segundo_plano(self, plano_key: str, correcciones_key: str, empresa: str, output_key: str):
        """Lógica que corre en BackgroundTasks"""
        temp_dir = f"/tmp/{uuid.uuid4().hex}"
        os.makedirs(temp_dir, exist_ok=True)
        
        plano_path = os.path.join(temp_dir, os.path.basename(plano_key))
        correcciones_path = os.path.join(temp_dir, os.path.basename(correcciones_key))
        output_path = os.path.join(temp_dir, output_key)

        try:
            print(f"BG_TASK: Descargando archivos...", flush=True)
            self.s3_client.download_file(self.bucket_name, plano_key, plano_path)
            self.s3_client.download_file(self.bucket_name, correcciones_key, correcciones_path)

            print(f"BG_TASK: Procesando Datacredito para {empresa}...", flush=True)
            self.service_api.process_files_for_api(
                plano_path=plano_path,
                correcciones_path=correcciones_path,
                output_path=output_path,
                empresa=empresa
            )

            print(f"BG_TASK: Subiendo resultado...", flush=True)
            self.s3_client.upload_file(output_path, self.bucket_name, f"resultados/{output_key}")
            
            self._limpiar_reportes_antiguos_s3(prefijo="resultados/", max_archivos=3)

        except Exception as e:
            print(f"BG_TASK_ERROR: {e}", flush=True)
        finally:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

    # --- MÉTODOS PÚBLICOS (LLAMADOS POR LA RUTA) ---

    def generar_urls_subida(self, data: dict):
        plano_name = data.get("plano_filename")
        corr_name = data.get("correcciones_filename")
        plano_type = data.get("plano_content_type")
        corr_type = data.get("correcciones_content_type")

        if not all([plano_name, corr_name, plano_type, corr_type]):
            raise HTTPException(status_code=400, detail="Faltan datos (filenames o content_types)")

        plano_key = f"uploads/{uuid.uuid4().hex}-{plano_name}"
        corr_key = f"uploads/{uuid.uuid4().hex}-{corr_name}"

        try:
            url_plano = self.s3_client.generate_presigned_url(
                'put_object',
                Params={'Bucket': self.bucket_name, 'Key': plano_key, 'ContentType': plano_type},
                ExpiresIn=3600
            )
            url_corr = self.s3_client.generate_presigned_url(
                'put_object',
                Params={'Bucket': self.bucket_name, 'Key': corr_key, 'ContentType': corr_type},
                ExpiresIn=3600
            )
            return {
                "plano": {"upload_url": url_plano, "key": plano_key},
                "correcciones": {"upload_url": url_corr, "key": corr_key}
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    def iniciar_procesamiento(self, data: dict, background_tasks: BackgroundTasks):
        plano_key = data.get("plano_key")
        corr_key = data.get("correcciones_key")
        empresa = data.get("empresa")

        if not all([plano_key, corr_key, empresa]):
            raise HTTPException(status_code=400, detail="Faltan datos (keys o empresa)")

        # Generar nombre único de salida
        base_name = os.path.basename(plano_key).split('-', 1)[-1]
        base_limpio = os.path.splitext(base_name)[0]
        job_id = uuid.uuid4().hex[:8]
        output_filename = f"Resultado_{empresa}_{base_limpio}_{job_id}.xlsx"

        # Agendar tarea en segundo plano
        background_tasks.add_task(
            self._tarea_segundo_plano,
            plano_key, corr_key, empresa, output_filename
        )

        return {
            "status": "accepted",
            "message": "Procesamiento iniciado en segundo plano",
            "output_key": f"resultados/{output_filename}"
        }

    def consultar_estado(self, key: str):
        if not key: raise HTTPException(status_code=400, detail="Falta el key")

        try:
            # Verificamos si existe (HEAD)
            self.s3_client.head_object(Bucket=self.bucket_name, Key=key)
            
            # Si existe, generamos URL de descarga
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket_name, 'Key': key},
                ExpiresIn=3600
            )
            return {"status": "completed", "download_url": url, "key": key}

        except self.s3_client.exceptions.ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            if error_code == '404' or error_code == 'NoSuchKey':
                return {"status": "processing", "message": "Aún procesando..."}
            else:
                raise HTTPException(status_code=500, detail=str(e))