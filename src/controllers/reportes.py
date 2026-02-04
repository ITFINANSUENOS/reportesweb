# src/controllers/reportes.py
from fastapi import HTTPException, BackgroundTasks
import boto3
import uuid
import json
import os
from datetime import datetime
from src.core.config import settings

# IMPORTS DE SERVICIOS
from src.services.orchestrator import ReportesOrchestrator
from src.services.storage.s3_service import S3Service

class ReportesController:
    # Constantes de validación
    MAX_FILE_SIZE_MB = 25
    MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
    ALLOWED_EXTENSIONS = {".xlsx"}
    REQUIRED_NAME_PATTERN = "Reporte_General"

    def __init__(self):
        # 1. Usamos el servicio de S3 centralizado para operaciones simples
        self.s3_service = S3Service()
        
        # OJO: Para generar URLs firmadas, seguimos necesitando el cliente crudo de boto3
        # porque el método generate_presigned_url es muy específico.
        # Podríamos moverlo a S3Service, pero por ahora está bien dejarlo aquí o refactorizarlo luego.
        self.s3_client_raw = self.s3_service.s3 
        
        # 2. Cliente SQS (Si existe)
        self.sqs = None
        if settings.SQS_QUEUE_URL:
            try:
                self.sqs = boto3.client(
                    'sqs',
                    region_name=settings.AWS_REGION,
                    aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
                )
            except Exception as e:
                print(f"⚠️ Error inicializando SQS: {e}")
                self.sqs = None

    def generar_url_subida(self, filename: str, content_type: str, file_size: int):
        """Valida reglas de negocio y genera URL firmada."""
        
        # 1. Validaciones
        if self.REQUIRED_NAME_PATTERN not in filename:
            raise HTTPException(status_code=400, detail=f"Nombre inválido. Debe contener '{self.REQUIRED_NAME_PATTERN}'.")

        _, ext = os.path.splitext(filename)
        if ext.lower() not in self.ALLOWED_EXTENSIONS:
            raise HTTPException(status_code=400, detail=f"Formato no permitido ({ext}).")

        if file_size > self.MAX_FILE_SIZE_BYTES:
            raise HTTPException(status_code=400, detail=f"El archivo excede {self.MAX_FILE_SIZE_MB}MB.")

        # 2. Generar Key única
        file_key = f"uploads/{uuid.uuid4().hex}-{filename}"
        
        try:
            url = self.s3_client_raw.generate_presigned_url(
                'put_object', 
                Params={
                    'Bucket': settings.S3_BUCKET_NAME, 
                    'Key': file_key, 
                    'ContentType': content_type
                }, 
                ExpiresIn=3600
            )
            return {"upload_url": url, "file_key": file_key}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error generando URL: {str(e)}")

    async def iniciar_procesamiento_async(self, file_key, empresa, tipo_reporte, background_tasks: BackgroundTasks):
        """Orquesta el inicio del trabajo (SQS vs Local)."""
        job_id = uuid.uuid4().hex
        
        # A. MODO PRODUCCIÓN (SQS)
        if self.sqs and settings.SQS_QUEUE_URL:
            try:
                message_body = {
                    "job_id": job_id, "file_key": file_key,
                    "empresa": empresa, "tipo_reporte": tipo_reporte
                }
                self.sqs.send_message(
                    QueueUrl=settings.SQS_QUEUE_URL,
                    MessageBody=json.dumps(message_body)
                )
                return {"status": "queued", "job_id": job_id, "message": "Encolado en SQS (Producción)."}
            except Exception as e:
                return {"status": "error", "message": f"Fallo SQS: {str(e)}"}

        # B. MODO LOCAL
        else:
            print(f"⚠️ SQS inactivo. Ejecutando Job {job_id} localmente.")
            background_tasks.add_task(self.procesar_reporte_batch, file_key, job_id, empresa)
            return {"status": "processing_local", "job_id": job_id, "message": "Ejecutando localmente."}

    def obtener_json_graficos(self, job_id, modulo):
        """Descarga JSON de gráficos usando el servicio de S3."""
        # Podemos implementar un método 'leer_json' en S3Service si queremos abstraer más,
        # pero por ahora usaremos el cliente raw para mantener compatibilidad rápida.
        s3_key = f"graficos/{modulo}/{job_id}.json"
        try:
            print(f"📥 Descargando gráfico: {s3_key}")
            response = self.s3_client_raw.get_object(Bucket=settings.S3_BUCKET_NAME, Key=s3_key)
            return json.loads(response['Body'].read().decode('utf-8'))
        except self.s3_client_raw.exceptions.NoSuchKey:
            raise HTTPException(status_code=404, detail=f"No hay datos para '{modulo}'.")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error S3: {str(e)}")

    # --- WORKER LOCAL (Lógica de fondo) ---
    async def procesar_reporte_batch(self, file_key: str, job_id: str, empresa: str):
        print(f"⚙️ WORKER: Procesando {empresa}...")
        
        # Aseguramos carpeta temporal
        os.makedirs("temp", exist_ok=True)
        local_input = os.path.join("temp", f"temp_{job_id}.xlsx")
        
        try:
            # 1. Descargar usando el servicio S3
            self.s3_service.descargar_archivo(file_key, local_input)
            
            # 2. Procesar (Orquestador)
            processor = ReportesOrchestrator()
            resultados = processor.procesar_excel_multi_modulo(local_input, job_id=job_id, empresa=empresa)
            
            # 3. Actualizar puntero "Reporte Activo"
            if resultados:
                print(f"✅ Job {job_id} completado.")
                try:
                    manifest_data = {
                        "active_job_id": job_id,
                        "fecha_actualizacion": datetime.now().isoformat(),
                        "empresa": empresa, "status": "ready"
                    }
                    # Usamos el servicio para guardar el JSON de config
                    self.s3_service.guardar_json(manifest_data, "config/reporte_activo.json")
                    print(f"🔄 Reporte Activo actualizado.")
                    return True
                except Exception as e:
                    print(f"⚠️ Error actualizando manifiesto: {e}")
                    return True
            return False

        except Exception as e:
            print(f"❌ ERROR WORKER: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            if os.path.exists(local_input): os.remove(local_input)