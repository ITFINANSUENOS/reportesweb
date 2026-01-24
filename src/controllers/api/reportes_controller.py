# src/controllers/api/reportes_controller.py
from fastapi import  HTTPException,BackgroundTasks
import boto3
import uuid
import json
import os
from datetime import datetime
from src.core.config import settings
from src.services.base.dataprocessor_service import DataProcessorService

class ReportesController:
    def __init__(self):
        # 1. Cliente S3 (Siempre necesario)
        self.s3_client = boto3.client(
            's3',
            region_name=settings.AWS_REGION,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
        )
        
        # 2. Cliente SQS (Solo si hay configuración)
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

    def generar_url_subida(self, filename: str, content_type: str):
        file_key = f"uploads/{uuid.uuid4().hex}-{filename}"
        try:
            url = self.s3_client.generate_presigned_url(
                'put_object', 
                Params={'Bucket': settings.S3_BUCKET_NAME, 'Key': file_key, 'ContentType': content_type}, 
                ExpiresIn=3600
            )
            return {"upload_url": url, "file_key": file_key}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error generando URL firmada: {str(e)}")

    async def iniciar_procesamiento_async(self, file_key, empresa, tipo_reporte, background_tasks: BackgroundTasks):
        """
        Orquesta el inicio del trabajo.
        - Si hay SQS configurado -> Envía a la cola (Producción).
        - Si NO hay SQS -> Ejecuta en background thread (Local).
        """
        job_id = uuid.uuid4().hex
        
        # Opción A: MODO PRODUCCIÓN (SQS)
        if self.sqs and settings.SQS_QUEUE_URL:
            try:
                message_body = {
                    "job_id": job_id,
                    "file_key": file_key,
                    "empresa": empresa,
                    "tipo_reporte": tipo_reporte
                }
                self.sqs.send_message(
                    QueueUrl=settings.SQS_QUEUE_URL,
                    MessageBody=json.dumps(message_body)
                )
                return {
                    "status": "queued",
                    "job_id": job_id,
                    "message": "Archivo encolado exitosamente (Modo Producción)."
                }
            except Exception as e:
                return {"status": "error", "message": f"Fallo al encolar en SQS: {str(e)}"}

        # Opción B: MODO LOCAL (Simulación)
        else:
            print(f"⚠️ SQS no configurado. Ejecutando Job {job_id} en hilo local.")
            
            # Inyectamos la tarea en el loop de eventos de FastAPI
            background_tasks.add_task(
                self.procesar_reporte_batch, 
                file_key, 
                job_id, 
                empresa
            )
            
            return {
                "status": "processing_local",
                "job_id": job_id,
                "message": "Ejecutando en segundo plano (Modo Local/Sin SQS)."
            }

    def obtener_json_graficos(self, job_id, modulo):
        """Descarga JSON de gráficos desde S3."""
        s3_key = f"graficos/{modulo}/{job_id}.json"
        
        try:
            print(f"📥 Descargando gráfico: {s3_key}")
            response = self.s3_client.get_object(Bucket=settings.S3_BUCKET_NAME, Key=s3_key)
            content = response['Body'].read().decode('utf-8')
            return json.loads(content)
            
        except self.s3_client.exceptions.NoSuchKey:
            raise HTTPException(status_code=404, detail=f"No se encontraron datos para el módulo '{modulo}'.")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error leyendo S3: {str(e)}")

    # --- LÓGICA DEL WORKER ---
    async def procesar_reporte_batch(self, file_key: str, job_id: str, empresa: str):
        print(f"⚙️ WORKER: Iniciando procesamiento para {empresa}...")
        
        local_input = f"temp_{job_id}.xlsx"
        
        try:
            # 1. Descargar Excel
            print(f"⬇️ Descargando {file_key}...")
            self.s3_client.download_file(settings.S3_BUCKET_NAME, file_key, local_input)
            
            # 2. Procesar Datos (El Servicio guarda todo en S3 internamente)
            processor = DataProcessorService()
            
            # Esta función ya guarda los Parquets y los JSONs en S3
            resultados = processor.procesar_excel_multi_modulo(local_input, job_id=job_id, empresa=empresa)
            
            # 3. Finalización: Actualizar Puntero "Reporte Activo"
            if resultados:
                print(f"✅ EXITO: Job {job_id} completado. Archivos guardados por el servicio.")
                
                try:
                    manifest_data = {
                        "active_job_id": job_id,
                        "fecha_actualizacion": datetime.now().isoformat(),
                        "empresa": empresa,
                        "status": "ready"
                    }
                    
                    self.s3_client.put_object(
                        Bucket=settings.S3_BUCKET_NAME,
                        Key="config/reporte_activo.json",
                        Body=json.dumps(manifest_data),
                        ContentType="application/json"
                    )
                    print(f"🔄 Reporte Activo actualizado a: {job_id}")
                    return True
                    
                except Exception as e:
                    print(f"⚠️ Advertencia actualizando reporte activo: {e}")
                    return True
            
            return False

        except Exception as e:
            print(f"❌ ERROR FATAL EN WORKER: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            # Limpieza del Excel de entrada
            if os.path.exists(local_input): os.remove(local_input)