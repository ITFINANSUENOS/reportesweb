# src/controllers/api/reportes_controller.py
import sys
import subprocess
import boto3
import uuid
import json
import os
from datetime import datetime
from src.core.config import settings
from src.services.base.dataprocessor_service import DataProcessorService

class ReportesController:
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            region_name=settings.AWS_REGION,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
        )

    def generar_url_subida(self, filename: str, content_type: str):
        file_key = f"uploads/{uuid.uuid4().hex}-{filename}"
        url = self.s3_client.generate_presigned_url(
            'put_object', 
            Params={'Bucket': settings.S3_BUCKET_NAME, 'Key': file_key, 'ContentType': content_type}, 
            ExpiresIn=3600
        )
        return {"upload_url": url, "file_key": file_key}

    def iniciar_procesamiento(self, file_key: str, empresa: str):
        job_id = uuid.uuid4().hex
        
        # En producción esto usaría SQS, en local lanza un subproceso
        if getattr(settings, "ENVIRONMENT", "production") == "local":
            subprocess.Popen([
                sys.executable, "worker.py", 
                "--file-key", file_key, 
                "--job-id", job_id, 
                "--empresa", empresa
            ])
            return {"status": "simulated", "message": "Worker lanzado localmente", "job_id": job_id}
        else:
            # Aquí iría la integración con AWS SQS
            return {"status": "queued", "job_id": job_id}

    # --- LÓGICA DEL WORKER ---
    async def procesar_reporte_batch(self, file_key: str, job_id: str, empresa: str):
        print(f"⚙️ WORKER: Iniciando procesamiento MULTI-MÓDULO para {empresa}...")
        
        local_input = f"temp_{job_id}.xlsx"
        
        try:
            # 1. Descargar Excel
            print(f"⬇️ Descargando {file_key}...")
            self.s3_client.download_file(settings.S3_BUCKET_NAME, file_key, local_input)
            
            # 2. Procesar Datos
            processor = DataProcessorService()
            todos_los_datos = processor.procesar_excel_multi_modulo(local_input, job_id=job_id)
            
            archivos_generados = 0
            
            # 3. Iterar y Distribuir Resultados
            for modulo, data in todos_los_datos.items():
                
                # CASO A: ARCHIVOS INTERNOS (PARQUET, ETC.) 📦
                # Si la llave empieza con "_", es una ruta de archivo local.
                if modulo.startswith("_"):
                    local_path = data # Ej: "data/cartera/abcd.parquet"
                    
                    if os.path.exists(local_path):
                        # Usamos la misma estructura de carpetas para S3
                        # Reemplazamos \ por / para compatibilidad Windows/Linux en S3
                        s3_key_file = local_path.replace("\\", "/")
                        
                        print(f"📦 Subiendo archivo optimizado ({modulo}) a: {s3_key_file}...")
                        
                        try:
                            self.s3_client.upload_file(
                                local_path, 
                                settings.S3_BUCKET_NAME, 
                                s3_key_file
                            )
                            # Borramos el archivo local después de subirlo
                            os.remove(local_path)
                        except Exception as e:
                            print(f"❌ Error subiendo archivo {modulo}: {e}")
                    else:
                        print(f"⚠️ El archivo reportado en {modulo} no se encontró en disco: {local_path}")
                    # CRÍTICO: 'continue' evita que este archivo se intente procesar como JSON abajo
                    continue 
                
                # CASO B: MÓDULOS DE DATOS (PARA GRÁFICOS JSON) 📊
                if not data or "error" in data:
                    if isinstance(data, dict) and "error" in data:
                        print(f"⚠️ MÓDULO {modulo} FALLÓ CON ERROR: {data['error']}")
                    else:
                        print(f"⚠️ MÓDULO {modulo} ESTÁ VACÍO.")
                    continue

                local_json_name = f"temp_{job_id}_{modulo}.json"
                
                json_final = {
                    "metadata": {
                        "job_id": job_id,
                        "empresa": empresa,
                        "modulo": modulo,
                        "fecha_generacion": datetime.now().isoformat()
                    },
                    "data": data
                }

                # Usamos el wrapper del servicio (o podrías importar guardar_json de utils)
                processor.guardar_json_resultado(json_final, local_json_name)
                
                # Subir a carpeta 'graficos/modulo/'
                s3_key = f"graficos/{modulo}/{job_id}.json"
                print(f"⬆️ Subiendo reporte {modulo} a: {s3_key}...")
                
                self.s3_client.upload_file(
                    local_json_name, 
                    settings.S3_BUCKET_NAME, 
                    s3_key,
                    ExtraArgs={'ContentType': 'application/json'}
                )
                
                if os.path.exists(local_json_name): os.remove(local_json_name)
                archivos_generados += 1

            # 4. FINALIZACIÓN Y ACTUALIZACIÓN DE PUNTERO
            if archivos_generados > 0:
                print(f"✅ EXITO: Job {job_id} completado. Reportes generados.")
                
                # Actualizar "Reporte Activo"
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
                    
                except Exception as e:
                    print(f"⚠️ Advertencia: No se pudo actualizar el reporte activo: {e}")

                return True
            
            return False

        except Exception as e:
            print(f"❌ ERROR FATAL EN WORKER: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            if os.path.exists(local_input): os.remove(local_input)