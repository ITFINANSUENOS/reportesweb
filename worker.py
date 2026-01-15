import boto3
import json
import time
import logging
from src.core.config import settings
from src.services.base.dataprocessor_service import DataProcessorService

# Configurar Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Worker")

def process_message(message_body):
    job_id = message_body.get("job_id")
    file_key = message_body.get("file_key")
    empresa = message_body.get("empresa")
    tipo_reporte = message_body.get("tipo_reporte")

    logger.info(f"👷 Worker iniciando Job: {job_id} [{tipo_reporte}]")

    service = DataProcessorService()
    
    try:
        # Llamamos al servicio que sabe qué hacer según el tipo de reporte
        service.ejecutar_pipeline(job_id, file_key, empresa, tipo_reporte)
        logger.info(f"✅ Job {job_id} completado.")
        return True
    except Exception as e:
        logger.error(f"❌ Job {job_id} falló: {e}")
        return False

def main():
    sqs = boto3.client('sqs', region_name=settings.AWS_REGION)
    queue_url = settings.SQS_QUEUE_URL
    
    logger.info(f"🚀 Worker escuchando en {queue_url}")

    while True:
        try:
            # Long Polling
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20
            )

            if 'Messages' in response:
                for msg in response['Messages']:
                    body = json.loads(msg['Body'])
                    receipt_handle = msg['ReceiptHandle']

                    if process_message(body):
                        # Borrar mensaje solo si tuvo éxito
                        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
            else:
                pass # Sigue esperando (en Fargate Spot esto escala a 0 si no hay mensajes)

        except Exception as e:
            logger.error(f"Error en loop: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()