import sys
import argparse
import asyncio
from src.controllers.api.reportes_controller import ReportesController

# Configura logs para verlos en AWS CloudWatch
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AWS-Worker")

async def main():
    # 1. Recibir argumentos desde AWS Batch
    parser = argparse.ArgumentParser(description="Worker de Procesamiento Pesado")
    parser.add_argument("--file-key", required=True, help="Ruta del archivo en S3")
    parser.add_argument("--job-id", required=True, help="ID único del trabajo")
    parser.add_argument("--empresa", required=True, help="Nombre de la empresa")
    
    args = parser.parse_args()

    logger.info(f"🚀 INICIANDO JOB {args.job_id} para {args.empresa}")
    logger.info(f"📂 Archivo a procesar: {args.file_key}")

    try:
        # 2. Instanciar el controlador (Reutilizamos tu lógica existente)
        controller = ReportesController()
        
        # 3. Ejecutar el proceso (Síncrono o Asíncrono)
        # Nota: En el worker, como es un solo proceso dedicado, 
        # no nos preocupa tanto bloquear el event loop, pero usamos async por consistencia.
        await controller.procesar_reporte_batch(
            file_key=args.file_key,
            job_id=args.job_id,
            empresa=args.empresa
        )
        
        logger.info("✅ PROCESO FINALIZADO CON ÉXITO")
        sys.exit(0) # Salida limpia (AWS marca el Job como SUCCEEDED)

    except Exception as e:
        logger.error(f"❌ ERROR FATAL: {str(e)}")
        sys.exit(1) # Salida con error (AWS marca el Job como FAILED y puede reintentar)

if __name__ == "__main__":
    asyncio.run(main())
    
    
