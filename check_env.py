import os
import sys

def check_env():
    print("=== Verificación de Variables de Entorno ===")
    
    # 1. Verificar S3_BUCKET_NAME
    bucket = os.getenv("S3_BUCKET_NAME")
    if not bucket:
        print("❌ [CRÍTICO] S3_BUCKET_NAME no está configurada.")
        print("   Esta variable es obligatoria en src/core/config.py.")
        print("   Sin ella, la aplicación fallará al iniciar.")
    else:
        print(f"✅ S3_BUCKET_NAME = {bucket}")

    # 2. Verificar otras vars
    region = os.getenv("AWS_REGION", "us-east-1 (default)")
    print(f"ℹ️  AWS_REGION = {region}")
    
    sqs = os.getenv("SQS_QUEUE_URL")
    if sqs:
        print(f"✅ SQS_QUEUE_URL configurada.")
    else:
        print("⚠️ SQS_QUEUE_URL no configurada (El Worker correrá en modo local/hilos).")

    print("\n=== Prueba de Importación de Configuración ===")
    try:
        from src.core.config import settings
        print("✅ Importación de 'src.core.config.settings' exitosa.")
        print("   La aplicación debería iniciar correctamente.")
    except Exception as e:
        print(f"❌ Error al importar configuración: {e}")
        print("   Esto es lo que está causando que App Runner falle.")

if __name__ == "__main__":
    # Asegurar que el directorio actual está en el path para imports
    sys.path.append(os.getcwd())
    check_env()
