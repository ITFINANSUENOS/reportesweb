import boto3
import json
import os
from datetime import datetime
from src.core.config import settings
from src.utils.polars_utils import guardar_parquet

class S3Service:
    def __init__(self):
        self.s3 = boto3.client(
            's3',
            region_name=settings.AWS_REGION,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
        )
        self.bucket = settings.S3_BUCKET_NAME

    def guardar_json(self, data: dict, key_s3: str, metadata: dict = None) -> bool:
        """Sube un diccionario como JSON a S3."""
        try:
            payload = {"data": data}
            if metadata:
                payload["metadata"] = {
                    **metadata, 
                    "fecha_generacion": datetime.now().isoformat()
                }
            
            json_str = json.dumps(payload, ensure_ascii=False)
            
            self.s3.put_object(
                Bucket=self.bucket,
                Key=key_s3,
                Body=json_str,
                ContentType='application/json'
            )
            print(f"✅ JSON guardado en S3: {key_s3}")
            return True
        except Exception as e:
            print(f"❌ Error guardando JSON {key_s3}: {e}")
            return False

    def guardar_parquet(self, df, key_s3: str, columnas_validas: list = None) -> bool:
        """Guarda un DataFrame como Parquet local y lo sube a S3."""
        nombre_local = None
        try:
            # Creamos un nombre local único y seguro
            nombre_local = key_s3.replace("/", "_")
            
            # Filtramos columnas si se especifican
            df_final = df.select(columnas_validas) if columnas_validas else df
            
            # Usamos la utilidad existente para guardar en disco
            guardar_parquet(df_final, nombre_local)
            
            print(f"☁️ Subiendo Parquet a S3: {key_s3}...")
            self.s3.upload_file(nombre_local, self.bucket, key_s3)
            
            return True
        except Exception as e:
            print(f"❌ Error subiendo Parquet {key_s3}: {e}")
            return False
        finally:
            # Limpieza del archivo temporal local
            if nombre_local and os.path.exists(nombre_local):
                os.remove(nombre_local)
    
    def descargar_archivo(self, key_s3: str, path_local: str):
        """Descarga un archivo desde S3 al disco local."""
        print(f"⬇️ Descargando {key_s3}...")
        
        # Crear directorio si no existe
        directory = os.path.dirname(path_local)
        if directory:
            os.makedirs(directory, exist_ok=True)
        
        # Verificar si ya existe y tiene contenido válido
        if os.path.exists(path_local) and os.path.getsize(path_local) > 0:
            print(f"✅ Archivo ya existe localmente: {path_local}")
            return
        
        self.s3.download_file(self.bucket, key_s3, path_local)

    def verificar_existe(self, key_s3: str) -> bool:
        """Verifica si un archivo existe en S3."""
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key_s3)
            return True
        except:
            return False