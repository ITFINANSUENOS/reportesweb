import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Variables generales
    APP_NAME: str = "API Reportes Financieros"
    API_V1_STR: str = "/api/v1"
    
    # --- LA LÍNEA QUE FALTABA ---
    # Por defecto será 'production', pero si el .env dice 'local', tomará ese valor.    
    ENVIRONMENT: str = "production" 
    
    # AWS
    AWS_REGION: str = "us-east-1"
    S3_BUCKET_NAME: str
    AWS_ACCESS_KEY_ID: str | None = None
    AWS_SECRET_ACCESS_KEY: str | None = None

    class Config:
        env_file = ".env"
        case_sensitive = True
        # Esto permite que si hay variables extra en el .env (basura), no rompa la app
        extra = "ignore" 

settings = Settings()