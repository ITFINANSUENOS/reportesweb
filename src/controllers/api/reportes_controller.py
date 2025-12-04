from fastapi import UploadFile, HTTPException
from src.services.nube.cloud_storage_service import CloudStorageService

class ReportesController:
    def __init__(self):
        # Instanciamos el servicio que acabas de terminar
        self.cloud_service = CloudStorageService()

    async def cargar_reporte_general(self, file: UploadFile):
        """
        Controlador que orquesta la subida del Reporte General.
        """
        # 1. Validación básica de entrada
        if not file.filename.endswith(('.xlsx', '.xls')):
            raise HTTPException(
                status_code=400, 
                detail="Formato inválido. Solo se permiten archivos Excel (.xlsx)"
            )

        try:
            # 2. Llamamos al servicio (Aquí ocurre la magia de Polars y S3)
            # Nota: file.file es el objeto binario que necesita tu servicio
            resultado = self.cloud_service.procesar_y_subir_reporte(file.file)
            
            # 3. Respuesta al Cliente
            # Si hubo errores parciales (ej. falló una hoja pero subieron las otras), avisamos.
            if resultado.get("errores"):
                return {
                    "status": "warning",
                    "message": "Archivo procesado parcialmente (algunas hojas fallaron)",
                    "data": resultado
                }

            return {
                "status": "success",
                "message": "Reporte procesado y optimizado en la nube correctamente",
                "data": resultado
            }

        except ValueError as ve:
            # Errores de validación de negocio (ej. Hoja no encontrada)
            raise HTTPException(status_code=422, detail=str(ve))
        
        except Exception as e:
            # Errores inesperados (AWS caído, bug en código)
            print(f"❌ Error Crítico en Controller: {e}")
            raise HTTPException(
                status_code=500, 
                detail="Error interno del servidor al procesar el archivo."
            )