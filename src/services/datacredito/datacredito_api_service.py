from src.models.datacredito_model import DataCreditoModel

class DataCreditoApiService:
    """
    Este servicio está diseñado EXCLUSIVAMENTE para la API de FastAPI.
    Orquesta el procesamiento de archivos para las solicitudes web.
    """
    def __init__(self):
        # Cada instancia del servicio tendrá su propia instancia del modelo.
        self.model = DataCreditoModel()

    def process_files_for_api(self, plano_path: str, correcciones_path: str, output_path: str, empresa: str):
        """
        Orquesta el procesamiento de archivos para una solicitud de la API.
        Llama al NUEVO método optimizado (process_files_in_chunks)
        """
        if not empresa:
            raise ValueError("El parámetro 'empresa' es obligatorio para el procesamiento.")

        try:
            # Esta es la única llamada que hacemos.
            # Este método ahora hace todo (cargar, procesar, guardar)
            # de manera eficiente y optimizada para 2GB de RAM.
            print("SERVICE_API: Iniciando procesamiento optimizado (chunks)...", flush=True)
            
            self.model.process_files_in_chunks(
                plano_path, 
                correcciones_path, 
                empresa.lower(), 
                output_path
            )
            
            print("SERVICE_API: Procesamiento optimizado completado.", flush=True)
            
        except Exception as e:
            # Si algo sale mal en el modelo, se relanza la excepción
            print(f"ERROR en DataCreditoApiService (chunks): {e}", flush=True)
            raise e