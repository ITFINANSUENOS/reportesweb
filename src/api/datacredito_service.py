# Importa tu modelo (la lógica de pandas)
from src.models.datacredito_model import DataCreditoModel

class DataCreditoApiService:
    """
    Este servicio está diseñado EXCLUSIVAMENTE para la API de FastAPI.
    No tiene dependencias de GUI y se encarga de orquestar
    el procesamiento de archivos para las solicitudes web.
    """
    def __init__(self):
        # Cada instancia del servicio tendrá su propia instancia del modelo.
        self.model = DataCreditoModel()

    def process_files_for_api(self, plano_path: str, correcciones_path: str, output_path: str, empresa: str):
        """
        Orquesta el procesamiento de archivos para una solicitud de la API.
        Este método es el equivalente al '_run_processing_thread' del controlador
        de Tkinter, pero sin ninguna dependencia gráfica.
        """
        if not empresa:
            raise ValueError("El parámetro 'empresa' es obligatorio para el procesamiento.")

        try:
            # 1. Cargar datos
            print("SERVICE_API: Cargando plano...")
            self.model.load_plano_file(plano_path)
            
            # 2. Procesar datos
            print("SERVICE_API: Procesando datos...")
            self.model.process_data(correcciones_path, empresa.lower())
            
            # 3. Guardar datos
            print("SERVICE_API: Guardando archivo procesado...")
            self.model.save_processed_file(output_path)
            print("SERVICE_API: Guardado completado.")
            
        except Exception as e:
            # Si algo sale mal en el modelo, se relanza la excepción
            # para que la ruta de FastAPI la capture y devuelva un error HTTP 500.
            print(f"ERROR en DataCreditoApiService: {e}")
            raise e