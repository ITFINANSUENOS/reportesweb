from src.models.anticipos_model import AnticiposConfig
from src.services.anticipos.dataloader_service import AnticiposDataLoader
from src.services.anticipos.dataprocessor_service import AnticiposDataProcessor
from src.services.anticipos.report_service import AnticiposReportWriter

class AnticiposService:
    """Orquesta el proceso de generación de reportes de anticipos."""

    def __init__(self, config: AnticiposConfig = None):
        self.config = config if config else AnticiposConfig()
        self.loader = AnticiposDataLoader(self.config)
        self.processor = AnticiposDataProcessor(self.config)
        self.writer = AnticiposReportWriter()

    def generate_report_data(self, file_path: str, status_callback) -> dict:
        """Genera los datos del reporte, pero no los guarda."""
        status_callback("Validando y cargando datos...", 10)
        dfs = self.loader.load_and_filter_data(file_path)

        status_callback("Aplicando lógica de negocio...", 40)
        final_df = self.processor.process_data(dfs)

        status_callback("Preparando hojas finales...", 80)
        sheets_to_save = self.processor.prepare_output_sheets(final_df)
        
        return sheets_to_save

    def save_report(self, output_path: str, sheets_data: dict, status_callback):
        """Guarda el reporte ya procesado y le aplica formato."""
        status_callback("Guardando reporte con formato...", 90)
        self.writer.save_report(output_path, sheets_data)
        status_callback("Reporte completado.", 100)