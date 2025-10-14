from typing import List, Dict, Any, Callable
import pandas as pd

# Importaciones de tu proyecto
from src.services.base.report_service import ReportService
from src.services.base.update_base_service import UpdateBaseService
from src.services.base.file_handler_service import FileHandlerService
from src.models.base_model import configuracion, ORDEN_COLUMNAS_FINAL

class ProcessingOrchestratorService:
    """
    Orquesta el flujo de procesamiento de reportes, decidiendo si se
    ejecuta una construcción completa o una actualización rápida.
    """
    def __init__(self, progress_callback: Callable[[str, int], None] = None):
        """
        Inicializa el orquestador.
        
        Args:
            progress_callback: Una función para reportar el progreso a la UI.
                               Debe aceptar un mensaje (str) y un porcentaje (int).
        """
        self.report_service = ReportService(config=configuracion)
        self.update_service = UpdateBaseService(report_service=self.report_service)
        self.file_handler = FileHandlerService()
        self.progress_callback = progress_callback or (lambda msg, pct: None)

    def execute_processing(self, file_paths: List[str], update_mode: bool, base_report_path: str = None, 
                           start_date: str = None, end_date: str = None) -> Dict[str, pd.DataFrame]:
        """
        Ejecuta el proceso principal de generación o actualización del reporte.

        Args:
            file_paths: Lista de rutas de los archivos de entrada.
            update_mode: Booleano que indica si es modo actualización.
            base_report_path: Ruta al reporte Excel anterior (para modo actualización).
            start_date: Fecha de inicio para filtrar (opcional).
            end_date: Fecha de fin para filtrar (opcional).

        Returns:
            Un diccionario con los dataframes resultantes: 'reporte_final', 
            'reporte_negativos', 'reporte_correcciones'.
        """
        if not file_paths:
            raise ValueError("No se ha seleccionado ningún archivo para procesar.")

        if update_mode:
            return self._run_update_mode(file_paths, base_report_path)
        else:
            return self._run_full_build_mode(file_paths, start_date, end_date)

    def _run_update_mode(self, new_files_paths: List[str], base_report_path: str) -> Dict[str, pd.DataFrame]:
        """Lógica para el modo de actualización rápida."""
        self.progress_callback("Iniciando sincronización rápida...", 10)
        
        if not base_report_path:
            raise ValueError("Para el modo actualización, debe seleccionar el reporte de Excel anterior.")
        
        self.progress_callback("Cargando base anterior desde Excel...", 20)
        df_base_anterior = self.file_handler.read_excel_base(base_report_path)
        
        self.progress_callback("Estandarizando archivos nuevos...", 30)
        dataframes_nuevos = self.report_service.data_loader.load_dataframes(new_files_paths)
        
        self.progress_callback("Sincronizando cambios...", 60)
        reporte_final, reporte_negativos, reporte_correcciones = self.update_service.sincronizar_reporte(
            df_base_anterior,
            dataframes_nuevos
        )
        return self._package_results(reporte_final, reporte_negativos, reporte_correcciones)

    def _run_full_build_mode(self, file_paths: List[str], start_date: str, end_date: str) -> Dict[str, pd.DataFrame]:
        """Lógica para la construcción completa del reporte."""
        self.progress_callback("Iniciando construcción completa...", 10)
        
        reporte_final, reporte_negativos, reporte_correcciones = self.report_service.generate_consolidated_report(
            file_paths=file_paths,
            orden_columnas=ORDEN_COLUMNAS_FINAL,
            start_date=start_date,
            end_date=end_date
        )
        return self._package_results(reporte_final, reporte_negativos, reporte_correcciones)

    def _package_results(self, final, negatives, corrections) -> Dict[str, pd.DataFrame]:
        """Empaqueta los dataframes resultantes en un diccionario estandarizado."""
        if final is None or final.empty:
            raise Exception("El reporte final está vacío o no se generó. Verifique los archivos de entrada.")
        
        return {
            "reporte_final": final,
            "reporte_negativos": negatives,
            "reporte_correcciones": corrections
        }