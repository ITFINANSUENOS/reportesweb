import pandas as pd
from typing import Dict, Optional

class FileHandlerService:
    """
    Servicio dedicado a la lectura y escritura de archivos,
    especialmente formatos como Excel.
    """

    def read_excel_base(self, file_path: str) -> Optional[pd.DataFrame]:
        """
        Lee un archivo Excel y lo devuelve como un DataFrame de pandas.
        Maneja posibles errores durante la lectura.

        Args:
            file_path: La ruta al archivo Excel.

        Returns:
            Un DataFrame con los datos o None si ocurre un error.
        """
        try:
            print(f"📖 Leyendo archivo base desde: {file_path}")
            return pd.read_excel(file_path, dtype=str)
        except Exception as e:
            print(f"Error al leer el archivo Excel base: {e}")
            raise ValueError(f"No se pudo leer el archivo Excel base: {e}")

    def _apply_styles(self, val: str) -> str:
        """Aplica estilos de fondo a las celdas según su valor."""
        if val == 'CORREGIR':
            return 'background-color: #FFCDD2'  # Rojo claro
        if val == 'BIEN':
            return 'background-color: #C8E6C9'  # Verde claro
        return 'background-color: #FFFFFF'      # Blanco

    def save_report_to_excel(self, output_path: str, reports: Dict[str, pd.DataFrame]):
        """
        Guarda múltiples DataFrames en un único archivo Excel, cada uno en una hoja.

        Args:
            output_path: La ruta donde se guardará el archivo.
            reports: Un diccionario donde la clave es el nombre de la hoja
                     y el valor es el DataFrame a guardar.
        """
        print(f"💾 Guardando reporte en {output_path}...")
        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # Guardar el reporte principal
                main_report = reports.get('reporte_final')
                if main_report is not None and not main_report.empty:
                    main_report.to_excel(writer, sheet_name='Reporte Consolidado', index=False)
                    print("  - Hoja 'Reporte Consolidado' guardada.")

                # Guardar créditos negativos si existen
                negative_report = reports.get('reporte_negativos')
                if negative_report is not None and not negative_report.empty:
                    negative_report.to_excel(writer, sheet_name='Creditos_Negativos', index=False)
                    print("  - Hoja 'Creditos_Negativos' añadida.")

                # Guardar registros para corregir con estilos si existen
                corrections_report = reports.get('reporte_correcciones')
                if corrections_report is not None and not corrections_report.empty:
                    print("  - 🎨 Aplicando estilos a la hoja de correcciones...")
                    styled_df = corrections_report.style.applymap(self._apply_styles)
                    styled_df.to_excel(writer, sheet_name='Registros_Para_Corregir', index=False)
                    print("  - ✅ Hoja 'Registros_Para_Corregir' añadida con colores.")
        except Exception as e:
            print(f"Error al guardar el archivo Excel: {e}")
            raise IOError(f"No se pudo guardar el archivo Excel: {e}")