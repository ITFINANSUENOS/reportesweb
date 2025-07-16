import pandas as pd
from typing import Dict
from src.models.anticipos_model import AnticiposConfig

class AnticiposDataLoader:
    """Se encarga de cargar, validar y preparar los datos desde el archivo Excel."""

    def __init__(self, config: AnticiposConfig):
        self.config = config

    def load_and_filter_data(self, file_path: str) -> Dict[str, pd.DataFrame]:
        """Carga, valida y filtra los datos del archivo Excel según la configuración."""
        try:
            self._validate_input_file(file_path)
            dfs = pd.read_excel(file_path, sheet_name=None)
            
            filtered_data = {}
            for sheet_name, columns in self.config.sheet_columns.items():
                if sheet_name not in dfs:
                    raise ValueError(f"Hoja '{sheet_name}' no encontrada en el archivo.\n")
                
                df = dfs[sheet_name][columns].copy()
                if sheet_name in self.config.rename_columns:
                    df.rename(columns=self.config.rename_columns[sheet_name], inplace=True)
                filtered_data[sheet_name] = df
                
            return filtered_data
        except Exception as e:
            raise ValueError(f"Error al cargar datos:\n {e}")

    def _validate_input_file(self, file_path: str) -> bool:
        """Valida que el archivo Excel contenga todas las hojas requeridas."""
        try:
            with pd.ExcelFile(file_path) as xls:
                sheets = xls.sheet_names
                missing_sheets = [s for s in self.config.required_sheets if s not in sheets]
                if missing_sheets:
                    raise ValueError(f"Faltan hojas requeridas: \n {', '.join(missing_sheets)}")
            return True
        except Exception as e:
            raise ValueError(f"Error al validar el archivo: \n {e}")