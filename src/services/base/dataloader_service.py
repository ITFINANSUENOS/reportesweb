import pandas as pd
from pathlib import Path

class ReportDataLoader:
    """Lee y estandariza múltiples archivos de Excel basados en una configuración."""

    def __init__(self, config):
        self.config = config

    def load_dataframes(self, file_paths: list) -> dict:
        """Lee todos los archivos, los clasifica y los convierte en DataFrames."""
        dataframes_por_tipo = {key: [] for key in self.config.keys()}
        print("--- 🔄 Iniciando lectura de archivos ---")
        
        for ruta_archivo in file_paths:
            try:
                nombre_archivo = Path(ruta_archivo).name
                tipo_archivo = self._get_file_type(nombre_archivo)
                
                if not tipo_archivo:
                    print(f"⚠️  Archivo '{nombre_archivo}' omitido (tipo no reconocido).")
                    continue

                config_actual = self.config[tipo_archivo]
                df_list = self._read_file_with_config(ruta_archivo, config_actual)
                dataframes_por_tipo[tipo_archivo].extend(df_list)
                
                print(f"✅ Archivo '{nombre_archivo}' procesado como tipo '{tipo_archivo}'.")
            except Exception as e:
                print(f"❌ Error procesando el archivo '{ruta_archivo}': {e}")
        
        return dataframes_por_tipo

    def _get_file_type(self, filename: str) -> str or None:
        """Determina el tipo de archivo usando el nombre."""
        nombre_base = filename.split('.')[0].upper().replace(" ", "_")
        sorted_keys = sorted(self.config.keys(), key=len, reverse=True)

        for tipo in sorted_keys:
            clave_config = tipo.upper().replace(" ", "_")
            if nombre_base.startswith(clave_config):
                return tipo
            
            palabras_en_nombre = set(nombre_base.split('_'))
            palabras_en_clave = set(clave_config.split('_'))
            if palabras_en_clave.issubset(palabras_en_nombre):
                return tipo
        
        return None

    def _read_file_with_config(self, path: str, config: dict) -> list:
        """Helper para leer un archivo según su configuración específica (multi-hoja o simple)."""
        if "sheets" in config:
            xls = pd.ExcelFile(path, engine='openpyxl')
            dfs = []
            for sheet_config in config["sheets"]:
                if sheet_config["sheet_name"] in xls.sheet_names:
                    df_hoja = pd.read_excel(xls, sheet_name=sheet_config["sheet_name"])
                    df_hoja.columns = df_hoja.columns.str.strip()
                    cols = [c for c in sheet_config["usecols"] if c in df_hoja.columns]
                    df_filtrado = df_hoja[cols].rename(columns=sheet_config["rename_map"])
                    dfs.append({"data": df_filtrado, "config": sheet_config})
            return dfs
        else:
            engine = 'xlrd' if path.upper().endswith('.XLS') else 'openpyxl'
            df = pd.read_excel(path, engine=engine, 
                               header=config.get("header"), 
                               skiprows=config.get("skiprows"), 
                               names=config.get("new_names"))
            df.columns = df.columns.str.strip()
            cols = [c for c in config["usecols"] if c in df.columns]
            df_filtrado = df[cols].rename(columns=config["rename_map"])
            return [df_filtrado]
