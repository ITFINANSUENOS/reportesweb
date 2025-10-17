import pandas as pd
from Levenshtein import distance
import re
from typing import Dict, Optional, List

class UsuariosService:
    """
    Servicio para enriquecer datos de vencimientos con información de usuarios (correo).
    """
    def __init__(self, config: Dict):
        self.config = config

    def _tiene_diversidad(self, texto: str, umbral: int = 3) -> bool:
        """
        Verifica si el texto tiene un número mínimo de caracteres alfabéticos únicos.
        """
        if not isinstance(texto, str):
            return False
        letras_unicas = set(c for c in texto.lower() if c.isalpha())
        return len(letras_unicas) >= umbral

    def _es_correo_valido_estricto(self, correo: str) -> bool:
        """
        Valida un correo electrónico usando una lógica estricta y avanzada.
        """
        if not isinstance(correo, str) or not correo:
            return False
        correo = correo.strip()
        pattern = re.compile(
            r"^(?![.-])(?!(?:.*[.]{2}))[A-Z0-9._%+-ñÑ]{3,}@[A-Z0-9.-]{3,}\.[A-Z]{2,}$",
            re.IGNORECASE
        )
        if not pattern.match(correo):
            return False
        try:
            usuario, _ = correo.split('@', 1)
            usuario = usuario.lower()
        except ValueError:
            return False # No tiene un solo @

        if usuario.isdigit():
            return False
            
        if not self._tiene_diversidad(usuario):
            return False

        blacklist = [
            "notiene", "sincorreo", "pendiente", "corregir", "noregistra",
            "nulo", "ninguno", "vacio", "nodisponible"
        ]
        for item_prohibido in blacklist:
            if distance(usuario, item_prohibido) <= 2:
                return False

        return True    

    def _load_and_prepare_file(self, file_path: str, config_key: str) -> Optional[pd.DataFrame]:
        """
        Método genérico para cargar un archivo Excel usando una clave de configuración específica.
        """
        config_part = self.config.get(config_key, {})
        if not config_part:
            raise ValueError(f"La configuración para '{config_key}' no fue encontrada.")
        
        try:
            df = pd.read_excel(
                file_path,
                usecols=config_part.get("usecols"),
                # Dtypes dinámicos para asegurar que los IDs siempre sean texto
                dtype={col: str for col in ['MCNVINCULA', 'MCNNUMCRU1', 'NUMERO_DOC', 'IDENTIFICA'] if col in config_part.get("usecols", [])}
            )
            df.rename(columns=config_part.get("rename_map", {}), inplace=True)
            return df
        except Exception as e:
            print(f"Ocurrió un error al leer el archivo ({config_key}): {e}")
            return None

    def crear_dataframe_usuarios(self, vencimientos_paths: List[str], consulta_path: str) -> Optional[pd.DataFrame]:
        """
        Carga los archivos de vencimientos y consulta, y los une para agregar el correo.
        """
        # --- 1. Cargar y consolidar los archivos de VENCIMIENTOS ---
        vencimientos_dfs = []
        for path in vencimientos_paths:
            df_temp = self._load_and_prepare_file(path, "VENCIMIENTOS")
            if df_temp is not None: vencimientos_dfs.append(df_temp)
        if not vencimientos_dfs: return None
        df_vencimientos = pd.concat(vencimientos_dfs, ignore_index=True)
        
        df_consulta = self._load_and_prepare_file(consulta_path, "CRTMPCONSULTA1")
        if df_consulta is None: return None
        
        df_vencimientos["Credito"] = df_vencimientos["Tipo_Credito"].astype(str) + "-" + df_vencimientos["Numero_Credito"].astype(str)
        df_consulta["Credito"] = df_consulta["Tipo_Credito"].astype(str) + "-" + df_consulta["Numero_Credito"].astype(str)
        df_consulta_limpio = df_consulta[["Cedula_Cliente", "Credito", "Correo"]].drop_duplicates()
        
        df_final = pd.merge(df_vencimientos, df_consulta_limpio, on=["Cedula_Cliente", "Credito"], how="left")
        df_final.drop_duplicates(subset=['Credito'], keep='first', inplace=True)

        # --- 3. APLICAR LA LIMPIEZA A LA COLUMNA DE CORREOS ---
        # Si el correo es válido, se conserva; de lo contrario, se reemplaza por un string vacío.
        df_final['Correo'] = df_final['Correo'].apply(
            lambda email: email if self._es_correo_valido_estricto(email) else ''
        )

        return df_final