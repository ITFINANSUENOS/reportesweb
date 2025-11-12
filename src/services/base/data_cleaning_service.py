import pandas as pd
import numpy as np
import re
import Levenshtein 

class DataCleaningService:
    """
    Servicio dedicado a la limpieza final de datos del reporte,
    aplicando validaciones estrictas y estandarizando valores.
    """
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
            # Comparamos si el usuario es muy similar a una palabra prohibida
            if Levenshtein.distance(usuario, item_prohibido) <= 2:
                return False

        return True

    def clean_email_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica la validación estricta a la columna 'Correo' y limpia los inválidos.
        """
        print("🧼 Limpiando la columna 'Correo' con validación estricta...")
        if 'Correo' in df.columns:
            # Creamos una máscara booleana: True para correos válidos, False para inválidos
            mask_validos = df['Correo'].apply(self._es_correo_valido_estricto)
            
            # Contamos cuántos se van a limpiar para informar al usuario
            invalid_count = (~mask_validos).sum()
            if invalid_count > 0:
                print(f"   - ⚠️ Se encontraron {invalid_count} correos inválidos que serán reemplazados.")
            
            # Reemplazamos los correos inválidos (donde la máscara es False) por NaN
            df.loc[~mask_validos, 'Correo'] = np.nan
        else:
            print("   - ℹ️ Columna 'Correo' no encontrada. Se omite la limpieza.")
            
        return df
    
    def clean_phone_numbers(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpia y valida números de teléfono en columnas específicas según las reglas de Colombia,
        incluyendo filtros por prefijos y números repetidos.
        """
        print("📞 Limpiando y validando números de teléfono (lógica estricta)...")
        
        columnas_telefono = ['Celular', 'Telefono_Codeudor1', 'Telefono_Codeudor2']
        for col in columnas_telefono:
            if col not in df.columns:
                print(f"   - ℹ️ Columna '{col}' no encontrada. Se omite.")
                continue

            mask_sin_codeudor = df[col] == 'SIN CODEUDOR'
            telefonos_limpios = df[col].astype(str).str.replace(r'\D', '', regex=True)

            # 3. Definimos las reglas de validación para Colombia
            es_celular_valido = (telefonos_limpios.str.len() == 10) & (telefonos_limpios.str.startswith('3'))
            es_fijo_nacional_valido = (telefonos_limpios.str.len() == 10) & (telefonos_limpios.str.startswith('60'))
            
            prefijos_fijos_validos = ('2', '4', '5', '6', '7', '8')
            es_fijo_local_valido = (telefonos_limpios.str.len() == 7) & (telefonos_limpios.str.startswith(prefijos_fijos_validos))
            no_son_repetidos = telefonos_limpios.apply(lambda x: len(set(x)) > 1 if x else True)
            mask_formato_valido = es_celular_valido | es_fijo_nacional_valido | es_fijo_local_valido
            mask_final_valido = mask_formato_valido & no_son_repetidos
            
            # 4. Asignamos los números limpios solo a las filas que son válidas
            df[col] = telefonos_limpios.where(mask_final_valido, np.nan)

            # 5. Asignamos el valor por defecto a los inválidos
            if col == 'Celular':
                df[col] = df[col].fillna('')
            else:
                df[col] = df[col].fillna('SIN CODEUDOR')
            
            # 6. Restauramos los valores originales 'SIN CODEUDOR'
            df.loc[mask_sin_codeudor, col] = 'SIN CODEUDOR'
            
            print(f"   - ✅ Columna '{col}' procesada.")

        return df

    def run_cleaning_pipeline(self, reporte_df: pd.DataFrame) -> pd.DataFrame:
        """
        Punto de entrada principal para ejecutar todos los pasos de limpieza.
        Por ahora, solo limpia los correos, pero puedes añadir más pasos aquí en el futuro.
        """
        print("\n--- 🧹 Iniciando pipeline de limpieza final ---")
        reporte_df = self.clean_email_column(reporte_df)
        reporte_df = self.clean_phone_numbers(reporte_df)
        print("--- ✅ Pipeline de limpieza final completado ---\n")
        return reporte_df