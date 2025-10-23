import pandas as pd
import re 

class ColaboradoresService:
    def __init__(self, configuracion):
        """
        Inicializa el servicio con la configuración específica 
        para COLABORADORES.
        """
        try:
            self.config = configuracion["COLABORADORES"]
            self.config_cartera = next(
                sheet for sheet in self.config["sheets"] if sheet["sheet_name"] == "CARTERA"
            )
            self.config_usuarios = next(
                sheet for sheet in self.config["sheets"] if sheet["sheet_name"] == "USUARIOS"
            )
        except (KeyError, StopIteration) as e:
            raise ValueError(f"Error al inicializar ColaboradoresService: {e}")

    def process_cartera(self, file_path: str) -> pd.DataFrame | None:
        """
        Procesa la hoja 'CARTERA' y añade las columnas faltantes 
        que el plano_service necesita.
        """
        try:
            df_cartera_colab = pd.read_excel(
                file_path,
                sheet_name=self.config_cartera["sheet_name"],
                usecols=self.config_cartera["usecols"]
            )
            df_cartera_colab = df_cartera_colab.rename(
                columns=self.config_cartera["rename_map"]
            )
            if df_cartera_colab.empty:
                print("Advertencia (Colaboradores): No se encontraron datos en la hoja 'CARTERA'.")
                return None
            
            def extraer_numero_cuota(texto: str) -> int:
                """Extrae solo el número de un texto como 'Pago de Cuota No. 11'."""
                if pd.isnull(texto):
                    return 0
                # Busca uno o más dígitos en el string
                numeros = re.findall(r'\d+', str(texto))
                if numeros:
                    return int(numeros[-1]) 
                return 0
            df_cartera_colab['Primera_Cuota_Atraso'] = df_cartera_colab['Primera_Cuota_Atraso'].apply(extraer_numero_cuota)
            df_cartera_colab['Ultima_Cuota_Atraso'] = df_cartera_colab['Primera_Cuota_Atraso']
            df_cartera_colab['Codigo'] = 0 
            
            # 3. Aseguramos tipos de datos que el plano_service espera
            df_cartera_colab['Valor'] = pd.to_numeric(df_cartera_colab['Valor'], errors='coerce').fillna(0)
            df_cartera_colab['Fecha_Atraso'] = pd.to_datetime(df_cartera_colab['Fecha_Atraso'])

            return df_cartera_colab

        except Exception as e:
            print(f"Error (Colaboradores) al leer o transformar 'CARTERA': {e}")
            return None

    def process_usuarios(self, file_path: str) -> pd.DataFrame | None:
        """
        Procesa la hoja 'USUARIOS'. (Sin cambios)
        """
        try:
            df_usuarios_colab = pd.read_excel(
                file_path,
                sheet_name=self.config_usuarios["sheet_name"],
                usecols=self.config_usuarios["usecols"]
            )
            df_usuarios_colab = df_usuarios_colab.rename(
                columns=self.config_usuarios["rename_map"]
            )
            if df_usuarios_colab.empty:
                print("Advertencia (Colaboradores): No se encontraron datos en la hoja 'USUARIOS'.")
                return None
            return df_usuarios_colab
        except Exception as e:
            print(f"Error (Colaboradores) al leer la hoja 'USUARIOS': {e}")
            return None