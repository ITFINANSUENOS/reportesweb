import pandas as pd
from typing import List,Dict, Optional

class EcollectService:
    """
    Contiene toda la lógica de negocio para procesar los archivos de Ecollect.
    """
    def __init__(self, config: Dict):
        """
        Inicializa el servicio con la configuración necesaria.
        
        Args:
            config (Dict): El diccionario de configuración del modelo.
        """
        self.config = config.get("VENCIMIENTOS", {})
        if not self.config:
            raise ValueError("La configuración para 'VENCIMIENTOS' no fue encontrada.")

    def _load_and_prepare_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """Carga y prepara un archivo Excel según la configuración."""
        try:
            df = pd.read_excel(
                file_path,
                usecols=self.config.get("usecols"),
                dtype={'MCNVINCULA': str, 'MCNNUMCRU1': str} # Asegurar que los IDs se lean como texto
            )
            df.rename(columns=self.config.get("rename_map", {}), inplace=True)
            return df
        except FileNotFoundError:
            print(f"Error: El archivo no fue encontrado en la ruta {file_path}")
            return None
        except Exception as e:
            print(f"Ocurrió un error al leer el archivo: {e}")
            return None

    def process_vencimientos(self, file_paths: List[str]) -> Optional[pd.DataFrame]:
        """
        Orquesta el proceso de transformación para múltiples archivos de vencimientos.
        """
        # --- CAMBIO: Cargar y combinar todos los archivos en un solo DataFrame ---
        all_dfs = []
        for path in file_paths:
            df_temp = self._load_and_prepare_file(path)
            if df_temp is not None:
                all_dfs.append(df_temp)
        
        if not all_dfs:
            print("No se pudo cargar ningún archivo correctamente.")
            return None
            
        df = pd.concat(all_dfs, ignore_index=True)

        # --- 1. Crear columna 'Credito' ---
        df["Credito"] = df["Tipo_Credito"].astype(str) + "-" + df["Numero_Credito"].astype(str)

        # --- 2. Limpiar la columna 'Cuota_Vigente' ---
        # Convierte a string, toma los últimos 2 caracteres y luego a número.
        df["Cuota_Vigente"] = pd.to_numeric(
            df["Cuota_Vigente"].astype(str).str[-2:], 
            errors='coerce' # Si algo falla, lo convierte en NaN
        ).fillna(0).astype(int)

        # --- 3. Filtrar cuotas atrasadas y del mes cursante ---
        df["Fecha_Cuota_Vigente"] = pd.to_datetime(df["Fecha_Cuota_Vigente"], errors='coerce')
        df.dropna(subset=["Fecha_Cuota_Vigente"], inplace=True) # Eliminar filas con fechas inválidas
        
        fecha_fin_de_mes = pd.Timestamp.now() + pd.offsets.MonthEnd(0)
        cuotas_a_procesar = df[df["Fecha_Cuota_Vigente"] <= fecha_fin_de_mes].copy()
        
        cuotas_a_procesar.sort_values(by="Fecha_Cuota_Vigente", inplace=True)

        # --- 4. Agrupar por cliente y crédito para los cálculos ---
        grouped = cuotas_a_procesar.groupby(["Cedula_Cliente", "Credito"])
        
        if grouped.groups:
            agg_data = grouped.agg(
                Primera_Cuota_Atraso=("Cuota_Vigente", "first"),
                Fecha_Atraso=("Fecha_Cuota_Vigente", "first"),
                Ultima_Cuota_Atraso=("Cuota_Vigente", "last"),
                Pago_Total=("Valor_Cuota", "sum"),
                Total_Intereses=("Intereses", "sum")
            ).reset_index()

            # --- 5. Crear los dos registros ('Pago_Total' e 'Intereses') ---
            pago_total_df = agg_data.copy()
            pago_total_df["Codigo"] = 0
            pago_total_df["Valor"] = pago_total_df["Pago_Total"]

            intereses_df = agg_data[agg_data["Total_Intereses"] > 0].copy()
            if not intereses_df.empty:
                intereses_df["Codigo"] = 40
                intereses_df["Valor"] = intereses_df["Total_Intereses"]

            # --- 6. Combinar y limpiar ---
            final_df = pd.concat([pago_total_df, intereses_df], ignore_index=True)
            
            columnas_finales = [
                "Cedula_Cliente", "Credito", "Primera_Cuota_Atraso", "Fecha_Atraso",
                "Ultima_Cuota_Atraso", "Codigo", "Valor"
            ]
            final_df = final_df[columnas_finales]
            final_df.sort_values(by=["Cedula_Cliente", "Credito", "Codigo"], inplace=True)
            
            return final_df
        else:
            print("No se encontraron cuotas para procesar (vencidas o del mes actual).")
            return pd.DataFrame()
