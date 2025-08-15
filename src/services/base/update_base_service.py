import pandas as pd

class UpdateBaseService:
    def __init__(self, report_service):
        self.report_service = report_service

    def sincronizar_reporte(self, df_base_anterior, dataframes_nuevos):
        """
        Sincroniza el reporte del mes anterior con los datos nuevos.
        1. Actualiza registros existentes.
        2. Añade registros nuevos.
        3. Elimina registros que ya no existen en el R91 nuevo.
        """
        print("⚡ Ejecutando sincronización rápida con la base anterior...")

        # --- CAMBIO CLAVE 1: Capturar los dos resultados de _preparar_datos_nuevos ---
        # 1. Procesar los archivos nuevos para tener una "mini-base" actualizada.
        #    Ignoramos el reporte de negativos, ya que se generará uno nuevo al final.
        df_datos_nuevos, _ = self.report_service._preparar_datos_nuevos(dataframes_nuevos)
        
        if df_datos_nuevos.empty:
            print("⚠️ Los archivos nuevos no contienen datos válidos. Sincronización cancelada.")
            return df_base_anterior

        # 2. Unir la base anterior con los datos nuevos
        df_merged = pd.merge(
            df_base_anterior,
            df_datos_nuevos,
            on='Credito',
            how='outer',
            suffixes=('_anterior', '_nuevo')
        )

        # 3. Identificar los créditos que deben permanecer (los del R91 nuevo)
        creditos_actuales = set(df_datos_nuevos['Credito'])
        df_final = df_merged[df_merged['Credito'].isin(creditos_actuales)].copy()

        # --- CAMBIO CLAVE 2: Usar combine_first para una actualización más robusta ---
        # 4. "Coalesce": Actualizar las columnas, dando prioridad a los datos nuevos.
        print("   - Actualizando información...")
        
        columnas_para_actualizar = df_datos_nuevos.columns.difference(['Credito'])

        for col in columnas_para_actualizar:
            col_nuevo = f"{col}_nuevo"
            col_anterior = f"{col}_anterior"
            
            if col_nuevo in df_final.columns and col_anterior in df_final.columns:
                # combine_first toma el valor de '_nuevo' si existe; si es nulo, toma el de '_anterior'.
                # Es más seguro que fillna para este caso.
                df_final[col] = df_final[col_nuevo].combine_first(df_final[col_anterior])

        # 5. Limpiar las columnas temporales
        columnas_a_limpiar = [c for c in df_final.columns if c.endswith('_anterior') or c.endswith('_nuevo')]
        df_final.drop(columns=columnas_a_limpiar, inplace=True, errors='ignore')
        
        print(f"✅ Sincronización completada. Total de registros: {len(df_final)}")
        return df_final