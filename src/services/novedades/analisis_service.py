import pandas as pd
import numpy as np

class AnalisisService:
    def __init__(self, config):
        self.config = config

    def calcular_rodamiento(self, df_base, df_analisis_unido):
        print("🔄 Calculando rodamiento...")
        
        # 1. El controlador ya ha cargado y unido los archivos.
        if 'Tipo_Credito' in df_analisis_unido.columns and 'Numero_Credito' in df_analisis_unido.columns:
            df_analisis_unido['Credito'] = df_analisis_unido['Tipo_Credito'].astype(str) + '-' + df_analisis_unido['Numero_Credito'].astype(str)
        else:
            raise KeyError("El archivo de Análisis no contiene las columnas 'Tipo_Credito' o 'Numero_Credito' necesarias para crear la llave 'Credito'.")

        df_analisis_unido.drop_duplicates(subset=['Credito'], keep='last', inplace=True)
        # 2. Unir el análisis al reporte base (left merge es clave)
        df_actualizado = pd.merge(df_base, df_analisis_unido[['Credito', 'Dias_Atraso_Final']], on='Credito', how='left')
        
        # 3. Calcular la 'Franja_Mora_Final'
        condiciones = [
            df_actualizado['Dias_Atraso_Final'] == 0,
            df_actualizado['Dias_Atraso_Final'].between(1, 30),
            df_actualizado['Dias_Atraso_Final'].between(31, 90),
            df_actualizado['Dias_Atraso_Final'].between(91, 180),
            df_actualizado['Dias_Atraso_Final'].between(181, 360),
            df_actualizado['Dias_Atraso_Final'] > 360
        ]
        valores = ['AL DIA', '1 A 30', '31 A 90', '91 A 180', '181 A 360', 'MAS DE 360']
        df_actualizado['Franja_Mora_Final'] = np.select(condiciones, valores, default=None)

        # 4. Calcular el 'Rodamiento' (el resto de la lógica no cambia)
        franja_map = {'AL DIA': 0, '1 A 30': 1, '31 A 90': 2, '91 A 180': 3, '181 A 360': 4, 'MAS DE 360': 5}
        df_actualizado['Franja_Mora_Num'] = df_actualizado['Franja_Mora'].map(franja_map)
        df_actualizado['Franja_Mora_Final_Num'] = df_actualizado['Franja_Mora_Final'].map(franja_map)

        cond_rodamiento = [
            df_actualizado['Dias_Atraso_Final'].isnull(),
            (df_actualizado['Franja_Mora_Num'] > 1) & (df_actualizado['Franja_Mora_Final_Num'] == 0),
            df_actualizado['Franja_Mora_Final_Num'] < df_actualizado['Franja_Mora_Num'],
            df_actualizado['Franja_Mora_Final_Num'] > df_actualizado['Franja_Mora_Num'],
            df_actualizado['Franja_Mora_Final_Num'] == df_actualizado['Franja_Mora_Num']
        ]
        valores_rodamiento = ['PAGO TOTAL', 'NORMALIZO', 'MEJORO', 'EMPEORO', 'SE MANTIENE']
        df_actualizado['Rodamiento'] = np.select(cond_rodamiento, valores_rodamiento, default='SIN INFO')

        df_actualizado.drop(columns=['Franja_Mora_Num', 'Franja_Mora_Final_Num'], inplace=True)
        
        print("✅ Cálculo de rodamiento completado.")
        return df_actualizado