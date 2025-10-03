import pandas as pd
import numpy as np

class AnalisisService:
    def __init__(self, config):
        self.config = config

    def calcular_rodamiento(self, df_base, df_analisis_unido):
        print("🔄 Calculando rodamiento...")
        
        if 'Tipo_Credito' in df_analisis_unido.columns and 'Numero_Credito' in df_analisis_unido.columns:
            df_analisis_unido['Credito'] = df_analisis_unido['Tipo_Credito'].astype(str) + '-' + df_analisis_unido['Numero_Credito'].astype(str)
        else:
            raise KeyError("El archivo de Análisis no contiene las columnas 'Tipo_Credito' o 'Numero_Credito' necesarias para crear la llave 'Credito'.")

        df_analisis_unido.drop_duplicates(subset=['Credito'], keep='last', inplace=True)
        df_actualizado = pd.merge(df_base, df_analisis_unido[['Credito', 'Dias_Atraso_Final']], on='Credito', how='left')
        
        # --- Depuración (puedes eliminar esto si ya no lo necesitas) ---
        print("\n--- Depurando Tipos de Datos ---")
        # ... (código de depuración) ...
        print("--------------------------------------\n")
        
        # --- Cálculo de Franjas (sin cambios) ---
        condiciones = [
            df_actualizado['Dias_Atraso_Final'] == 0,
            df_actualizado['Dias_Atraso_Final'].between(1, 30),
            df_actualizado['Dias_Atraso_Final'].between(31, 90),
            df_actualizado['Dias_Atraso_Final'].between(91, 180),
            df_actualizado['Dias_Atraso_Final'].between(181, 360),
            df_actualizado['Dias_Atraso_Final'] > 360
        ]
        valores = ['AL DIA', '1 A 30', '31 A 90', '91 A 180', '181 A 360', 'MAS DE 360']
        df_actualizado['Franja_Meta_Final'] = np.select(condiciones, valores, default=None)

        condiciones_cartea = [
            (df_actualizado['Dias_Atraso_Final'] == 0).fillna(False),
            (df_actualizado['Dias_Atraso_Final'].between(1, 30)).fillna(False),
            (df_actualizado['Dias_Atraso_Final'].between(31, 60)).fillna(False),
            (df_actualizado['Dias_Atraso_Final'].between(61, 90)).fillna(False),
            (df_actualizado['Dias_Atraso_Final'].between(91, 120)).fillna(False),
            (df_actualizado['Dias_Atraso_Final'].between(121, 150)).fillna(False),
            (df_actualizado['Dias_Atraso_Final'].between(151, 180)).fillna(False),
            (df_actualizado['Dias_Atraso_Final'].between(181, 210)).fillna(False),
            (df_actualizado['Dias_Atraso_Final'].between(211, 270)).fillna(False),
            (df_actualizado['Dias_Atraso_Final'].between(271, 360)).fillna(False),
            (df_actualizado['Dias_Atraso_Final'] > 360).fillna(False)
        ]
        valores_cartea = [
            'AL DIA', '1 A 30', '31 A 60', '61 A 90', '91 A 120',
            '121 A 150', '151 A 180', '181 A 210', '211 A 270',
            '271 A 360', 'MAS DE 360'
        ]
        df_actualizado['Franja_Cartera_Final'] = np.select(condiciones_cartea, valores_cartea, default='SIN INFO')

        # --- Cálculo de 'Rodamiento' (con la lógica ajustada) ---
        franja_map = {'AL DIA': 0, '1 A 30': 1, '31 A 90': 2, '91 A 180': 3, '181 A 360': 4, 'MAS DE 360': 5}
        df_actualizado['Franja_Meta_Num'] = df_actualizado['Franja_Meta'].map(franja_map)
        df_actualizado['Franja_Meta_Final_Num'] = df_actualizado['Franja_Meta_Final'].map(franja_map)

        cond_rodamiento = [
            df_actualizado['Dias_Atraso_Final'].isnull(),
            # --- MODIFICADO: Cambiamos > 1 por > 0 para incluir CUALQUIER atraso previo ---
            (df_actualizado['Franja_Meta_Num'] > 0) & (df_actualizado['Franja_Meta_Final_Num'] == 0),
            df_actualizado['Franja_Meta_Final_Num'] < df_actualizado['Franja_Meta_Num'],
            df_actualizado['Franja_Meta_Final_Num'] > df_actualizado['Franja_Meta_Num'],
            df_actualizado['Franja_Meta_Final_Num'] == df_actualizado['Franja_Meta_Num']
        ]
        valores_rodamiento = ['PAGO TOTAL', 'NORMALIZO', 'MEJORO', 'EMPEORO', 'SE MANTIENE']
        df_actualizado['Rodamiento'] = np.select(cond_rodamiento, valores_rodamiento, default='SIN INFO')

        # --- Cálculo de 'Rodamiento_Cartera' (con la misma lógica ajustada) ---
        franja_cartera_map = {
            'AL DIA': 0, '1 A 30': 1, '31 A 60': 2, '61 A 90': 3,
            '91 A 120': 4, '121 A 150': 5, '151 A 180': 6,
            '181 A 210': 7, '211 A 270': 8, '271 A 360': 9, 'MAS DE 360': 10
        }
        df_actualizado['Franja_Cartera_Num'] = df_actualizado['Franja_Cartera'].map(franja_cartera_map)
        df_actualizado['Franja_Cartera_Final_Num'] = df_actualizado['Franja_Cartera_Final'].map(franja_cartera_map)

        cond_rodamiento_cartera = [
            df_actualizado['Dias_Atraso_Final'].isnull(),
            # --- MODIFICADO: Cambiamos > 1 por > 0 para incluir CUALQUIER atraso previo ---
            (df_actualizado['Franja_Cartera_Num'] > 0) & (df_actualizado['Franja_Cartera_Final_Num'] == 0),
            df_actualizado['Franja_Cartera_Final_Num'] < df_actualizado['Franja_Cartera_Num'],
            df_actualizado['Franja_Cartera_Final_Num'] > df_actualizado['Franja_Cartera_Num'],
            df_actualizado['Franja_Cartera_Final_Num'] == df_actualizado['Franja_Cartera_Num']
        ]
        df_actualizado['Rodamiento_Cartera'] = np.select(cond_rodamiento_cartera, valores_rodamiento, default='SIN INFO')

        # --- Limpieza de columnas (sin cambios) ---
        df_actualizado.drop(
            columns=['Franja_Meta_Num', 'Franja_Meta_Final_Num',
                    'Franja_Cartera_Num', 'Franja_Cartera_Final_Num'],
            inplace=True
        )
        
        print("✅ Cálculo de rodamiento completado.")
        return df_actualizado