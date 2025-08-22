import pandas as pd

class RecaudoR91Service:
    def procesar_recaudos(self, df_r91):
        """
        Calcula las columnas de Recaudo_Meta y Total_Recaudo.
        """
        print("🔄 Calculando recaudos desde R91...")
        
        # 1. Crear la llave 'Credito'
        df_r91['Credito'] = df_r91['Tipo_Credito'].astype(str) + '-' + df_r91['Numero_Credito'].astype(str)

        # 2. Asegurar que las columnas de recaudo sean numéricas
        columnas_recaudo = ['Recaudo_DC_Al_Dia', 'Recaudo_DC_Atraso', 'Recaudo_Atraso', 'Recaudo_Anticipado']
        for col in columnas_recaudo:
            df_r91[col] = pd.to_numeric(df_r91[col], errors='coerce').fillna(0)
            
        # 3. Calcular 'Recaudo_Meta'
        df_r91['Recaudo_Meta'] = df_r91['Recaudo_DC_Al_Dia'] + df_r91['Recaudo_DC_Atraso'] + df_r91['Recaudo_Atraso']
        
        # 4. Calcular 'Total_Recaudo'
        df_r91['Total_Recaudo'] = df_r91['Recaudo_Meta'] + df_r91['Recaudo_Anticipado']
        
        # 5. Seleccionar y devolver las columnas finales
        columnas_finales = ['Credito', 'Recaudo_Anticipado', 'Recaudo_Meta', 'Total_Recaudo']
        df_resultado = df_r91[columnas_finales]
        
        df_resultado = df_resultado.drop_duplicates(subset=['Credito'], keep='last')
        
        print("✅ Cálculo de recaudos completado.")
        return df_resultado