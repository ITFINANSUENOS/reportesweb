import pandas as pd
import numpy as np

class CategorizationService:
    """
    Servicio para aplicar categorizaciones y mapeos de datos
    basados en reglas de negocio, como la asignación de gestores
    y franjas de mora.
    """
    def map_call_center_data(self, reporte_df):
        """
        Limpia la columna 'Gestor' y crea las columnas 'Franja_Meta', 'Franja_Cartera'
        y de Call Center consolidadas, basándose en los días de atraso.
        """
        print("📞 Mapeando datos de Gestor y Call Center...")

        if 'Gestor' in reporte_df.columns:
            print("   - Limpiando columna 'Gestor'...")
            reporte_df.loc[reporte_df['Gestor'] == 'SIN GESTOR', 'Gestor'] = 'CALL CENTER'
            reporte_df['Gestor'].fillna('OTRAS ZONAS', inplace=True)
        else:
            print("   - ⚠️ Columna 'Gestor' no encontrada. Se omite la limpieza.")

        if 'Dias_Atraso' not in reporte_df.columns:
            print("⚠️ Columna 'Dias_Atraso' no encontrada. No se puede mapear la franja de mora.")
            return reporte_df
            
        reporte_df['Dias_Atraso'] = pd.to_numeric(reporte_df['Dias_Atraso'], errors='coerce')

        condiciones_mora = [
            reporte_df['Dias_Atraso'] == 0, reporte_df['Dias_Atraso'].between(1, 30),
            reporte_df['Dias_Atraso'].between(31, 90), reporte_df['Dias_Atraso'].between(91, 180),
            reporte_df['Dias_Atraso'].between(181, 360), reporte_df['Dias_Atraso'] > 360
        ]
        valores_mora = ['AL DIA', '1 A 30', '31 A 90', '91 A 180','181 A 360','MAS DE 360']
        reporte_df['Franja_Meta'] = np.select(condiciones_mora, valores_mora, default='SIN INFO')
        
        condiciones_cartera = [
            reporte_df['Dias_Atraso'] == 0, reporte_df['Dias_Atraso'].between(1, 30),
            reporte_df['Dias_Atraso'].between(31, 60), reporte_df['Dias_Atraso'].between(61, 90),
            reporte_df['Dias_Atraso'].between(91, 120), reporte_df['Dias_Atraso'].between(121, 150),
            reporte_df['Dias_Atraso'].between(151, 180), reporte_df['Dias_Atraso'].between(181, 210),
            reporte_df['Dias_Atraso'].between(211, 270), reporte_df['Dias_Atraso'].between(271, 360),
            reporte_df['Dias_Atraso'] > 360
        ]
        valores_cartera = [
            'AL DIA', '1 A 30', '31 A 60', '61 A 90', '91 A 120', '121 A 150',
            '151 A 180', '181 A 210', '211 A 270', '271 A 360', 'MAS DE 360'
        ]
        reporte_df['Franja_Cartera'] = np.select(condiciones_cartera, valores_cartera, default='SIN INFO')
        
        mapa_franjas = {
            '1 A 30': ('call_center_1_30_dias', 'call_center_nombre_1_30', 'call_center_telefono_1_30'),
            '31 A 90': ('call_center_31_90_dias', 'call_center_nombre_31_90', 'call_center_telefono_31_90'),
            '91 A 180': ('call_center_91_360_dias', 'call_center_nombre_91_360', 'call_center_telefono_91_360'),
            '181 A 360': ('call_center_91_360_dias', 'call_center_nombre_91_360', 'call_center_telefono_91_360')
        }

        reporte_df['Call_Center_Apoyo'] = np.nan
        reporte_df['Nombre_Call_Center'] = np.nan
        reporte_df['Telefono_Call_Center'] = np.nan

        print("   - Asignando datos de Call Center por franja de mora...")
        for franja, cols in mapa_franjas.items():
            mask = reporte_df['Franja_Meta'] == franja
            if cols[0] in reporte_df.columns:
                reporte_df.loc[mask, 'Call_Center_Apoyo'] = reporte_df.loc[mask, cols[0]]
            if cols[1] in reporte_df.columns:
                reporte_df.loc[mask, 'Nombre_Call_Center'] = reporte_df.loc[mask, cols[1]]
            if cols[2] in reporte_df.columns:
                reporte_df.loc[mask, 'Telefono_Call_Center'] = reporte_df.loc[mask, cols[2]]

        cols_a_borrar_matriz = [item for sublist in mapa_franjas.values() for item in sublist]
        columnas_existentes_a_borrar = [col for col in cols_a_borrar_matriz if col in reporte_df.columns]
        reporte_df.drop(columns=columnas_existentes_a_borrar, inplace=True, errors='ignore')
        
        print("✅ Mapeo completado.")
        return reporte_df
    
    def calculate_last_payment_range(self, reporte_df):
        """
        Calcula el rango de tiempo desde el último pago inicial hasta una fecha de referencia.
        Crea la columna 'Rango_Ultimo_pago_Inicial'.
        """
        print("📅 Calculando el rango de la fecha de último pago inicial...")

        # Verificamos que la columna necesaria exista
        if 'Fecha_Ultimo_pago_Inicial' not in reporte_df.columns:
            print("   - ⚠️ Columna 'Fecha_Ultimo_pago_Inicial' no encontrada. Se omite el cálculo del rango.")
            return reporte_df
        
        # 1. Aseguramos que la columna sea del tipo datetime
        reporte_df['Fecha_Ultimo_pago_Inicial'] = pd.to_datetime(reporte_df['Fecha_Ultimo_pago_Inicial'], errors='coerce')

        # 2. Definimos la fecha de referencia (día 5 del mes actual)
        hoy = pd.Timestamp.now()
        fecha_referencia = hoy.replace(day=5)

        # 3. Calculamos las fechas límite
        fecha_6_meses = fecha_referencia - pd.DateOffset(months=6)
        fecha_12_meses = fecha_referencia - pd.DateOffset(months=12)
        fecha_24_meses = fecha_referencia - pd.DateOffset(months=24)
        fecha_48_meses = fecha_referencia - pd.DateOffset(months=48)

        # 4. Definimos las condiciones de clasificación
        condiciones_pago = [
            reporte_df['Fecha_Ultimo_pago_Inicial'] > fecha_6_meses,
            reporte_df['Fecha_Ultimo_pago_Inicial'].between(fecha_12_meses, fecha_6_meses, inclusive='right'),
            reporte_df['Fecha_Ultimo_pago_Inicial'].between(fecha_24_meses, fecha_12_meses, inclusive='right'),
            reporte_df['Fecha_Ultimo_pago_Inicial'].between(fecha_48_meses, fecha_24_meses, inclusive='right'),
            reporte_df['Fecha_Ultimo_pago_Inicial'] <= fecha_48_meses
        ]
        
        # 5. Definimos los valores para cada rango
        valores_pago = [
            '6 MESES',
            '6 A 12 MESES',
            '1 a 2 AÑOS',
            '2 A 4 AÑOS',
            'MAS 4 AÑOS'
        ]

        # 6. Creamos la columna usando np.select
        reporte_df['Rango_Ultimo_pago_Inicial'] = np.select(
            condiciones_pago, 
            valores_pago, 
            default='SIN PAGO REGISTRADO'
        )
        
        print("✅ Rango de último pago inicial calculado.")
        return reporte_df