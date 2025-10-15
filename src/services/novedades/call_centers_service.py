import pandas as pd
import numpy as np

class CallCenterService:
    """
    Servicio para generar reportes de rendimiento específicos para Call Centers.
    """

    def _limpiar_y_preparar_datos(self, df):
        """
        Realiza una limpieza inicial de los datos necesarios para el reporte.
        """
        print("🧹 Limpiando y preparando datos para el reporte de Call Centers...")
        df_copy = df.copy()

        # --- Columnas numéricas requeridas ---
        columnas_numericas = ['Meta_General', 'Meta_$', 'Recaudo_Meta']
        for col in columnas_numericas:
            if col in df_copy.columns:
                df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').fillna(0)
            else:
                # Si una columna numérica crucial no existe, la creamos con ceros
                df_copy[col] = 0

        # --- Columnas de texto requeridas ---
        columnas_texto = [
            'Zona', 'Cobrador', 'Call_Center_Apoyo', 'Nombre_Call_Center', 'Franja_Meta'
        ]
        for col in columnas_texto:
            if col in df_copy.columns:
                df_copy[col] = df_copy[col].astype(str).str.strip().str.upper().replace('NAN', '')
            else:
                # Si una columna de texto no existe, la creamos vacía
                df_copy[col] = ''
                
        return df_copy

    def generar_reporte_call_center(self, df_analisis_cartera):
        """
        Genera un reporte consolidado del rendimiento de los Call Centers.

        Combina datos de 'Zona' (para CL1-CL4) y 'Call_Center_Apoyo' (para CL5-CL9),
        aplica diferentes reglas para la meta y calcula el rendimiento.
        """
        print("🔄 Iniciando la generación del reporte de Call Centers...")
        
        df = self._limpiar_y_preparar_datos(df_analisis_cartera)

        # --- Parte 1: Procesar Call Centers CL1 a CL4 ---
        print("📊 Procesando Call Centers CL1 - CL4...")
        df_cl1_4 = df[df['Zona'].isin(['CL1', 'CL2', 'CL3', 'CL4']) & (df['Franja_Meta'] == 'AL DIA')]
        
        # Agrupamos para consolidar los datos
        agg_cl1_4 = df_cl1_4.groupby(['Zona', 'Cobrador']).agg(
            META = pd.NamedAgg(column='Meta_General', aggfunc='sum'),
            Recaudo_Meta=pd.NamedAgg(column='Recaudo_Meta', aggfunc='sum')
        ).reset_index()

        # Renombramos las columnas para estandarizar
        agg_cl1_4.rename(columns={'Zona': 'CALL_CENTER', 'Cobrador': 'NOMBRE'}, inplace=True)


        # --- Parte 2: Procesar Call Centers CL5 a CL9 ---
        print("📊 Procesando Call Centers CL5 - CL9...")
        df_cl5_9 = df[df['Call_Center_Apoyo'].isin(['CL5', 'CL6', 'CL7', 'CL8', 'CL9'])]
        
        # Agrupamos para consolidar los datos
        agg_cl5_9 = df_cl5_9.groupby(['Call_Center_Apoyo', 'Nombre_Call_Center']).agg(
            META=pd.NamedAgg(column='Meta_$', aggfunc='sum'),
            Recaudo_Meta=pd.NamedAgg(column='Recaudo_Meta', aggfunc='sum')
        ).reset_index()

        # Renombramos las columnas para estandarizar
        agg_cl5_9.rename(columns={'Call_Center_Apoyo': 'CALL_CENTER', 'Nombre_Call_Center': 'NOMBRE'}, inplace=True)


        # --- Parte 3: Combinar y Calcular ---
        print("🧩 Combinando datos y realizando cálculos finales...")
        df_reporte = pd.concat([agg_cl1_4, agg_cl5_9], ignore_index=True)

        # Si no hay datos, retornamos un DataFrame vacío con la estructura correcta
        if df_reporte.empty:
            print("⚠️ No se encontraron datos para generar el reporte de Call Centers.")
            return pd.DataFrame(columns=[
                'CALL_CENTER', 'NOMBRE', 'META_$', 'Recaudo_Meta', 'Faltante', 'Cumplimiento_%'
            ])

        # Calcular 'Faltante'
        df_reporte['Faltante'] = df_reporte['META_$'] - df_reporte['Recaudo_Meta']

        # Calcular 'Cumplimiento_%' manejando división por cero
        cumplimiento_decimal = np.where(
            df_reporte['META_$'] > 0,
            df_reporte['Recaudo_Meta'] / df_reporte['META_$'],
            0
        )
        df_reporte['Cumplimiento_%'] = [f"{format(x * 100, '.2f')}%".replace('.', ',') for x in cumplimiento_decimal]

        
        # --- Parte 4: Finalizar y Ordenar ---
        print("✨ Formateando y ordenando el reporte final...")
        
        # Seleccionar y ordenar las columnas finales
        columnas_finales = [
            'CALL_CENTER', 'NOMBRE', 'META_$', 'Recaudo_Meta', 'Faltante', 'Cumplimiento_%'
        ]
        df_reporte = df_reporte[columnas_finales]

        # Ordenar por el nombre del Call Center
        df_reporte = df_reporte.sort_values(by='CALL_CENTER').reset_index(drop=True)

        print("✅ Reporte de Call Centers generado exitosamente.")
        return df_reporte