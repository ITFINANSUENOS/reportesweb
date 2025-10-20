import pandas as pd
import numpy as np

class CallCenterService:
    """
    Servicio para generar reportes de rendimiento específicos para Call Centers.
    """
    def generar_reporte_llamadas(self, rutas_call_center, config):
        """
        Carga los datos de llamadas y flujos desde los archivos de Call Center,
        los une por la extensión y genera un reporte detallado.

        Args:
            rutas_call_center (list): Lista de rutas a los archivos de Call Center.
            config (dict): El diccionario de configuración global.

        Returns:
            pd.DataFrame: Un DataFrame con los datos de llamadas enriquecidos.
        """
        print("🔄 Iniciando la generación del Reporte de Llamadas...")
        if not rutas_call_center:
            print("⚠️ No se proporcionaron archivos de Call Center.")
            return pd.DataFrame()

        config_cc = config.get("CALL_CENTER", {})
        if not config_cc or "sheets" not in config_cc:
            print("❌ Error: La configuración para 'CALL_CENTER' no es válida.")
            return pd.DataFrame()
            
        # Extraer la configuración para cada hoja
        config_llamadas = next((item for item in config_cc['sheets'] if item["sheet_name"] == "Llamadas_Call"), None)
        config_flujos = next((item for item in config_cc['sheets'] if item["sheet_name"] == "Flujos"), None)

        if not config_llamadas or not config_flujos:
            print("❌ Error: No se encontró la configuración para 'Llamadas_Call' o 'Flujos'.")
            return pd.DataFrame()

        lista_llamadas, lista_flujos = [], []

        try:
            for path in rutas_call_center:
                # Cargar la hoja de llamadas
                df_llamadas = pd.read_excel(
                    path,
                    sheet_name=config_llamadas["sheet_name"],
                    usecols=config_llamadas["usecols"]
                ).rename(columns=config_llamadas["rename_map"])
                lista_llamadas.append(df_llamadas)

                # Cargar la hoja de flujos
                df_flujos = pd.read_excel(
                    path,
                    sheet_name=config_flujos["sheet_name"],
                    usecols=config_flujos["usecols"]
                ).rename(columns=config_flujos["rename_map"])
                lista_flujos.append(df_flujos)

            df_llamadas_total = pd.concat(lista_llamadas, ignore_index=True)
            df_flujos_total = pd.concat(lista_flujos, ignore_index=True)
            
            print("🧹 Limpiando la columna 'Duracion_Llamada'...")
            if 'Duracion_Llamada' in df_llamadas_total.columns:
                # 1. Extraer solo los números del inicio de la cadena (ej: de "357s (5m 57s)" extrae "357")
                extracted_seconds = df_llamadas_total['Duracion_Llamada'].astype(str).str.extract(r'^(\d+)')
                # 2. Convertir la columna extraída a número, manejando posibles errores
                numeric_seconds = pd.to_numeric(extracted_seconds[0], errors='coerce').fillna(0)
                # 3. Asignar los valores limpios y convertidos a entero a la columna original
                df_llamadas_total['Duracion_Llamada'] = numeric_seconds.astype(int)
            
            # Asegurar que la columna de unión sea del mismo tipo y no tenga nulos
            df_llamadas_total['Extension_Llamada'] = df_llamadas_total['Extension_Llamada'].astype(str)
            df_flujos_total['Extension_Llamada'] = df_flujos_total['Extension_Llamada'].astype(str)
            
            # Eliminar duplicados en flujos para evitar filas duplicadas en el resultado
            df_flujos_total.drop_duplicates(subset=['Extension_Llamada'], inplace=True)

            print("🧩 Uniendo datos de llamadas y flujos...")
            df_reporte = pd.merge(
                df_llamadas_total,
                df_flujos_total,
                on='Extension_Llamada',
                how='left'
            )
            
            print("✅ Reporte de Llamadas generado exitosamente.")
            return df_reporte

        except Exception as e:
            print(f"❌ Ocurrió un error al procesar los archivos de Call Center: {e}")
            return pd.DataFrame()
    
    def _limpiar_y_preparar_datos(self, df):
        """
        Realiza una limpieza inicial de los datos necesarios para el reporte.
        """
        print("🧹 Limpiando y preparando datos para el reporte de Call Centers...")
        df_copy = df.copy()
        columnas_numericas = ['Meta_General', 'Meta_$', 'Recaudo_Meta']
        for col in columnas_numericas:
            if col in df_copy.columns:
                df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').fillna(0)
            else:
                df_copy[col] = 0
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
        agg_cl1_4 = df_cl1_4.groupby(['Zona', 'Cobrador']).agg(**{
            # --- CORRECCIÓN AQUÍ ---
            'META_$': pd.NamedAgg(column='Meta_General', aggfunc='sum'),
            'Recaudo_Meta': pd.NamedAgg(column='Recaudo_Meta', aggfunc='sum')
        }).reset_index()
        agg_cl1_4.rename(columns={'Zona': 'CALL_CENTER', 'Cobrador': 'NOMBRE'}, inplace=True)

        print("📊 Procesando Call Centers CL5 - CL9...")
        df_cl5_9 = df[df['Call_Center_Apoyo'].isin(['CL5', 'CL6', 'CL7', 'CL8', 'CL9'])]
        agg_cl5_9 = df_cl5_9.groupby(['Call_Center_Apoyo', 'Nombre_Call_Center']).agg(**{
            # --- CORRECCIÓN AQUÍ ---
            'META_$': pd.NamedAgg(column='Meta_$', aggfunc='sum'),
            'Recaudo_Meta': pd.NamedAgg(column='Recaudo_Meta', aggfunc='sum')
        }).reset_index()
        agg_cl5_9.rename(columns={'Call_Center_Apoyo': 'CALL_CENTER', 'Nombre_Call_Center': 'NOMBRE'}, inplace=True)
        
        print("🧩 Combinando datos y realizando cálculos finales...")
        df_reporte = pd.concat([agg_cl1_4, agg_cl5_9], ignore_index=True)

        if df_reporte.empty:
            print("⚠️ No se encontraron datos para generar el reporte de Call Centers.")
            return pd.DataFrame(columns=[
                'CALL_CENTER', 'NOMBRE', 'META_$', 'Recaudo_Meta', 'Faltante', 'Cumplimiento_%'
            ])
        # Calcular 'Faltante' (Ahora funcionará)
        df_reporte['Faltante'] = df_reporte['META_$'] - df_reporte['Recaudo_Meta']
        cumplimiento_decimal = np.where(
            df_reporte['META_$'] > 0,
            df_reporte['Recaudo_Meta'] / df_reporte['META_$'],
            0
        )
        df_reporte['Cumplimiento_%'] = [f"{format(x * 100, '.2f')}%".replace('.', ',') for x in cumplimiento_decimal]
        
        # --- Parte 4: Aplicar Formato de Moneda ---
        print("💰 Aplicando formato de moneda a las columnas financieras...")
        columnas_moneda = ['META_$', 'Recaudo_Meta', 'Faltante']
        for col in columnas_moneda:
            if col in df_reporte.columns:
                df_reporte[col] = df_reporte[col].apply(lambda x: f"$ {int(round(x, 0)):,}".replace(',', '.'))
        # --- Parte 5: Finalizar y Ordenar ---
        print("✨ Ordenando el reporte final...")
        columnas_finales = [
            'CALL_CENTER', 'NOMBRE', 'META_$', 'Recaudo_Meta', 'Faltante', 'Cumplimiento_%'
        ]
        df_reporte = df_reporte[columnas_finales]
        df_reporte = df_reporte.sort_values(by='CALL_CENTER').reset_index(drop=True)
        print("✅ Reporte de Call Centers generado exitosamente.")
        return df_reporte
    
    
    