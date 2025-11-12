import pandas as pd
import numpy as np

class CallCenterService:
    """
    Servicio para generar reportes de rendimiento, llamadas y mensajería
    específicos para Call Centers.
    """

    def _cargar_call_center_sheets(self, rutas_call_center, config):
        """
        Método auxiliar para cargar todas las hojas de los archivos de Call Center.
        """
        config_cc = config.get("CALL_CENTER", {})
        if not config_cc or "sheets" not in config_cc:
            print("❌ Error: La configuración para 'CALL_CENTER' no es válida.")
            return None, None, None

        # Encontrar configuraciones
        config_llamadas = next((item for item in config_cc['sheets'] if item["sheet_name"] == "Llamadas_Call"), None)
        config_flujos = next((item for item in config_cc['sheets'] if item["sheet_name"] == "Flujos"), None)
        config_mensajes = next((item for item in config_cc['sheets'] if item["sheet_name"] == "Mensajeria_Call"), None)

        lista_llamadas, lista_flujos, lista_mensajes = [], [], []

        for path in rutas_call_center:
            try:
                # Cargar hoja de llamadas (si existe config)
                if config_llamadas:
                    df_llamadas = pd.read_excel(
                        path,
                        sheet_name=config_llamadas["sheet_name"],
                        usecols=config_llamadas["usecols"]
                    ).rename(columns=config_llamadas["rename_map"])
                    lista_llamadas.append(df_llamadas)

                # Cargar hoja de flujos (si existe config)
                if config_flujos:
                    df_flujos = pd.read_excel(
                        path,
                        sheet_name=config_flujos["sheet_name"],
                        usecols=config_flujos["usecols"]
                    ).rename(columns=config_flujos["rename_map"])
                    lista_flujos.append(df_flujos)
                
                # Cargar hoja de mensajería (si existe config)
                if config_mensajes:
                    df_mensajes = pd.read_excel(
                        path,
                        sheet_name=config_mensajes["sheet_name"],
                        usecols=config_mensajes["usecols"]
                    ).rename(columns=config_mensajes["rename_map"])
                    lista_mensajes.append(df_mensajes)

            except Exception as e:
                print(f"⚠️ Error leyendo el archivo {path}. Hoja no encontrada o error: {e}")
                # Continuamos por si otros archivos sí son correctos
                pass

        # Concatenar los DataFrames
        df_llamadas_total = pd.concat(lista_llamadas, ignore_index=True) if lista_llamadas else pd.DataFrame()
        df_flujos_total = pd.concat(lista_flujos, ignore_index=True) if lista_flujos else pd.DataFrame()
        df_mensajes_total = pd.concat(lista_mensajes, ignore_index=True) if lista_mensajes else pd.DataFrame()
        
        return df_llamadas_total, df_flujos_total, df_mensajes_total

    def generar_reporte_llamadas(self, rutas_call_center, config):
        """
        Carga los datos de llamadas y flujos, los une por extensión
        y genera un reporte detallado de llamadas.
        """
        print("🔄 Iniciando la generación del Reporte de Llamadas...")
        if not rutas_call_center:
            print("⚠️ No se proporcionaron archivos de Call Center.")
            return pd.DataFrame()

        try:
            # Usamos el método auxiliar para cargar los datos
            df_llamadas_total, df_flujos_total, _ = self._cargar_call_center_sheets(rutas_call_center, config)

            if df_llamadas_total.empty or df_flujos_total.empty:
                print("⚠️ No se pudieron cargar datos de 'Llamadas_Call' o 'Flujos'. Abortando Reporte de Llamadas.")
                return pd.DataFrame()
            
            # --- Lógica de limpieza de Duracion_Llamada (tu código anterior) ---
            print("🧹 Limpiando la columna 'Duracion_Llamada'...")
            if 'Duracion_Llamada' in df_llamadas_total.columns:
                print("💾 Preservando formato original de 'Duracion_Llamada'...")
                df_llamadas_total['Duracion_Original_Str'] = df_llamadas_total['Duracion_Llamada'].astype(str)
                
                extracted_seconds = df_llamadas_total['Duracion_Llamada'].astype(str).str.extract(r'^(\d+)')
                numeric_seconds = pd.to_numeric(extracted_seconds[0], errors='coerce').fillna(0)
                df_llamadas_total['Duracion_Llamada'] = numeric_seconds.astype(int)
                
                print("🔄 Aplicando lógica de negocio a 'Estado_Llamada' (>= 30s)...")
                if 'Estado_Llamada' in df_llamadas_total.columns:
                    df_llamadas_total['Estado_Llamada'] = np.where(
                        (df_llamadas_total['Estado_Llamada'] == 'ANSWERED') & (df_llamadas_total['Duracion_Llamada'] < 30),
                        'FAILED',
                        df_llamadas_total['Estado_Llamada']
                    )
                else:
                    print("⚠️ Advertencia: No se encontró 'Estado_Llamada' para aplicar la lógica de < 30s.")
            else:
                 print("⚠️ Advertencia: No se encontró la columna 'Duracion_Llamada'.")
            
            # --- Lógica de unión para Reporte de Llamadas ---
            df_llamadas_total['Extension_Llamada'] = df_llamadas_total['Extension_Llamada'].astype(str)
            df_flujos_total['Extension_Llamada'] = df_flujos_total['Extension_Llamada'].astype(str)
            
            # Preparamos flujos para este merge (solo por Extensión)
            df_flujos_para_llamadas = df_flujos_total.drop_duplicates(subset=['Extension_Llamada'])

            print("🧩 Uniendo datos de llamadas y flujos por 'Extension_Llamada'...")
            df_reporte = pd.merge(
                df_llamadas_total,
                df_flujos_para_llamadas,
                on='Extension_Llamada',
                how='left'
            )
            
            # Restaurar formato original de Duración
            if 'Duracion_Original_Str' in df_reporte.columns:
                print("✨ Restaurando formato string original de 'Duracion_Llamada'...")
                df_reporte['Duracion_Llamada'] = df_reporte['Duracion_Original_Str']
                df_reporte.drop(columns=['Duracion_Original_Str'], inplace=True)

            # Eliminamos Flujo_Truora si existe, ya que este reporte se une por Extension
            if 'Flujo_Truora' in df_reporte.columns:
                print("🗑️ Eliminando la columna 'Flujo_Truora' del reporte final...")
                df_reporte.drop(columns=['Flujo_Truora'], inplace=True)
            
            print("✅ Reporte de Llamadas generado exitosamente.")
            return df_reporte

        except Exception as e:
            print(f"❌ Ocurrió un error al generar Reporte de Llamadas: {e}")
            return pd.DataFrame()

    def generar_reporte_mensajes(self, rutas_call_center, config):
        """
        Carga los datos de mensajería y flujos, los une por Flujo_Truora
        y genera un reporte detallado de mensajes.
        """
        print("🔄 Iniciando la generación del Reporte de Mensajes...")
        if not rutas_call_center:
            print("⚠️ No se proporcionaron archivos de Call Center.")
            return pd.DataFrame()
            
        try:
            # Usamos el método auxiliar para cargar los datos
            _, df_flujos_total, df_mensajes_total = self._cargar_call_center_sheets(rutas_call_center, config)

            if df_mensajes_total.empty or df_flujos_total.empty:
                print("⚠️ No se pudieron cargar datos de 'Mensajeria_Call' o 'Flujos'. Abortando Reporte de Mensajes.")
                return pd.DataFrame()

            # --- Lógica de unión para Reporte de Mensajes ---
            print("🧹 Preparando datos para el cruce por 'Flujo_Truora'...")
            
            # Preparar DF de Mensajes
            df_mensajes_total['Flujo_Truora'] = df_mensajes_total['Flujo_Truora'].astype(str)

            # Preparar DF de Flujos:
            # 1. Seleccionar solo columnas necesarias (evitar 'Extension_Llamada')
            columnas_flujo = ['Flujo_Truora', 'Call_Center', 'Nombre_Call']
            df_flujos_para_mensajes = df_flujos_total[columnas_flujo].copy()
            
            # 2. Convertir llave a string
            df_flujos_para_mensajes['Flujo_Truora'] = df_flujos_para_mensajes['Flujo_Truora'].astype(str)
            
            # 3. Eliminar duplicados en base a la llave
            df_flujos_para_mensajes.drop_duplicates(subset=['Flujo_Truora'], inplace=True)

            print("🧩 Uniendo datos de mensajes y flujos por 'Flujo_Truora'...")
            df_reporte = pd.merge(
                df_mensajes_total,
                df_flujos_para_mensajes,
                on='Flujo_Truora',
                how='left'
            )
            
            print("✅ Reporte de Mensajes generado exitosamente.")
            return df_reporte

        except Exception as e:
            print(f"❌ Ocurrió un error al generar Reporte de Mensajes: {e}")
            return pd.DataFrame()

    def _limpiar_y_preparar_datos(self, df):
        """
        Realiza una limpieza inicial de los datos necesarios para el reporte.
        """
        # ... (Este método no cambia) ...
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
                df_copy[col] = ''          
        return df_copy


    def generar_reporte_call_center(self, df_analisis_cartera):
        """
        Genera un reporte consolidado del rendimiento de los Call Centers.
        """
        # ... (Este método no cambia) ...
        print("🔄 Iniciando la generación del reporte de Call Centers...")
        df = self._limpiar_y_preparar_datos(df_analisis_cartera)

        print("📊 Procesando Call Centers CL1 - CL4...")
        df_cl1_4 = df[df['Zona'].isin(['CL1', 'CL2', 'CL3', 'CL4']) & (df['Franja_Meta'] == 'AL DIA')]
        
        agg_cl1_4 = df_cl1_4.groupby(['Zona', 'Cobrador']).agg(**{
            'META_$': pd.NamedAgg(column='Meta_General', aggfunc='sum'),
            'Recaudo_Meta': pd.NamedAgg(column='Recaudo_Meta', aggfunc='sum')
        }).reset_index()
        agg_cl1_4.rename(columns={'Zona': 'CALL_CENTER', 'Cobrador': 'NOMBRE'}, inplace=True)

        print("📊 Procesando Call Centers CL5 - CL9...")
        df_cl5_9 = df[df['Call_Center_Apoyo'].isin(['CL5', 'CL6', 'CL7', 'CL8', 'CL9'])]
        agg_cl5_9 = df_cl5_9.groupby(['Call_Center_Apoyo', 'Nombre_Call_Center']).agg(**{
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
        df_reporte['Faltante'] = df_reporte['META_$'] - df_reporte['Recaudo_Meta']
        cumplimiento_decimal = np.where(
            df_reporte['META_$'] > 0,
            df_reporte['Recaudo_Meta'] / df_reporte['META_$'],
            0
        )
        df_reporte['Cumplimiento_%'] = [f"{format(x * 100, '.2f')}%".replace('.', ',') for x in cumplimiento_decimal]
        
        print("💰 Aplicando formato de moneda a las columnas financieras...")
        columnas_moneda = ['META_$', 'Recaudo_Meta', 'Faltante']
        for col in columnas_moneda:
            if col in df_reporte.columns:
                df_reporte[col] = df_reporte[col].apply(lambda x: f"$ {int(round(x, 0)):,}".replace(',', '.'))
        print("✨ Ordenando el reporte final...")
        columnas_finales = [
            'CALL_CENTER', 'NOMBRE', 'META_$', 'Recaudo_Meta', 'Faltante', 'Cumplimiento_%'
        ]
        df_reporte = df_reporte[columnas_finales]
        df_reporte = df_reporte.sort_values(by='CALL_CENTER').reset_index(drop=True)
        print("✅ Reporte de Call Centers generado exitosamente.")
        return df_reporte