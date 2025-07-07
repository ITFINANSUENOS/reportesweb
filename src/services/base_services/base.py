import pandas as pd
import numpy as np

class ReportService:
    """
    Servicio encargado de toda la lógica de negocio para procesar y consolidar
    los reportes de cartera.
    """
    def __init__(self, config):
        self.config = config

    def _load_dataframes(self, file_paths):
        """Lee todos los archivos de Excel y los convierte en DataFrames según la configuración."""
        dataframes_por_tipo = {key: [] for key in self.config.keys()}
        print("--- 🔄 Iniciando lectura de archivos ---")
        for ruta_archivo in file_paths:
            try:
                nombre_archivo = ruta_archivo.split('/')[-1]
                tipo_archivo_actual = self._get_file_type(nombre_archivo)
                
                if not tipo_archivo_actual:
                    print(f"⚠️  Archivo '{nombre_archivo}' omitido.")
                    continue

                config_actual = self.config[tipo_archivo_actual]
                
                if "sheets" in config_actual:
                    # Lógica para archivos con múltiples hojas
                    xls = pd.ExcelFile(ruta_archivo, engine='openpyxl')
                    for sheet_config in config_actual["sheets"]:
                        if sheet_config["sheet_name"] in xls.sheet_names:
                            df_hoja = pd.read_excel(xls, sheet_name=sheet_config["sheet_name"])
                            df_hoja.columns = df_hoja.columns.str.strip()
                            columnas_a_usar = [col for col in sheet_config["usecols"] if col in df_hoja.columns]
                            df_filtrado = df_hoja[columnas_a_usar].rename(columns=sheet_config["rename_map"])
                            dataframes_por_tipo[tipo_archivo_actual].append({"data": df_filtrado, "config": sheet_config})
                elif "new_names" in config_actual:
                    # Lógica para archivos con formato especial (ej. MATRIZ_CARTERA)
                    df = pd.read_excel(ruta_archivo, header=config_actual.get("header"), skiprows=config_actual.get("skiprows"), names=config_actual.get("new_names"))
                    dataframes_por_tipo[tipo_archivo_actual].append(df)
                else:
                    # Lógica para archivos estándar
                    engine = 'xlrd' if ruta_archivo.upper().endswith('.XLS') else 'openpyxl'
                    df = pd.read_excel(ruta_archivo, engine=engine)
                    df.columns = df.columns.str.strip()
                    columnas_a_usar = [col for col in config_actual["usecols"] if col in df.columns]
                    df_filtrado = df[columnas_a_usar]
                    if tipo_archivo_actual == "R03":
                        df_filtrado = df_filtrado.replace('.', 'SIN CODEUDOR').fillna('SIN CODEUDOR')
                    df_renombrado = df_filtrado.rename(columns=config_actual["rename_map"])
                    dataframes_por_tipo[tipo_archivo_actual].append(df_renombrado)
                
                print(f"✅ Archivo '{nombre_archivo}' procesado como tipo '{tipo_archivo_actual}'.")
            except Exception as e:
                print(f"❌ Error procesando el archivo '{ruta_archivo}': {e}")
        
        return dataframes_por_tipo

    def _get_file_type(self, filename):
        """Determina el tipo de archivo basado en su nombre y las claves de configuración."""
        nombre_base = filename.split('.')[0].upper().replace(" ", "_")
        palabras_en_nombre = set(nombre_base.split('_'))
        for tipo in self.config.keys():
            palabras_en_clave = set(tipo.split('_'))
            if palabras_en_clave.issubset(palabras_en_nombre):
                return tipo
        return None

    def _create_credit_key(self, df):
        """Crea una llave única 'Credito' combinando Tipo y Número de crédito."""
        if 'Numero_Credito' in df.columns and 'Tipo_Credito' in df.columns:
            df['Numero_Credito'] = pd.to_numeric(df['Numero_Credito'], errors='coerce').astype('Int64')
            df['Credito'] = df['Tipo_Credito'].astype(str) + '-' + df['Numero_Credito'].astype(str).str.replace('<NA>', '', regex=False)
        return df
    
     # --- NUEVO MÉTODO PARA ENRIQUECER DETALLES DEL CRÉDITO ---
    def _enrich_credit_details(self, reporte_df, sc04_df, desembolsos_df):
        """
        Puebla las columnas Total_Cuotas, Valor_Cuota y Valor_Desembolso
        usando 'Factura_Venta' para SC04 y 'Credito' para Desembolsos.
        """
        print("✨ Enriqueciendo detalles de cuotas y desembolsos...")

        if sc04_df.empty and desembolsos_df.empty:
            print("⚠️ No se encontraron archivos SC04 ni de Desembolsos. Se omiten los detalles del crédito.")
            return reporte_df

        # --- Preparar datos de ARPESOD (SC04) ---
        if not sc04_df.empty:
            
            # --- LÓGICA CLAVE: Transformar la llave 'Factura_Venta' en SC04 ---
            def transformar_factura(valor):
                if isinstance(valor, str):
                    partes = valor.split(',')
                    if len(partes) >= 2:
                        return f"{partes[-2]}-{partes[-1]}"
                return None

            sc04_df['Factura_Venta'] = sc04_df['Factura_Venta'].apply(transformar_factura)
            sc04_df.dropna(subset=['Factura_Venta'], inplace=True) # Eliminar filas donde no se pudo transformar la factura

            sc04_df.drop_duplicates(subset='Factura_Venta', keep='last', inplace=True)

            sc04_df['Valor_Cuota'] = pd.to_numeric(sc04_df['Valor_Cuota'], errors='coerce')
            sc04_df['Total_Cuotas'] = pd.to_numeric(sc04_df['Total_Cuotas'], errors='coerce')
            sc04_df['Valor_Desembolso'] = sc04_df['Valor_Cuota'] * sc04_df['Total_Cuotas']

            mapa_cuotas_arp = sc04_df.set_index('Factura_Venta')['Total_Cuotas']
            mapa_valor_arp = sc04_df.set_index('Factura_Venta')['Valor_Cuota']
            mapa_desembolso_arp = sc04_df.set_index('Factura_Venta')['Valor_Desembolso']

            mask_arp = reporte_df['Empresa'] == 'ARPESOD'
            # Usamos la columna 'Factura_Venta' del reporte final para mapear
            reporte_df.loc[mask_arp, 'Total_Cuotas'] = reporte_df.loc[mask_arp, 'Factura_Venta'].map(mapa_cuotas_arp)
            reporte_df.loc[mask_arp, 'Valor_Cuota'] = reporte_df.loc[mask_arp, 'Factura_Venta'].map(mapa_valor_arp)
            reporte_df.loc[mask_arp, 'Valor_Desembolso'] = reporte_df.loc[mask_arp, 'Factura_Venta'].map(mapa_desembolso_arp)
        
        # --- Preparar datos de FINANSUEÑOS (Desembolsos) ---
        if not desembolsos_df.empty:
            desembolsos_df.drop_duplicates(subset='Credito', keep='last', inplace=True)

            mapa_cuotas_fns = desembolsos_df.set_index('Credito')['Total_Cuotas']
            mapa_valor_fns = desembolsos_df.set_index('Credito')['Valor_Cuota']
            mapa_desembolso_fns = desembolsos_df.set_index('Credito')['Valor_Desembolso']

            mask_fns = reporte_df['Empresa'] == 'FINANSUEÑOS'
            reporte_df.loc[mask_fns, 'Total_Cuotas'] = reporte_df.loc[mask_fns, 'Credito'].map(mapa_cuotas_fns)
            reporte_df.loc[mask_fns, 'Valor_Cuota'] = reporte_df.loc[mask_fns, 'Credito'].map(mapa_valor_fns)
            reporte_df.loc[mask_fns, 'Valor_Desembolso'] = reporte_df.loc[mask_fns, 'Credito'].map(mapa_desembolso_fns)

        return reporte_df
    
    # --- NUEVO MÉTODO PARA PROCESAR FECHAS DE VENCIMIENTOS ---
    def _process_vencimientos_data(self, vencimientos_df):
        """
        Filtra el dataframe de vencimientos para mantener una única entrada por 'Credito'.
        Prioriza las fechas dentro del mes y año en curso. Si no existen,
        selecciona la fecha más reciente disponible para ese crédito.
        """
        print("⚙️  Procesando fechas de VENCIMIENTOS con lógica de prioridad...")

        # Si el dataframe está vacío, no hay nada que hacer
        if vencimientos_df.empty:
            return pd.DataFrame()

        # 1. Crear una copia y asegurar que los datos clave existen y tienen el formato correcto
        df = vencimientos_df.copy()
        df['Fecha_Cuota_Vigente'] = pd.to_datetime(df['Fecha_Cuota_Vigente'], errors='coerce')
        df.dropna(subset=['Credito', 'Fecha_Cuota_Vigente'], inplace=True)

        # 2. Obtener el mes y año actual
        today = pd.Timestamp.now()
        current_month = today.month
        current_year = today.year

        # 3. Prioridad 1: Filtrar créditos con fechas en el mes y año actual
        df_current_month = df[
            (df['Fecha_Cuota_Vigente'].dt.month == current_month) &
            (df['Fecha_Cuota_Vigente'].dt.year == current_year)
        ]
        
        # De las fechas del mes actual, nos quedamos con la más reciente por cada crédito
        df_current_month = df_current_month.sort_values('Fecha_Cuota_Vigente', ascending=False)
        result_current_month = df_current_month.drop_duplicates(subset='Credito', keep='first')

        # 4. Prioridad 2: Procesar los créditos que NO se encontraron en el paso anterior
        credits_found = result_current_month['Credito'].unique()
        df_fallback = df[~df['Credito'].isin(credits_found)]
        
        # De los créditos restantes, nos quedamos con la fecha más reciente (la última)
        df_fallback = df_fallback.sort_values('Fecha_Cuota_Vigente', ascending=False)
        result_fallback = df_fallback.drop_duplicates(subset='Credito', keep='first')

        # 5. Combinar los dos resultados para obtener el dataframe final
        final_vencimientos_df = pd.concat([result_current_month, result_fallback], ignore_index=True)

        print(f"✅ Fechas de VENCIMIENTOS procesadas. Se seleccionaron {len(final_vencimientos_df)} registros únicos.")
        
        return final_vencimientos_df
    
    # --- NUEVO MÉTODO PARA CORREGIR VALORES DE CUOTAS ---
    def _clean_installment_data(self, reporte_df):
        """
        Corrige valores erróneos en las columnas de cuotas.
        Si un valor es > 100, se asume que es un error y se toman los 2 últimos dígitos.
        """
        print("🧼 Limpiando datos de cuotas...")
        columnas_a_limpiar = ['Cuotas_Pagadas', 'Cuota_Vigente']
        
        for col in columnas_a_limpiar:
            if col in reporte_df.columns:
                # Asegurar que la columna sea numérica para poder comparar
                reporte_df[col] = pd.to_numeric(reporte_df[col], errors='coerce')
                
                # Definir la condición: valores mayores a 100 y que no sean nulos
                mask = (reporte_df[col] > 100) & (reporte_df[col].notna())
                
                # Aplicar la corrección usando el módulo 100
                reporte_df.loc[mask, col] = reporte_df.loc[mask, col] % 100
        
        return reporte_df
    
     # --- NUEVO MÉTODO PARA ASIGNAR FACTURA DE VENTA ---
    def _assign_sales_invoice(self, reporte_df, crtmp_df):
        """
        Crea la columna 'Factura_Venta' asignando el valor según la empresa.
        Para FINANSUEÑOS, busca la factura correspondiente en el archivo CRTMPCONSULTA1.
        """
        print("🧾 Asignando facturas de venta...")
        
        # Si no hay datos de donde buscar, no se puede continuar
        if crtmp_df.empty:
            print("⚠️ Archivo CRTMPCONSULTA1 no encontrado o vacío. No se pueden asignar facturas para FINANSUEÑOS.")
            reporte_df['Factura_Venta'] = np.where(reporte_df['Empresa'] == 'ARPESOD', reporte_df['Credito'], 'NO DISPONIBLE')
            return reporte_df

        # Lógica para ARPESOD (simple)
        reporte_df['Factura_Venta'] = np.nan
        reporte_df.loc[reporte_df['Empresa'] == 'ARPESOD', 'Factura_Venta'] = reporte_df['Credito']

        # --- Lógica avanzada para FINANSUEÑOS ---
        
        # 1. Preparar el DataFrame de búsqueda (crtmp_df)
        crtmp_df_copy = crtmp_df.copy() # Hacemos una copia para no modificar el original
        crtmp_df_copy['Fecha_Facturada'] = pd.to_datetime(crtmp_df_copy['Fecha_Facturada'], dayfirst=True, errors='coerce')
        
        # Si después de la conversión todas las fechas son nulas, detenemos.
        if crtmp_df_copy['Fecha_Facturada'].isnull().all():
            print("❌ Error crítico: No se pudo interpretar ninguna fecha en CRTMPCONSULTA1. Verifique el formato.")
            reporte_df['Factura_Venta'].fillna('ERROR DE FECHA', inplace=True)
            return reporte_df

        # 2. Separar créditos de FINANSUEÑOS y facturas de venta
        creditos_fns = crtmp_df_copy[crtmp_df_copy['Credito'].str.startswith('DF', na=False)].copy()
        facturas_fns = crtmp_df_copy[~crtmp_df_copy['Credito'].str.startswith('DF', na=False)].copy()

        # 3. Cruzar créditos y facturas por 'Cedula_Cliente'
        merged_df = pd.merge(
            creditos_fns,
            facturas_fns,
            on='Cedula_Cliente',
            suffixes=('_credito', '_factura')
        )
        
        # 4. Filtrar por la condición de fecha (diferencia de <= 30 días)
        merged_df['dias_diferencia'] = (merged_df['Fecha_Facturada_factura'] - merged_df['Fecha_Facturada_credito']).dt.days.abs()
        coincidencias_validas = merged_df[merged_df['dias_diferencia'] <= 30].copy()

        # 5. En caso de múltiples coincidencias, elegir la más cercana en tiempo
        coincidencias_validas.sort_values(by=['Credito_credito', 'dias_diferencia'], inplace=True)
        mapeo_final = coincidencias_validas.drop_duplicates(subset='Credito_credito', keep='first')
        
        # 6. Crear un mapa (diccionario) para la asignación: {Credito: Factura}
        mapa_facturas = pd.Series(mapeo_final['Credito_factura'].values, index=mapeo_final['Credito_credito']).to_dict()

        # 7. Asignar los valores al reporte final usando el mapa
        filtro_fns = reporte_df['Empresa'] == 'FINANSUEÑOS'
        reporte_df.loc[filtro_fns, 'Factura_Venta'] = reporte_df.loc[filtro_fns, 'Credito'].map(mapa_facturas)

        # 8. Rellenar los valores no encontrados
        reporte_df['Factura_Venta'].fillna('NO ASIGNADA', inplace=True)

        return reporte_df
    
     # --- NUEVO MÉTODO PARA TRANSFORMAR DATOS DE LA MATRIZ ---
    def _map_call_center_data(self, reporte_df):
        """
        Crea las columnas 'Franja_Mora' y de Call Center consolidadas,
        basándose en los días de atraso.
        """
        print("Mappings de datos de Call Center...")

        # 1. Asegurarse que 'Dias_Atraso' sea numérico para poder comparar
        if 'Dias_Atraso' not in reporte_df.columns:
            print("⚠️ Columna 'Dias_Atraso' no encontrada. No se puede mapear la franja de mora.")
            return reporte_df
            
        reporte_df['Dias_Atraso'] = pd.to_numeric(reporte_df['Dias_Atraso'], errors='coerce')

        # 2. Definir condiciones y valores para 'Franja_Mora'
        condiciones_mora = [
            reporte_df['Dias_Atraso'] == 0,
            reporte_df['Dias_Atraso'].between(1, 30),
            reporte_df['Dias_Atraso'].between(31, 90),
            reporte_df['Dias_Atraso'] > 90
        ]
        valores_mora = ['AL DIA', '1 A 30 DIAS', '31 A 90 DIAS', '91 A 360 DIAS']
        reporte_df['Franja_Mora'] = np.select(condiciones_mora, valores_mora, default='SIN INFO')

        # 3. Mapear datos del Call Center según la 'Franja_Mora'
        # Se definen las columnas de origen para cada franja
        mapa_columnas = {
            '1 A 30 DIAS': ('call_center_1_30_dias', 'call_center_nombre_1_30', 'call_center_telefono_1_30'),
            '31 A 90 DIAS': ('call_center_31_90_dias', 'call_center_nombre_31_90', 'call_center_telefono_31_90'),
            '91 A 360 DIAS': ('call_center_91_360_dias', 'call_center_nombre_91_360', 'call_center_telefono_91_360')
        }

        # Se crean las nuevas columnas vacías
        reporte_df['Call_Center_Apoyo'] = np.nan
        reporte_df['Nombre_Call_Center'] = np.nan
        reporte_df['Telefono_Call_Center'] = np.nan

        # Se llenan las nuevas columnas iterando sobre el mapa
        for franja, cols in mapa_columnas.items():
            # cols[0] = apoyo, cols[1] = nombre, cols[2] = telefono
            # Solo se actualizan las filas que coinciden con la franja actual
            mask = reporte_df['Franja_Mora'] == franja
            if cols[0] in reporte_df.columns:
                reporte_df.loc[mask, 'Call_Center_Apoyo'] = reporte_df.loc[mask, cols[0]]
            if cols[1] in reporte_df.columns:
                reporte_df.loc[mask, 'Nombre_Call_Center'] = reporte_df.loc[mask, cols[1]]
            if cols[2] in reporte_df.columns:
                 reporte_df.loc[mask, 'Telefono_Call_Center'] = reporte_df.loc[mask, cols[2]]

        # 4. Eliminar las columnas originales de la matriz para limpiar el reporte
        cols_a_borrar_matriz = [
            'call_center_1_30_dias', 'call_center_nombre_1_30', 'call_center_telefono_1_30', 
            'call_center_31_90_dias', 'call_center_nombre_31_90', 'call_center_telefono_31_90', 
            'call_center_91_360_dias', 'call_center_nombre_91_360', 'call_center_telefono_91_360'
        ]
        # Nos aseguramos de no borrar 'Zona' que es la clave de cruce
        columnas_existentes_a_borrar = [col for col in cols_a_borrar_matriz if col in reporte_df.columns]
        reporte_df.drop(columns=columnas_existentes_a_borrar, inplace=True, errors='ignore')
        
        return reporte_df


    def _calculate_balances(self, reporte_df, fnz003_df):
        """Calcula los diferentes saldos y los agrega al reporte."""
        print("📊 Calculando saldos...")
        reporte_df['Saldo_Capital'] = np.where(reporte_df['Empresa'] == 'ARPESOD', reporte_df.get('Saldo_Factura'), np.nan)
        if not fnz003_df.empty:
            fnz003_df = self._create_credit_key(fnz003_df)
            fnz003_df['Credito'] = fnz003_df['Credito'].astype(str)
            mapa_capital = fnz003_df[fnz003_df['Concepto'].isin(['CAPITAL', 'ABONO DIF TASA'])].groupby('Credito')['Saldo'].sum()
            mapa_avales = fnz003_df[fnz003_df['Concepto'] == 'AVAL'].groupby('Credito')['Saldo'].sum()
            mapa_interes = fnz003_df[fnz003_df['Concepto'] == 'INTERES CORRIENTE'].groupby('Credito')['Saldo'].sum()
            
            reporte_df['Saldo_Capital'] = reporte_df['Credito'].map(mapa_capital).combine_first(reporte_df['Saldo_Capital'])
            reporte_df['Saldo_Avales'] = reporte_df['Credito'].map(mapa_avales)
            reporte_df['Saldo_Interes_Corriente'] = reporte_df['Credito'].map(mapa_interes)
        
        reporte_df['Saldo_Capital'] = pd.to_numeric(reporte_df['Saldo_Capital'], errors='coerce').fillna(0).astype(int)
        reporte_df['Saldo_Avales'] = np.where(reporte_df['Empresa'] == 'FINANSUEÑOS', reporte_df.get('Saldo_Avales').fillna(0).astype(int), 'NO APLICA')
        reporte_df['Saldo_Interes_Corriente'] = np.where(reporte_df['Empresa'] == 'FINANSUEÑOS', reporte_df.get('Saldo_Interes_Corriente').fillna(0).astype(int), 'NO APLICA')
        return reporte_df
    
    # --- NUEVO MÉTODO PARA CALCULAR MÉTRICAS DE METAS ---
    def _calculate_goal_metrics(self, reporte_df, metas_franjas_df):
        """
        Calcula las diferentes métricas de metas, interpretando correctamente
        los porcentajes y limpiando las columnas intermedias al final.
        """
        print("🎯 Calculando métricas de metas...")

        # 1. Calcular Meta_General (sin cambios)
        for col in ['Meta_DC_Al_Dia', 'Meta_DC_Atraso', 'Meta_Atraso']:
            if col in reporte_df.columns:
                reporte_df[col] = pd.to_numeric(reporte_df[col], errors='coerce').fillna(0)
        reporte_df['Meta_General'] = reporte_df['Meta_DC_Al_Dia'] + reporte_df['Meta_DC_Atraso'] + reporte_df['Meta_Atraso']

        if metas_franjas_df.empty:
            print("⚠️ Archivo de Metas por Franja no encontrado. Se omiten los cálculos de metas % y $.")
            return reporte_df

        # 2. Unir metas por franja al reporte principal por 'Zona'
        metas_franjas_df['Zona'] = metas_franjas_df['Zona'].astype(str).str.strip()
        reporte_df['Zona'] = reporte_df['Zona'].astype(str).str.strip()
        # Guardamos los nombres de las columnas que vamos a borrar al final
        columnas_metas_a_borrar = [col for col in metas_franjas_df.columns if col != 'Zona']
        reporte_df = pd.merge(reporte_df, metas_franjas_df, on='Zona', how='left')

        # --- NUEVO: 3. Convertir las columnas de porcentaje a números ---
        print("   - Convirtiendo porcentajes a números...")
        columnas_porcentaje = ['Meta_1_A_30', 'Meta_31_A_90', 'Meta_91_A_180', 'Meta_181_A_360', 'Total_Recaudo']
        for col in columnas_porcentaje:
            if col in reporte_df.columns:
                reporte_df[col] = reporte_df[col].astype(str) \
                                                 .str.replace(',', '.', regex=False) \
                                                 .str.strip('%')
                reporte_df[col] = pd.to_numeric(reporte_df[col], errors='coerce') / 100
                reporte_df[col] = reporte_df[col].fillna(0)


        # 4. Calcular 'Meta_%' dinámicamente
        dias_atraso = reporte_df['Dias_Atraso']
        condiciones = [
            dias_atraso.between(1, 30),
            dias_atraso.between(31, 90),
            dias_atraso.between(91, 180),
            dias_atraso.between(181, 360)
        ]
        valores = [
            reporte_df['Meta_1_A_30'],
            reporte_df['Meta_31_A_90'],
            reporte_df['Meta_91_A_180'],
            reporte_df['Meta_181_A_360']
        ]
        reporte_df['Meta_%'] = np.select(condiciones, valores, default=0)

        # 5. Calcular 'Meta_$'
        reporte_df['Meta_$'] = reporte_df['Meta_General'] * reporte_df['Meta_%']
        
        # 6. Calcular 'Meta_T.R_%' y 'Meta_T.R_$'
        reporte_df['Meta_T.R_%'] = reporte_df['Total_Recaudo']

        saldo_capital_num = pd.to_numeric(reporte_df['Saldo_Capital'], errors='coerce').fillna(0)
        saldo_avales_num = pd.to_numeric(reporte_df['Saldo_Avales'], errors='coerce').fillna(0)
        saldo_interes_num = pd.to_numeric(reporte_df['Saldo_Interes_Corriente'], errors='coerce').fillna(0)
        
        total_saldo_fns = saldo_capital_num + saldo_avales_num + saldo_interes_num
        
        reporte_df['Meta_T.R_$'] = np.where(
            reporte_df['Empresa'] == 'FINANSUEÑOS', 
            total_saldo_fns, 
            saldo_capital_num
        ) * reporte_df['Meta_T.R_%']

        # --- NUEVO: 7. Eliminar las columnas intermedias de metas ---
        print("   - Limpiando columnas de metas intermedias...")
        reporte_df.drop(columns=columnas_metas_a_borrar, inplace=True, errors='ignore')

        return reporte_df

    def _finalize_report(self, reporte_df, orden_columnas):
        """Realiza la limpieza, formato y reordenamiento final del reporte."""
        print("🧹 Realizando transformaciones y limpieza final...")
        cols_a_borrar = [col for col in reporte_df.columns if any(sufijo in col for sufijo in ['_Venc', '_R03', '_Analisis'])]
        reporte_df = reporte_df.drop(columns=cols_a_borrar, errors='ignore')
        
        print("📅 Formateando fechas a DD/MM/YY...")
        for col_fecha in ['Fecha_Vencimiento', 'Fecha_Facturada']:
            if col_fecha in reporte_df.columns:
                # Se asegura que la columna de fecha sea tipo texto antes de aplicar formato
                if pd.api.types.is_datetime64_any_dtype(reporte_df[col_fecha]):
                     reporte_df[col_fecha] = reporte_df[col_fecha].dt.strftime('%d/%m/%y')
                else:
                     reporte_df[col_fecha] = pd.to_datetime(reporte_df[col_fecha], errors='coerce').dt.strftime('%d/%m/%y')
                reporte_df[col_fecha] = reporte_df[col_fecha].fillna('')


        # --- NUEVA SECCIÓN: Formatear columnas de porcentaje para presentación ---
        print("✨ Formateando columnas de porcentaje...")
        columnas_porcentaje_a_formatear = ['Meta_%', 'Meta_T.R_%']
        for col in columnas_porcentaje_a_formatear:
            if col in reporte_df.columns:
                # Asegura que la columna es numérica antes de formatear
                numeric_col = pd.to_numeric(reporte_df[col], errors='coerce')
                # Aplica el formato de texto de porcentaje
                reporte_df[col] = numeric_col.apply(
                    lambda x: f'{(x * 100):.2f}%' if pd.notna(x) else ''
                )

        print("🏗️  Reordenando columnas según la configuración...")
        columnas_actuales = reporte_df.columns.tolist()
        columnas_ordenadas = [col for col in orden_columnas if col in columnas_actuales]
        columnas_restantes = [col for col in columnas_actuales if col not in columnas_ordenadas]
        reporte_df = reporte_df[columnas_ordenadas + columnas_restantes]
        
        return reporte_df


    def generate_consolidated_report(self, file_paths, orden_columnas):
        """
        Orquesta todo el proceso de ETL: cargar, transformar y consolidar los datos.
        """
        dataframes_por_tipo = self._load_dataframes(file_paths)

        def safe_concat(items):
            if not items: return pd.DataFrame()
            df_list = [item["data"] if isinstance(item, dict) else item for item in items]
            return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()

        # ... (toda la carga de dataframes: analisis_df, r91_df, etc. se queda igual)
        analisis_df = safe_concat(dataframes_por_tipo.get("ANALISIS", []))
        r91_df = safe_concat(dataframes_por_tipo.get("R91", []))
        vencimientos_df = safe_concat(dataframes_por_tipo.get("VENCIMIENTOS", []))
        r03_df = safe_concat(dataframes_por_tipo.get("R03", []))
        crtmp_df = safe_concat(dataframes_por_tipo.get("CRTMPCONSULTA1", []))
        fnz003_df = safe_concat(dataframes_por_tipo.get("FNZ003", []))
        matriz_cartera_df = safe_concat(dataframes_por_tipo.get("MATRIZ_CARTERA", []))
        sc04_df = safe_concat(dataframes_por_tipo.get("SC04", []))
        desembolsos_df = safe_concat(dataframes_por_tipo.get("DESEMBOLSOS_FINANSUEÑOS", []))
        metas_franjas_df = safe_concat(dataframes_por_tipo.get("METAS_FRANJAS", []))
        
        if r91_df.empty:
            print("\n❌ No se encontraron archivos R91 para construir el reporte base. Proceso detenido.")
            return None

        # ... (toda la creación del reporte_final y los merges se quedan igual)
        vencimientos_df = self._process_vencimientos_data(vencimientos_df)
        # --- FIN DE LA MODIFICACIÓN ---

        reporte_final['Empresa'] = np.where(reporte_final['Tipo_Credito'] == 'DF', 'FINANSUEÑOS', 'ARPESOD')
        
        for df, col_name in [(reporte_final, 'Cedula_Cliente'), (vencimientos_df, 'Cedula_Cliente'), (r03_df, 'Cedula_Cliente')]:
                if col_name in df.columns:
                    df[col_name] = df[col_name].astype(str).str.strip()

        print("🔍 Uniendo información de todos los archivos...")
        if not analisis_df.empty:
            columnas_a_excluir = ['Total_Cuotas', 'Valor_Cuota']
            columnas_analisis = [col for col in analisis_df.columns if col not in columnas_a_excluir]
            reporte_final = pd.merge(reporte_final, analisis_df[columnas_analisis].drop_duplicates('Credito'), on='Credito', how='left', suffixes=('', '_Analisis'))
        
        if not vencimientos_df.empty:
            # El merge ahora usa el dataframe ya procesado y no necesita drop_duplicates aquí
            reporte_final = pd.merge(reporte_final, vencimientos_df, on='Credito', how='left', suffixes=('', '_Venc'))
            
        if not r03_df.empty:
            reporte_final = pd.merge(reporte_final, r03_df.drop_duplicates('Cedula_Cliente'), on='Cedula_Cliente', how='left', suffixes=('', '_R03'))
        if not matriz_cartera_df.empty:
            reporte_final['Zona'] = reporte_final['Zona'].astype(str).str.strip()
            matriz_cartera_df['Zona'] = matriz_cartera_df['Zona'].astype(str).str.strip()
            reporte_final = pd.merge(reporte_final, matriz_cartera_df.drop_duplicates('Zona'), on='Zona', how='left')
        if not crtmp_df.empty:
            reporte_final = pd.merge(reporte_final, crtmp_df[['Credito', 'Correo', 'Fecha_Facturada']].drop_duplicates('Credito'), on='Credito', how='left')

        for item in dataframes_por_tipo.get("ASESORES", []):
            info_df = item["data"]
            merge_key = item["config"]["merge_on"]
            if not info_df.empty and merge_key in reporte_final.columns:
                info_df[merge_key] = info_df[merge_key].astype(str).str.strip()
                reporte_final[merge_key] = reporte_final[merge_key].astype(str).str.strip()
                reporte_final = pd.merge(reporte_final, info_df.drop_duplicates(merge_key), on=merge_key, how='left')
        
        # ... (el resto del método, con las llamadas a _assign_sales_invoice, _enrich_credit_details, etc., permanece igual)
        reporte_final = self._assign_sales_invoice(reporte_final, crtmp_df)
        reporte_final = self._enrich_credit_details(reporte_final, sc04_df, desembolsos_df)
        reporte_final = self._clean_installment_data(reporte_final)
        reporte_final = self._map_call_center_data(reporte_final)
        reporte_final = self._calculate_balances(reporte_final, fnz003_df)
        reporte_final = self._calculate_goal_metrics(reporte_final, metas_franjas_df)
        reporte_final = self._finalize_report(reporte_final, orden_columnas)

        return reporte_final