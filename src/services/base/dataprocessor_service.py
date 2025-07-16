import pandas as pd
import numpy as np

class ReportDataProcessor:
    """
    Contiene todos los métodos para transformar, enriquecer y calcular
    los datos del reporte de cartera.
    """

    def create_credit_key(self, df: pd.DataFrame) -> pd.DataFrame:
        """Crea una llave 'Credito' robusta, limpiando espacios y estandarizando tipos."""
        if df.empty or not all(col in df.columns for col in ['Numero_Credito', 'Tipo_Credito']):
            return df
            
        df['Tipo_Credito'] = df['Tipo_Credito'].astype(str).str.strip().str.upper()
        df['Numero_Credito'] = pd.to_numeric(df['Numero_Credito'], errors='coerce').astype('Int64')
        df['Credito'] = df['Tipo_Credito'] + '-' + df['Numero_Credito'].astype(str).str.replace('<NA>', '', regex=False)
        return df

    def add_products_and_gifts(self, reporte_df: pd.DataFrame, crtmp_df: pd.DataFrame) -> pd.DataFrame:
        """
        Añade columnas de productos/obsequios y sus cantidades al reporte final,
        usando una llave de agrupación diferente por empresa.
        """
        print("🎁 Agregando productos, obsequios y cantidades al reporte final...")
        
        if crtmp_df.empty:
            reporte_df['Nombre_Producto'] = 'NO DISPONIBLE'
            reporte_df['Obsequio'] = 'NO DISPONIBLE'
            reporte_df['Cantidad_Producto'] = 0
            reporte_df['Cantidad_Obsequio'] = 0
            reporte_df['Cantidad_Total_Producto'] = 0
            return reporte_df

        df_items = crtmp_df.copy()
        df_items['Total_Venta'] = pd.to_numeric(df_items['Total_Venta'], errors='coerce')
        df_items['Cantidad_Item'] = pd.to_numeric(df_items['Cantidad_Item'], errors='coerce').fillna(0)

        def join_unique(series):
            items = series.dropna().astype(str).unique()
            return ', '.join(items) if len(items) > 0 else 'NO APLICA'

        es_producto = df_items['Total_Venta'] > 6000
        es_obsequio = df_items['Total_Venta'] <= 6000

        mapa_nombres_productos = df_items[es_producto].groupby('Credito')['Nombre_Producto'].apply(join_unique)
        mapa_nombres_obsequios = df_items[es_obsequio].groupby('Credito')['Nombre_Producto'].apply(join_unique)
        mapa_cantidad_productos = df_items[es_producto].groupby('Credito')['Cantidad_Item'].sum()
        mapa_cantidad_obsequios = df_items[es_obsequio].groupby('Credito')['Cantidad_Item'].sum()

        es_arpesod = reporte_df['Empresa'] == 'ARPESOD'
        es_finansuenos = reporte_df['Empresa'] == 'FINANSUEÑOS'

        reporte_df.loc[es_arpesod, 'Nombre_Producto'] = reporte_df.loc[es_arpesod, 'Credito'].map(mapa_nombres_productos)
        reporte_df.loc[es_arpesod, 'Obsequio'] = reporte_df.loc[es_arpesod, 'Credito'].map(mapa_nombres_obsequios)
        reporte_df.loc[es_finansuenos, 'Nombre_Producto'] = reporte_df.loc[es_finansuenos, 'Factura_Venta'].map(mapa_nombres_productos)
        reporte_df.loc[es_finansuenos, 'Obsequio'] = reporte_df.loc[es_finansuenos, 'Factura_Venta'].map(mapa_nombres_obsequios)
        
        reporte_df.loc[es_arpesod, 'Cantidad_Producto'] = reporte_df.loc[es_arpesod, 'Credito'].map(mapa_cantidad_productos)
        reporte_df.loc[es_arpesod, 'Cantidad_Obsequio'] = reporte_df.loc[es_arpesod, 'Credito'].map(mapa_cantidad_obsequios)
        reporte_df.loc[es_finansuenos, 'Cantidad_Producto'] = reporte_df.loc[es_finansuenos, 'Factura_Venta'].map(mapa_cantidad_productos)
        reporte_df.loc[es_finansuenos, 'Cantidad_Obsequio'] = reporte_df.loc[es_finansuenos, 'Factura_Venta'].map(mapa_cantidad_obsequios)
        
        reporte_df['Nombre_Producto'].fillna('NO APLICA', inplace=True)
        reporte_df['Obsequio'].fillna('NO APLICA', inplace=True)
        reporte_df['Cantidad_Producto'] = reporte_df['Cantidad_Producto'].fillna(0).astype(int)
        reporte_df['Cantidad_Obsequio'] = reporte_df['Cantidad_Obsequio'].fillna(0).astype(int)
        
        reporte_df['Cantidad_Total_Producto'] = reporte_df['Cantidad_Producto'] + reporte_df['Cantidad_Obsequio']
        
        print("✅ Productos, obsequios y cantidades asignados correctamente.")
        return reporte_df

    def enrich_credit_details(self, reporte_df: pd.DataFrame, sc04_df: pd.DataFrame, desembolsos_df: pd.DataFrame) -> pd.DataFrame:
        """
        Puebla las columnas Total_Cuotas, Valor_Cuota y Valor_Desembolso
        usando 'Factura_Venta' para SC04 y 'Credito' para Desembolsos.
        """
        print("✨ Enriqueciendo detalles de cuotas y desembolsos...")

        if sc04_df.empty and desembolsos_df.empty:
            print("⚠️ No se encontraron archivos SC04 ni de Desembolsos. Se omiten los detalles del crédito.")
            return reporte_df

        if not sc04_df.empty:
            def transformar_factura(valor):
                if isinstance(valor, str):
                    partes = valor.split(',')
                    if len(partes) >= 2:
                        return f"{partes[-2]}-{partes[-1]}"
                return None

            sc04_df['Factura_Venta'] = sc04_df['Factura_Venta'].apply(transformar_factura)
            sc04_df.dropna(subset=['Factura_Venta'], inplace=True)
            sc04_df.drop_duplicates(subset='Factura_Venta', keep='last', inplace=True)

            sc04_df['Valor_Cuota'] = pd.to_numeric(sc04_df['Valor_Cuota'], errors='coerce')
            sc04_df['Total_Cuotas'] = pd.to_numeric(sc04_df['Total_Cuotas'], errors='coerce')
            sc04_df['Valor_Desembolso'] = sc04_df['Valor_Cuota'] * sc04_df['Total_Cuotas']

            mapa_cuotas_arp = sc04_df.set_index('Factura_Venta')['Total_Cuotas']
            mapa_valor_arp = sc04_df.set_index('Factura_Venta')['Valor_Cuota']
            mapa_desembolso_arp = sc04_df.set_index('Factura_Venta')['Valor_Desembolso']

            mask_arp = reporte_df['Empresa'] == 'ARPESOD'
            reporte_df.loc[mask_arp, 'Total_Cuotas'] = reporte_df.loc[mask_arp, 'Factura_Venta'].map(mapa_cuotas_arp)
            reporte_df.loc[mask_arp, 'Valor_Cuota'] = reporte_df.loc[mask_arp, 'Factura_Venta'].map(mapa_valor_arp)
            reporte_df.loc[mask_arp, 'Valor_Desembolso'] = reporte_df.loc[mask_arp, 'Factura_Venta'].map(mapa_desembolso_arp)
        
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
    
    def process_vencimientos_data(self, vencimientos_df: pd.DataFrame) -> pd.DataFrame:
        """
        Procesa el dataframe de vencimientos de forma aislada para devolver un
        resumen con una fila por crédito y todas las columnas calculadas.
        """
        print("⚙️  Procesando datos de VENCIMIENTOS de forma aislada...")
        if vencimientos_df.empty:
            return pd.DataFrame()

        df = vencimientos_df.copy()
        df['Fecha_Cuota_Vigente'] = pd.to_datetime(df['Fecha_Cuota_Vigente'], errors='coerce')
        df['Valor_Cuota_Vigente'] = pd.to_numeric(df['Valor_Cuota_Vigente'], errors='coerce')
        df.dropna(subset=['Credito', 'Fecha_Cuota_Vigente'], inplace=True)

        today = pd.Timestamp.now().normalize()
        resumen_creditos = pd.DataFrame(df['Credito'].unique(), columns=['Credito']).set_index('Credito')
        
        df_atrasados = df[df['Fecha_Cuota_Vigente'] < today].copy()
        if not df_atrasados.empty:
            mapa_valor_vencido = df_atrasados.groupby('Credito')['Valor_Cuota_Vigente'].sum()
            idx_primera_mora = df_atrasados.groupby('Credito')['Fecha_Cuota_Vigente'].idxmin()
            mapa_primera_mora = df.loc[idx_primera_mora].set_index('Credito')

            resumen_creditos['Valor_Vencido'] = resumen_creditos.index.map(mapa_valor_vencido)
            resumen_creditos['Fecha_Cuota_Atraso'] = resumen_creditos.index.map(mapa_primera_mora['Fecha_Cuota_Vigente'])
            resumen_creditos['Primera_Cuota_Mora'] = resumen_creditos.index.map(mapa_primera_mora['Cuota_Vigente'])
            resumen_creditos['Valor_Cuota_Atraso'] = resumen_creditos.index.map(mapa_primera_mora['Valor_Cuota_Vigente'])

        df_vigentes = df[(df['Fecha_Cuota_Vigente'].dt.year == today.year) & (df['Fecha_Cuota_Vigente'].dt.month == today.month)].copy()
        if not df_vigentes.empty:
            idx_ultima_vigente = df_vigentes.groupby('Credito')['Fecha_Cuota_Vigente'].idxmax()
            mapa_vigentes = df.loc[idx_ultima_vigente].set_index('Credito')
            
            resumen_creditos['Fecha_Cuota_Vigente'] = resumen_creditos.index.map(mapa_vigentes['Fecha_Cuota_Vigente'])
            resumen_creditos['Cuota_Vigente'] = resumen_creditos.index.map(mapa_vigentes['Cuota_Vigente'])
            resumen_creditos['Valor_Cuota_Vigente'] = resumen_creditos.index.map(mapa_vigentes['Valor_Cuota_Vigente'])

        print("✅ Resumen de vencimientos creado.")
        return resumen_creditos.reset_index()
    
    def adjust_arrears_status(self, reporte_df: pd.DataFrame) -> pd.DataFrame:
        """Ajusta el estado de mora basado en la columna 'Dias_Atraso' del reporte final."""
        print("🔧 Ajustando estado final de la mora...")
        if 'Dias_Atraso' in reporte_df.columns:
            sin_mora_mask = (pd.to_numeric(reporte_df['Dias_Atraso'], errors='coerce').fillna(0) == 0)
            columnas_mora_a_limpiar = ['Fecha_Cuota_Atraso', 'Primera_Cuota_Mora', 'Valor_Cuota_Atraso', 'Valor_Vencido']
            for col in columnas_mora_a_limpiar:
                if col in reporte_df.columns:
                    valor_a_poner = 0 if 'Valor' in col else 'SIN MORA'
                    reporte_df.loc[sin_mora_mask, col] = valor_a_poner
        return reporte_df
    
    def clean_installment_data(self, reporte_df: pd.DataFrame) -> pd.DataFrame:
        """Corrige valores erróneos en las columnas de cuotas."""
        print("🧼 Limpiando datos de cuotas...")
        columnas_a_limpiar = ['Cuotas_Pagadas', 'Cuota_Vigente', 'Primera_Cuota_Mora']
        for col in columnas_a_limpiar:
            if col in reporte_df.columns:
                reporte_df[col] = pd.to_numeric(reporte_df[col], errors='coerce')
                mask = (reporte_df[col] > 100) & (reporte_df[col].notna())
                reporte_df.loc[mask, col] = reporte_df.loc[mask, col] % 100
        return reporte_df
    
    def assign_sales_invoice(self, reporte_df: pd.DataFrame, crtmp_df: pd.DataFrame) -> pd.DataFrame:
        """
        Crea la columna 'Factura_Venta' asignando el valor según la empresa.
        Para FINANSUEÑOS, busca la factura correspondiente en el archivo CRTMPCONSULTA1.
        """
        print("🧾 Asignando facturas de venta...")
        if crtmp_df.empty:
            print("⚠️ Archivo CRTMPCONSULTA1 no encontrado o vacío. No se pueden asignar facturas para FINANSUEÑOS.")
            reporte_df['Factura_Venta'] = np.where(reporte_df['Empresa'] == 'ARPESOD', reporte_df['Credito'], 'NO DISPONIBLE')
            return reporte_df

        reporte_df['Factura_Venta'] = np.nan
        reporte_df.loc[reporte_df['Empresa'] == 'ARPESOD', 'Factura_Venta'] = reporte_df['Credito']
        
        crtmp_df_copy = crtmp_df.copy()
        crtmp_df_copy['Fecha_Facturada'] = pd.to_datetime(crtmp_df_copy['Fecha_Facturada'], dayfirst=True, errors='coerce')
        
        if crtmp_df_copy['Fecha_Facturada'].isnull().all():
            print("❌ Error crítico: No se pudo interpretar ninguna fecha en CRTMPCONSULTA1. Verifique el formato.")
            reporte_df['Factura_Venta'].fillna('ERROR DE FECHA', inplace=True)
            return reporte_df

        creditos_fns = crtmp_df_copy[crtmp_df_copy['Credito'].str.startswith('DF', na=False)].copy()
        facturas_fns = crtmp_df_copy[~crtmp_df_copy['Credito'].str.startswith('DF', na=False)].copy()

        merged_df = pd.merge(creditos_fns, facturas_fns, on='Cedula_Cliente', suffixes=('_credito', '_factura'))
        
        merged_df['dias_diferencia'] = (merged_df['Fecha_Facturada_factura'] - merged_df['Fecha_Facturada_credito']).dt.days.abs()
        coincidencias_validas = merged_df[merged_df['dias_diferencia'] <= 30].copy()

        coincidencias_validas.sort_values(by=['Credito_credito', 'dias_diferencia'], inplace=True)
        mapeo_final = coincidencias_validas.drop_duplicates(subset='Credito_credito', keep='first')
        
        mapa_facturas = pd.Series(mapeo_final['Credito_factura'].values, index=mapeo_final['Credito_credito']).to_dict()

        filtro_fns = reporte_df['Empresa'] == 'FINANSUEÑOS'
        reporte_df.loc[filtro_fns, 'Factura_Venta'] = reporte_df.loc[filtro_fns, 'Credito'].map(mapa_facturas)
        reporte_df['Factura_Venta'].fillna('NO ASIGNADA', inplace=True)

        return reporte_df
    
    def map_call_center_data(self, reporte_df: pd.DataFrame) -> pd.DataFrame:
        """
        Crea las columnas 'Franja_Mora' y de Call Center consolidadas,
        basándose en los días de atraso.
        """
        print("Mappings de datos de Call Center...")
        if 'Dias_Atraso' not in reporte_df.columns:
            print("⚠️ Columna 'Dias_Atraso' no encontrada. No se puede mapear la franja de mora.")
            return reporte_df
            
        reporte_df['Dias_Atraso'] = pd.to_numeric(reporte_df['Dias_Atraso'], errors='coerce')

        condiciones_mora = [
            reporte_df['Dias_Atraso'] == 0,
            reporte_df['Dias_Atraso'].between(1, 30),
            reporte_df['Dias_Atraso'].between(31, 90),
            reporte_df['Dias_Atraso'] > 90
        ]
        valores_mora = ['AL DIA', '1 A 30 DIAS', '31 A 90 DIAS', '91 A 360 DIAS']
        reporte_df['Franja_Mora'] = np.select(condiciones_mora, valores_mora, default='SIN INFO')

        mapa_columnas = {
            '1 A 30 DIAS': ('call_center_1_30_dias', 'call_center_nombre_1_30', 'call_center_telefono_1_30'),
            '31 A 90 DIAS': ('call_center_31_90_dias', 'call_center_nombre_31_90', 'call_center_telefono_31_90'),
            '91 A 360 DIAS': ('call_center_91_360_dias', 'call_center_nombre_91_360', 'call_center_telefono_91_360')
        }
        reporte_df['Call_Center_Apoyo'] = np.nan
        reporte_df['Nombre_Call_Center'] = np.nan
        reporte_df['Telefono_Call_Center'] = np.nan

        for franja, cols in mapa_columnas.items():
            mask = reporte_df['Franja_Mora'] == franja
            if cols[0] in reporte_df.columns:
                reporte_df.loc[mask, 'Call_Center_Apoyo'] = reporte_df.loc[mask, cols[0]]
            if cols[1] in reporte_df.columns:
                reporte_df.loc[mask, 'Nombre_Call_Center'] = reporte_df.loc[mask, cols[1]]
            if cols[2] in reporte_df.columns:
                reporte_df.loc[mask, 'Telefono_Call_Center'] = reporte_df.loc[mask, cols[2]]

        cols_a_borrar_matriz = [
            'call_center_1_30_dias', 'call_center_nombre_1_30', 'call_center_telefono_1_30', 
            'call_center_31_90_dias', 'call_center_nombre_31_90', 'call_center_telefono_31_90', 
            'call_center_91_360_dias', 'call_center_nombre_91_360', 'call_center_telefono_91_360'
        ]
        columnas_existentes_a_borrar = [col for col in cols_a_borrar_matriz if col in reporte_df.columns]
        reporte_df.drop(columns=columnas_existentes_a_borrar, inplace=True, errors='ignore')
        
        return reporte_df
    
    def filter_by_date_range(self, reporte_df: pd.DataFrame, start_date, end_date) -> pd.DataFrame:
        """
        Filtra el reporte final por un rango de fechas en la columna 'Fecha_Cuota_Vigente'.
        Este filtro es opcional.
        """
        if not start_date or not end_date:
            return reporte_df

        print(f"🔍 Aplicando filtro de fecha: desde {start_date} hasta {end_date}")
        df = reporte_df.copy()
        df['Fecha_Cuota_Vigente'] = pd.to_datetime(df['Fecha_Cuota_Vigente'], format='%d/%m/%Y', errors='coerce')
        start_date_dt = pd.to_datetime(start_date, format='%d/%m/%Y', errors='coerce')
        end_date_dt = pd.to_datetime(end_date, format='%d/%m/%Y', errors='coerce')

        if pd.isna(start_date_dt) or pd.isna(end_date_dt):
            print("⚠️ Formato de fecha inválido. Se omite el filtro.")
            return reporte_df

        mask = (df['Fecha_Cuota_Vigente'] >= start_date_dt) & (df['Fecha_Cuota_Vigente'] <= end_date_dt)
        filtered_df = df[mask]
        print(f"✅ Filtro aplicado. {len(filtered_df)} registros encontrados en el rango.")
        
        return filtered_df

    def calculate_balances(self, reporte_df: pd.DataFrame, fnz003_df: pd.DataFrame) -> pd.DataFrame:
        """Calcula los diferentes saldos y los agrega al reporte."""
        print("📊 Calculando saldos...")
        reporte_df['Saldo_Capital'] = np.where(reporte_df['Empresa'] == 'ARPESOD', reporte_df.get('Saldo_Factura'), np.nan)
        if not fnz003_df.empty:
            fnz003_df = self.create_credit_key(fnz003_df)
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
    
    def calculate_goal_metrics(self, reporte_df: pd.DataFrame, metas_franjas_df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula las diferentes métricas de metas, interpretando correctamente
        los porcentajes y limpiando las columnas intermedias al final.
        """
        print("🎯 Calculando métricas de metas...")
        for col in ['Meta_DC_Al_Dia', 'Meta_DC_Atraso', 'Meta_Atraso']:
            if col in reporte_df.columns:
                reporte_df[col] = pd.to_numeric(reporte_df[col], errors='coerce').fillna(0)
        reporte_df['Meta_General'] = reporte_df['Meta_DC_Al_Dia'] + reporte_df['Meta_DC_Atraso'] + reporte_df['Meta_Atraso']

        if metas_franjas_df.empty:
            print("⚠️ Archivo de Metas por Franja no encontrado. Se omiten los cálculos de metas % y $.")
            return reporte_df

        metas_franjas_df['Zona'] = metas_franjas_df['Zona'].astype(str).str.strip()
        reporte_df['Zona'] = reporte_df['Zona'].astype(str).str.strip()
        columnas_metas_a_borrar = [col for col in metas_franjas_df.columns if col != 'Zona']
        reporte_df = pd.merge(reporte_df, metas_franjas_df, on='Zona', how='left')

        print("  - Convirtiendo porcentajes a números...")
        columnas_porcentaje = ['Meta_1_A_30', 'Meta_31_A_90', 'Meta_91_A_180', 'Meta_181_A_360', 'Total_Recaudo']
        for col in columnas_porcentaje:
            if col in reporte_df.columns:
                reporte_df[col] = reporte_df[col].astype(str).str.replace(',', '.', regex=False).str.strip('%')
                reporte_df[col] = pd.to_numeric(reporte_df[col], errors='coerce') / 100
                reporte_df[col] = reporte_df[col].fillna(0)

        dias_atraso = reporte_df['Dias_Atraso']
        condiciones = [dias_atraso.between(1, 30), dias_atraso.between(31, 90), dias_atraso.between(91, 180), dias_atraso.between(181, 360)]
        valores = [reporte_df['Meta_1_A_30'], reporte_df['Meta_31_A_90'], reporte_df['Meta_91_A_180'], reporte_df['Meta_181_A_360']]
        reporte_df['Meta_%'] = np.select(condiciones, valores, default=0)
        reporte_df['Meta_$'] = reporte_df['Meta_General'] * reporte_df['Meta_%']
        reporte_df['Meta_T.R_%'] = reporte_df['Total_Recaudo']

        saldo_capital_num = pd.to_numeric(reporte_df['Saldo_Capital'], errors='coerce').fillna(0)
        saldo_avales_num = pd.to_numeric(reporte_df['Saldo_Avales'], errors='coerce').fillna(0)
        saldo_interes_num = pd.to_numeric(reporte_df['Saldo_Interes_Corriente'], errors='coerce').fillna(0)
        total_saldo_fns = saldo_capital_num + saldo_avales_num + saldo_interes_num
        
        reporte_df['Meta_T.R_$'] = np.where(reporte_df['Empresa'] == 'FINANSUEÑOS', total_saldo_fns, saldo_capital_num) * reporte_df['Meta_T.R_%']

        print("  - Limpiando columnas de metas intermedias...")
        reporte_df.drop(columns=columnas_metas_a_borrar, inplace=True, errors='ignore')
        return reporte_df

    def finalize_report(self, reporte_df: pd.DataFrame, orden_columnas: list) -> pd.DataFrame:
        """Realiza la limpieza, formato y reordenamiento final del reporte."""
        print("🧹 Realizando transformaciones y limpieza final...")
        columnas_vencimiento = {
            'Fecha_Cuota_Vigente': 'VIGENCIA EXPIRADA', 'Cuota_Vigente': 'VIGENCIA EXPIRADA',
            'Valor_Cuota_Vigente': 'VIGENCIA EXPIRADA', 'Fecha_Cuota_Atraso': 'SIN MORA',
            'Primera_Cuota_Mora': 'SIN MORA', 'Valor_Cuota_Atraso': 0, 'Valor_Vencido': 0
        }
        for col, default_value in columnas_vencimiento.items():
            if col not in reporte_df.columns:
                reporte_df[col] = default_value
            else:
                if 'Fecha' in col:
                    reporte_df[col] = pd.to_datetime(reporte_df[col], errors='coerce').dt.strftime('%d/%m/%Y')
                reporte_df[col].fillna(default_value, inplace=True)

        if 'Fecha_Facturada' in reporte_df.columns:
             reporte_df['Fecha_Facturada'] = pd.to_datetime(reporte_df['Fecha_Facturada'], errors='coerce').dt.strftime('%d/%m/%Y').fillna('')

        print("✨ Formateando columnas de porcentaje...")
        columnas_porcentaje = ['Meta_%', 'Meta_T.R_%']
        for col in columnas_porcentaje:
            if col in reporte_df.columns:
                numeric_col = pd.to_numeric(reporte_df[col], errors='coerce')
                reporte_df[col] = numeric_col.apply(lambda x: f'{(x * 100):.2f}%' if pd.notna(x) else '')
        
        print("🏗️  Reordenando columnas según la configuración...")
        columnas_a_eliminar = ['Saldo_Factura'] + [col for col in reporte_df.columns if '_Analisis' in col or '_R03' in col or '_Venc' in col]
        reporte_df.drop(columns=columnas_a_eliminar, inplace=True, errors='ignore')
        
        columnas_actuales = reporte_df.columns.tolist()
        columnas_ordenadas = [col for col in orden_columnas if col in columnas_actuales]
        columnas_restantes = [col for col in columnas_actuales if col not in columnas_ordenadas]
        
        return reporte_df[columnas_ordenadas + columnas_restantes]