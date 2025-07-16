from pathlib import Path
import pandas as pd
import numpy as np

class ReportService:
    """
    Servicio optimizado para procesar y consolidar reportes de cartera
    con una arquitectura de flujo de datos clara y modular.
    """
    def __init__(self, config):
        self.config = config
        self.today = pd.Timestamp.now().normalize()

    def _get_file_type(self, filename):
        """Determina el tipo de archivo usando la configuración."""
        nombre_base = Path(filename).stem.upper().replace(" ", "_")
        # Ordenar por longitud de clave para evitar coincidencias parciales (ej. 'R9' vs 'R91')
        sorted_keys = sorted(self.config.keys(), key=len, reverse=True)
        for tipo in sorted_keys:
            if nombre_base.startswith(tipo.upper().replace(" ", "_")):
                return tipo
        return None

    def _load_dataframes(self, file_paths):
        """Lee y convierte archivos de Excel en un diccionario de DataFrames."""
        dataframes = {key: [] for key in self.config.keys()}
        print("--- 🔄 Iniciando lectura de archivos ---")
        for path in file_paths:
            try:
                file_name = Path(path).name
                file_type = self._get_file_type(file_name)
                if not file_type:
                    print(f"⚠️  Archivo '{file_name}' omitido por tipo no reconocido.")
                    continue

                config = self.config[file_type]
                df = pd.read_excel(path, engine=config.get('engine', 'openpyxl'), **config.get("read_options", {}))
                df.columns = df.columns.str.strip()

                if "rename_map" in config:
                    df = df[config["usecols"]].rename(columns=config["rename_map"])
                
                if file_type == "R03":
                    df = df.replace('.', 'SIN CODEUDOR').fillna('SIN CODEUDOR')

                dataframes[file_type].append(df)
                print(f"✅ Archivo '{file_name}' procesado como '{file_type}'.")
            except Exception as e:
                print(f"❌ Error procesando '{path}': {e}")
        return {k: pd.concat(v, ignore_index=True) if v else pd.DataFrame() for k, v in dataframes.items()}

    def _create_credit_key(self, df):
        """Crea una llave 'Credito' robusta y estandarizada."""
        if df.empty or not {'Numero_Credito', 'Tipo_Credito'}.issubset(df.columns):
            return df
        
        df['Tipo_Credito'] = df['Tipo_Credito'].astype(str).str.strip().str.upper()
        df['Numero_Credito'] = pd.to_numeric(df['Numero_Credito'], errors='coerce').astype('Int64')
        df['Credito'] = df['Tipo_Credito'] + '-' + df['Numero_Credito'].astype(str)
        return df.dropna(subset=['Credito'])

    def _assign_sales_invoice(self, reporte_df, crtmp_df):
        """Asigna 'Factura_Venta', con lógica especial para FINANSUEÑOS."""
        print("🧾 Asignando facturas de venta...")
        reporte_df['Factura_Venta'] = np.where(reporte_df['Empresa'] == 'ARPESOD', reporte_df['Credito'], pd.NA)

        if crtmp_df.empty:
            reporte_df['Factura_Venta'].fillna('NO DISPONIBLE', inplace=True)
            return reporte_df

        fns_mask = reporte_df['Empresa'] == 'FINANSUEÑOS'
        if not fns_mask.any():
            return reporte_df

        crtmp_df['Fecha_Facturada'] = pd.to_datetime(crtmp_df['Fecha_Facturada'], dayfirst=True, errors='coerce')
        if crtmp_df['Fecha_Facturada'].isnull().all():
            reporte_df.loc[fns_mask, 'Factura_Venta'] = 'ERROR DE FECHA'
            return reporte_df

        id_cols = ['Cedula_Cliente', 'Credito', 'Fecha_Facturada']
        creditos = crtmp_df[crtmp_df['Credito'].str.startswith('DF', na=False)][id_cols]
        facturas = crtmp_df[~crtmp_df['Credito'].str.startswith('DF', na=False)][id_cols]

        merged = pd.merge(creditos, facturas, on='Cedula_Cliente', suffixes=('_cred', '_fact'))
        merged['dias_diff'] = (merged['Fecha_Facturada_fact'] - merged['Fecha_Facturada_cred']).dt.days.abs()
        
        valid_matches = merged[merged['dias_diff'] <= 30].sort_values(['Credito_cred', 'dias_diff'])
        mapa_facturas = valid_matches.drop_duplicates('Credito_cred', keep='first').set_index('Credito_cred')['Credito_fact']
        
        reporte_df.loc[fns_mask, 'Factura_Venta'] = reporte_df.loc[fns_mask, 'Credito'].map(mapa_facturas)
        reporte_df['Factura_Venta'].fillna('NO ASIGNADA', inplace=True)
        return reporte_df

    def _add_products_and_gifts(self, reporte_df, crtmp_df):
        """Añade columnas de productos/obsequios y sus cantidades."""
        print("🎁 Agregando productos y obsequios...")
        if crtmp_df.empty:
            return reporte_df.assign(Nombre_Producto='NO DISPONIBLE', Obsequio='NO DISPONIBLE', Cantidad_Producto=0, Cantidad_Obsequio=0)

        df = crtmp_df.copy()
        df['Total_Venta'] = pd.to_numeric(df['Total_Venta'], errors='coerce')
        df['Cantidad_Item'] = pd.to_numeric(df['Cantidad_Item'], errors='coerce').fillna(0)
        
        # Determinar llave de cruce (Credito para ARPESOD, Factura_Venta para FINANSUEÑOS)
        df['join_key'] = np.where(df['Credito'].str.startswith('DF', na=False), df['Credito'], df['Credito']) # Asumiendo que Factura_Venta es el mismo Credito para Arpesod
        
        es_producto = df['Total_Venta'] > 6000
        es_obsequio = df['Total_Venta'] <= 6000

        def aggregate_items(data, condition, name):
            agg = data[condition].groupby('join_key').agg(
                Item_Name=('Nombre_Producto', lambda s: ', '.join(s.dropna().astype(str).unique())),
                Item_Count=('Cantidad_Item', 'sum')
            ).rename(columns={'Item_Name': name, 'Item_Count': f'Cantidad_{name}'})
            return agg

        productos = aggregate_items(df, es_producto, 'Producto')
        obsequios = aggregate_items(df, es_obsequio, 'Obsequio')
        
        # Asignar llave de cruce al reporte principal
        reporte_df['join_key'] = np.where(reporte_df['Empresa'] == 'FINANSUEÑOS', reporte_df['Factura_Venta'], reporte_df['Credito'])
        
        reporte_df = reporte_df.merge(productos, on='join_key', how='left')
        reporte_df = reporte_df.merge(obsequios, on='join_key', how='left')

        reporte_df.fillna({'Producto': 'NO APLICA', 'Obsequio': 'NO APLICA', 'Cantidad_Producto': 0, 'Cantidad_Obsequio': 0}, inplace=True)
        reporte_df[['Cantidad_Producto', 'Cantidad_Obsequio']] = reporte_df[['Cantidad_Producto', 'Cantidad_Obsequio']].astype(int)
        reporte_df['Cantidad_Total_Producto'] = reporte_df['Cantidad_Producto'] + reporte_df['Cantidad_Obsequio']
        
        return reporte_df.drop(columns=['join_key'])

    def _enrich_credit_details(self, reporte_df, sc04_df, desembolsos_df):
        """Puebla detalles de cuotas y desembolso desde SC04 y Desembolsos."""
        print("✨ Enriqueciendo detalles de crédito...")
        # Lógica para ARPESOD (SC04)
        if not sc04_df.empty:
            sc04_df['Factura_Venta'] = sc04_df['Factura_Venta'].str.split(',').str[-2:].str.join('-')
            sc04_df.dropna(subset=['Factura_Venta'], inplace=True)
            sc04_df['Valor_Cuota'] = pd.to_numeric(sc04_df['Valor_Cuota'], errors='coerce')
            sc04_df['Total_Cuotas'] = pd.to_numeric(sc04_df['Total_Cuotas'], errors='coerce')
            sc04_df['Valor_Desembolso'] = sc04_df['Valor_Cuota'] * sc04_df['Total_Cuotas']
            mapa_arp = sc04_df.drop_duplicates('Factura_Venta', keep='last').set_index('Factura_Venta')
            
            mask_arp = reporte_df['Empresa'] == 'ARPESOD'
            for col in ['Total_Cuotas', 'Valor_Cuota', 'Valor_Desembolso']:
                reporte_df.loc[mask_arp, col] = reporte_df.loc[mask_arp, 'Factura_Venta'].map(mapa_arp[col])

        # Lógica para FINANSUEÑOS (Desembolsos)
        if not desembolsos_df.empty:
            mapa_fns = desembolsos_df.drop_duplicates('Credito', keep='last').set_index('Credito')
            mask_fns = reporte_df['Empresa'] == 'FINANSUEÑOS'
            for col in ['Total_Cuotas', 'Valor_Cuota', 'Valor_Desembolso']:
                reporte_df.loc[mask_fns, col] = reporte_df.loc[mask_fns, 'Credito'].map(mapa_fns[col])
        return reporte_df

    def _process_vencimientos_data(self, df):
        """Procesa y resume la información de vencimientos."""
        print("⚙️  Procesando datos de VENCIMIENTOS...")
        if df.empty: return pd.DataFrame()
        
        df['Fecha_Cuota_Vigente'] = pd.to_datetime(df['Fecha_Cuota_Vigente'], errors='coerce')
        df.dropna(subset=['Credito', 'Fecha_Cuota_Vigente'], inplace=True)
        
        atrasados = df[df['Fecha_Cuota_Vigente'] < self.today]
        primera_mora = atrasados.loc[atrasados.groupby('Credito')['Fecha_Cuota_Vigente'].idxmin()]
        mapa_mora = primera_mora.set_index('Credito').rename(columns={
            'Fecha_Cuota_Vigente': 'Fecha_Cuota_Atraso',
            'Cuota_Vigente': 'Primera_Cuota_Mora',
            'Valor_Cuota_Vigente': 'Valor_Cuota_Atraso'
        })
        mapa_mora['Valor_Vencido'] = atrasados.groupby('Credito')['Valor_Cuota_Vigente'].sum()
        
        resumen = pd.DataFrame(df['Credito'].unique(), columns=['Credito'])
        return resumen.merge(mapa_mora, on='Credito', how='left')

    def _map_call_center_data(self, reporte_df):
        """Consolida la información del Call Center basado en la franja de mora."""
        print("📞 Mapeando datos de Call Center...")
        reporte_df['Dias_Atraso'] = pd.to_numeric(reporte_df['Dias_Atraso'], errors='coerce').fillna(0)
        
        condiciones = [
            reporte_df['Dias_Atraso'] == 0,
            reporte_df['Dias_Atraso'].between(1, 30),
            reporte_df['Dias_Atraso'].between(31, 90),
            reporte_df['Dias_Atraso'] > 90
        ]
        valores = ['AL DIA', '1 A 30 DIAS', '31 A 90 DIAS', '91 A 360 DIAS']
        reporte_df['Franja_Mora'] = np.select(condiciones, valores, default='SIN INFO')

        mapa_franjas = {
            '1 A 30 DIAS': ('1_30', 'dias'),
            '31 A 90 DIAS': ('31_90', 'dias'),
            '91 A 360 DIAS': ('91_360', 'dias')
        }
        
        reporte_df['Call_Center_Apoyo'] = pd.NA
        reporte_df['Nombre_Call_Center'] = pd.NA
        reporte_df['Telefono_Call_Center'] = pd.NA

        for franja, (prefijo, _) in mapa_franjas.items():
            mask = reporte_df['Franja_Mora'] == franja
            reporte_df.loc[mask, 'Call_Center_Apoyo'] = reporte_df.loc[mask, f'call_center_{prefijo}_{_}']
            reporte_df.loc[mask, 'Nombre_Call_Center'] = reporte_df.loc[mask, f'call_center_nombre_{prefijo}']
            reporte_df.loc[mask, 'Telefono_Call_Center'] = reporte_df.loc[mask, f'call_center_telefono_{prefijo}']

        cols_to_drop = [f'call_center_{p}_{s}' for p,s in mapa_franjas.values()] + \
                       [f'call_center_nombre_{p}' for p,_ in mapa_franjas.values()] + \
                       [f'call_center_telefono_{p}' for p,_ in mapa_franjas.values()]
        return reporte_df.drop(columns=cols_to_drop, errors='ignore')


    def _calculate_balances(self, reporte_df, fnz003_df):
        """Calcula y agrega los diferentes saldos al reporte."""
        print("📊 Calculando saldos...")
        reporte_df['Saldo_Capital'] = np.where(reporte_df['Empresa'] == 'ARPESOD', reporte_df.get('Saldo_Factura'), np.nan)
        if not fnz003_df.empty:
            saldos = fnz003_df.groupby(['Credito', 'Concepto'])['Saldo'].sum().unstack(fill_value=0)
            mapa_capital = saldos.get('CAPITAL', 0) + saldos.get('ABONO DIF TASA', 0)
            
            reporte_df['Saldo_Capital'] = reporte_df['Credito'].map(mapa_capital).combine_first(reporte_df['Saldo_Capital'])
            reporte_df['Saldo_Avales'] = reporte_df['Credito'].map(saldos.get('AVAL', 0))
            reporte_df['Saldo_Interes_Corriente'] = reporte_df['Credito'].map(saldos.get('INTERES CORRIENTE', 0))
        
        reporte_df['Saldo_Capital'] = pd.to_numeric(reporte_df['Saldo_Capital'], errors='coerce').fillna(0).astype(int)
        reporte_df['Saldo_Avales'] = np.where(reporte_df['Empresa'] == 'FINANSUEÑOS', reporte_df.get('Saldo_Avales', 0).fillna(0).astype(int), 'NO APLICA')
        reporte_df['Saldo_Interes_Corriente'] = np.where(reporte_df['Empresa'] == 'FINANSUEÑOS', reporte_df.get('Saldo_Interes_Corriente', 0).fillna(0).astype(int), 'NO APLICA')
        return reporte_df

    def _finalize_report(self, reporte_df, orden_columnas):
        """Realiza la limpieza, formato y reordenamiento final."""
        print("🧹 Finalizando reporte...")
        # Limpieza de datos de mora
        sin_mora_mask = pd.to_numeric(reporte_df['Dias_Atraso'], errors='coerce').fillna(0) <= 0
        for col in ['Fecha_Cuota_Atraso', 'Primera_Cuota_Mora', 'Valor_Cuota_Atraso', 'Valor_Vencido']:
            if col in reporte_df.columns:
                default = 0 if 'Valor' in col else 'SIN MORA'
                reporte_df.loc[sin_mora_mask, col] = default

        # Formateo de fechas
        for col in ['Fecha_Cuota_Vigente', 'Fecha_Cuota_Atraso', 'Fecha_Facturada']:
            if col in reporte_df.columns:
                reporte_df[col] = pd.to_datetime(reporte_df[col], errors='coerce').dt.strftime('%d/%m/%Y').fillna('N/A')
        
        # Columnas finales
        final_cols = [col for col in orden_columnas if col in reporte_df.columns]
        return reporte_df[final_cols]


    def generate_consolidated_report(self, file_paths, orden_columnas, start_date=None, end_date=None):
        """Orquesta el proceso de ETL para generar el reporte consolidado."""
        dataframes = self._load_dataframes(file_paths)
        
        # Estandarizar llaves en todos los dataframes necesarios
        for key in ["R91", "ANALISIS", "VENCIMIENTOS", "CRTMPCONSULTA1", "FNZ003", "DESEMBOLSOS_FINANSUEÑOS"]:
            if key in dataframes:
                dataframes[key] = self._create_credit_key(dataframes[key])

        if dataframes["R91"].empty:
            print("❌ No se encontró el archivo base R91. No se puede generar el reporte.")
            return None

        # --- Flujo de Procesamiento con .pipe() ---
        reporte_final = (
            dataframes["R91"]
            .pipe(lambda df: pd.merge(df, self._process_vencimientos_data(dataframes.get("VENCIMIENTOS")), on='Credito', how='left'))
            .pipe(lambda df: pd.merge(df, dataframes.get("ANALISIS", pd.DataFrame()).drop_duplicates('Credito'), on='Credito', how='left', suffixes=('', '_Analisis')))
            .pipe(lambda df: pd.merge(df, dataframes.get("R03", pd.DataFrame()).drop_duplicates('Cedula_Cliente'), on='Cedula_Cliente', how='left', suffixes=('', '_R03')))
            .pipe(lambda df: pd.merge(df, dataframes.get("MATRIZ_CARTERA", pd.DataFrame()).drop_duplicates('Zona'), on='Zona', how='left'))
            .assign(Empresa=np.where(lambda df: df['Tipo_Credito'] == 'DF', 'FINANSUEÑOS', 'ARPESOD'))
            .pipe(self._assign_sales_invoice, dataframes.get("CRTMPCONSULTA1"))
            .pipe(self._add_products_and_gifts, dataframes.get("CRTMPCONSULTA1"))
            .pipe(self._enrich_credit_details, dataframes.get("SC04"), dataframes.get("DESEMBOLSOS_FINANSUEÑOS"))
            .pipe(self._map_call_center_data)
            .pipe(self._calculate_balances, dataframes.get("FNZ003"))
            # .pipe(self._calculate_goal_metrics, dataframes.get("METAS_FRANJAS")) # Asumiendo que esta lógica también se refactoriza
            .pipe(self._finalize_report, orden_columnas)
        )

        print("\n🎉 Reporte consolidado generado exitosamente.")
        return reporte_final