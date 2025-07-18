import pandas as pd
import numpy as np

class CreditDetailsService:
    """Servicio para manejar los detalles específicos de los créditos"""
    
    def enrich_credit_details(self, reporte_df, sc04_df, desembolsos_df):
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
            sc04_df.dropna(subset=['Factura_Venta'], inplace=True) 

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

    def process_vencimientos_data(self, vencimientos_df):
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
        
        # --- Cálculos de Atraso ---
        df_atrasados = df[df['Fecha_Cuota_Vigente'] < today].copy()
        if not df_atrasados.empty:
            mapa_valor_vencido = df_atrasados.groupby('Credito')['Valor_Cuota_Vigente'].sum()
            idx_primera_mora = df_atrasados.groupby('Credito')['Fecha_Cuota_Vigente'].idxmin()
            mapa_primera_mora = df.loc[idx_primera_mora].set_index('Credito')

            resumen_creditos['Valor_Vencido'] = resumen_creditos.index.map(mapa_valor_vencido)
            resumen_creditos['Fecha_Cuota_Atraso'] = resumen_creditos.index.map(mapa_primera_mora['Fecha_Cuota_Vigente'])
            resumen_creditos['Primera_Cuota_Mora'] = resumen_creditos.index.map(mapa_primera_mora['Cuota_Vigente'])
            resumen_creditos['Valor_Cuota_Atraso'] = resumen_creditos.index.map(mapa_primera_mora['Valor_Cuota_Vigente'])

        # --- Cálculos de Vigencia ---
        df_vigentes = df[(df['Fecha_Cuota_Vigente'].dt.year == today.year) & (df['Fecha_Cuota_Vigente'].dt.month == today.month)].copy()
        if not df_vigentes.empty:
            idx_ultima_vigente = df_vigentes.groupby('Credito')['Fecha_Cuota_Vigente'].idxmax()
            mapa_vigentes = df.loc[idx_ultima_vigente].set_index('Credito')
            
            resumen_creditos['Fecha_Cuota_Vigente'] = resumen_creditos.index.map(mapa_vigentes['Fecha_Cuota_Vigente'])
            resumen_creditos['Cuota_Vigente'] = resumen_creditos.index.map(mapa_vigentes['Cuota_Vigente'])
            resumen_creditos['Valor_Cuota_Vigente'] = resumen_creditos.index.map(mapa_vigentes['Valor_Cuota_Vigente'])

        print("✅ Resumen de vencimientos creado.")
        return resumen_creditos.reset_index()

    def adjust_arrears_status(self, reporte_df):
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

    def clean_installment_data(self, reporte_df):
        """Corrige valores erróneos en las columnas de cuotas."""
        print("🧼 Limpiando datos de cuotas...")
        columnas_a_limpiar = ['Cuotas_Pagadas', 'Cuota_Vigente', 'Primera_Cuota_Mora']
        
        for col in columnas_a_limpiar:
            if col in reporte_df.columns:
                reporte_df[col] = pd.to_numeric(reporte_df[col], errors='coerce')
                mask = (reporte_df[col] > 100) & (reporte_df[col].notna())
                reporte_df.loc[mask, col] = reporte_df.loc[mask, col] % 100
        return reporte_df