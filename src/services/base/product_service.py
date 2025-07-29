import pandas as pd
import numpy as np

class ProductsSalesService:
    """Servicio para manejar productos, obsequios y facturas de venta"""
    
    def assign_sales_invoice(self, reporte_df, crtmp_df):
        """
        Asigna la columna 'Factura_Venta' a cada crédito según la lógica de la empresa.
        Este método DEBE ejecutarse primero.
        """
        print("🧾 Asignando facturas de venta...")

        if crtmp_df.empty:
            print("⚠️ Archivo CRTMP no encontrado o vacío. No se pueden asignar facturas.")
            reporte_df['Factura_Venta'] = 'NO DISPONIBLE'
            return reporte_df

        reporte_df['Factura_Venta'] = np.nan

        # --- Lógica para FINANSUEÑOS ---
        filtro_fns = reporte_df['Empresa'] == 'FINANSUEÑOS'
        if filtro_fns.any():
            print("   - Procesando facturas de FINANSUEÑOS...")
            crtmp_copy = crtmp_df.copy()
            crtmp_copy['Fecha_Facturada'] = pd.to_datetime(crtmp_copy['Fecha_Facturada'], dayfirst=True, errors='coerce')
            if not crtmp_copy['Fecha_Facturada'].isnull().all():
                creditos_fns = crtmp_copy[crtmp_copy['Credito'].str.startswith('DF', na=False)].copy()
                facturas_fns = crtmp_copy[~crtmp_copy['Credito'].str.startswith('DF', na=False)].copy()
                merged_df = pd.merge(creditos_fns, facturas_fns, on='Cedula_Cliente', suffixes=('_credito', '_factura'))
                merged_df['dias_diferencia'] = (merged_df['Fecha_Facturada_factura'] - merged_df['Fecha_Facturada_credito']).dt.days.abs()
                coincidencias = merged_df[merged_df['dias_diferencia'] <= 30].copy()
                coincidencias.sort_values(by=['Credito_credito', 'dias_diferencia'], inplace=True)
                mapa_final = coincidencias.drop_duplicates(subset='Credito_credito', keep='first')
                mapa_facturas = pd.Series(mapa_final['Credito_factura'].values, index=mapa_final['Credito_credito']).to_dict()
                reporte_df.loc[filtro_fns, 'Factura_Venta'] = reporte_df.loc[filtro_fns, 'Credito'].map(mapa_facturas)

        # --- Lógica para ARPESOD ---
        print("   - Procesando facturas de ARPESOD...")
        prefijos = ['RTC', 'PR', 'NC', 'NT', 'NF']
        filtro_arp_especial = (reporte_df['Empresa'] == 'ARPESOD') & (reporte_df['Credito'].str.startswith(tuple(prefijos), na=False))
        if filtro_arp_especial.any():
            reporte_df['Numero_Busqueda'] = reporte_df['Credito'].str.split('-').str[1].str.strip()
            crtmp_busqueda = crtmp_df[['Cedula_Cliente', 'Tipo_Credito', 'Numero_Credito']].copy()
            for col in ['Numero_Credito', 'Cedula_Cliente']:
                crtmp_busqueda[col] = crtmp_busqueda[col].astype(str).str.strip()
            
            reporte_busqueda = reporte_df.loc[filtro_arp_especial, ['Credito', 'Cedula_Cliente', 'Numero_Busqueda']].copy()
            reporte_busqueda['Cedula_Cliente'] = reporte_busqueda['Cedula_Cliente'].astype(str).str.strip()
            
            merged_arp = pd.merge(reporte_busqueda, crtmp_busqueda, left_on=['Cedula_Cliente', 'Numero_Busqueda'], right_on=['Cedula_Cliente', 'Numero_Credito'], how='left')
            merged_arp['Factura_Encontrada'] = np.where(merged_arp['Numero_Credito'].notna(), merged_arp['Tipo_Credito'] + '-' + merged_arp['Numero_Credito'], np.nan)
            mapa_facturas_arp = pd.Series(merged_arp['Factura_Encontrada'].values, index=merged_arp['Credito']).to_dict()
            reporte_df.loc[filtro_arp_especial, 'Factura_Venta'] = reporte_df.loc[filtro_arp_especial, 'Credito'].map(mapa_facturas_arp)
            reporte_df.drop(columns=['Numero_Busqueda'], inplace=True, errors='ignore')

        filtro_arp_normal = (reporte_df['Empresa'] == 'ARPESOD') & (reporte_df['Factura_Venta'].isnull())
        reporte_df.loc[filtro_arp_normal, 'Factura_Venta'] = reporte_df['Credito']
        
        # Llenar vacíos restantes
        reporte_df['Factura_Venta'] = reporte_df['Factura_Venta'].fillna('NO ASIGNADA')
        
        print("✅ Asignación de facturas de venta completada.")
        return reporte_df

    def add_product_details(self, reporte_df, crtmp_df):
        """
        Usa la columna 'Factura_Venta' (previamente asignada) para añadir detalles de 
        productos, obsequios, cantidades y fechas.
        """
        print("🎁 Agregando productos, obsequios y detalles de la factura...")

        if crtmp_df.empty:
            print("⚠️ Archivo CRTMP no encontrado. No se pueden agregar detalles de productos.")
            reporte_df['Productos'] = 'NO DISPONIBLE'
            reporte_df['Cantidad_Productos'] = 0
            reporte_df['Obsequios'] = 'NO DISPONIBLE'
            reporte_df['Cantidad_Obsequios'] = 0
            return reporte_df


        crtmp_detalles = crtmp_df.copy()
        crtmp_detalles['Factura_Venta'] = crtmp_detalles['Tipo_Credito'].astype(str) + '-' + crtmp_detalles['Numero_Credito'].astype(str)
        crtmp_detalles['Total_Venta'] = pd.to_numeric(crtmp_detalles['Total_Venta'], errors='coerce').fillna(0)
        crtmp_detalles['Cantidad_Item'] = pd.to_numeric(crtmp_detalles['Cantidad_Item'], errors='coerce').fillna(1)
        
        # 2. Clasificar items y preparar texto (sin cambios)
        es_obsequio = (crtmp_detalles['Total_Venta'] >= 1000) & (crtmp_detalles['Total_Venta'] <= 2000)
        crtmp_detalles['Tipo_Item'] = np.where(es_obsequio, 'Obsequio', 'Producto')
        crtmp_detalles['Item_Texto'] = crtmp_detalles['Nombre_Producto'].astype(str) + ' (' + crtmp_detalles['Cantidad_Item'].astype(int).astype(str) + ')'

        # 3. Agrupar items por factura (SE ELIMINA LA BÚSQUEDA DE FECHA)
        productos = crtmp_detalles[crtmp_detalles['Tipo_Item'] == 'Producto'].groupby('Factura_Venta').agg(
            Productos=('Item_Texto', ', '.join),
            Cantidad_Productos=('Cantidad_Item', 'sum')
        )
        obsequios = crtmp_detalles[crtmp_detalles['Tipo_Item'] == 'Obsequio'].groupby('Factura_Venta').agg(
            Obsequios=('Item_Texto', ', '.join),
            Cantidad_Obsequios=('Cantidad_Item', 'sum')
        )

        # 4. Unir solo la información de productos y obsequios
        info_facturas = productos.join(obsequios, how='outer')

        # 5. Cruzar esta información con el reporte principal
        reporte_df = pd.merge(reporte_df, info_facturas, on='Factura_Venta', how='left')
        
        # 6. Limpieza final de las nuevas columnas
        reporte_df['Productos'] = reporte_df['Productos'].fillna('NO REGISTRA')
        reporte_df['Obsequios'] = reporte_df['Obsequios'].fillna('SIN OBSEQUIOS')
        reporte_df['Cantidad_Productos'] = reporte_df['Cantidad_Productos'].fillna(0).astype(int)
        reporte_df['Cantidad_Obsequios'] = reporte_df['Cantidad_Obsequios'].fillna(0).astype(int)
        
        print("✅ Detalles de productos y obsequios agregados.")
        return reporte_df
