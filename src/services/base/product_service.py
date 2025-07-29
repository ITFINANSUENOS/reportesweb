import pandas as pd
import numpy as np

class ProductsSalesService:
    """Servicio para manejar productos, obsequios y facturas de venta"""
    
    def assign_invoice_and_products(self, reporte_df, crtmp_df):
        """
        1. Asigna la 'Factura_Venta' a cada crédito según la lógica de la empresa.
        2. Usa la factura encontrada para buscar y agregar detalles como fecha, productos,
        obsequios y sus cantidades desde el archivo de consulta (CRTMP).
        """
        print("🧾 Asignando facturas y enriqueciendo con detalles de productos...")

        # ==========================================================================
        # PARTE 1: ASIGNACIÓN DE LA FACTURA DE VENTA (Lógica que ya tenías)
        # ==========================================================================
        if crtmp_df.empty:
            print("⚠️ Archivo CRTMP no encontrado o vacío. No se pueden asignar facturas ni detalles.")
            reporte_df['Factura_Venta'] = 'NO DISPONIBLE'
            return reporte_df

        reporte_df['Factura_Venta'] = np.nan

        # --- Lógica para FINANSUEÑOS ---
        filtro_fns = reporte_df['Empresa'] == 'FINANSUEÑOS'
        if filtro_fns.any():
            print("   - Procesando facturas de FINANSUEÑOS...")
            crtmp_df_copy = crtmp_df.copy()
            crtmp_df_copy['Fecha_Facturada'] = pd.to_datetime(crtmp_df_copy['Fecha_Facturada'], dayfirst=True, errors='coerce')
            if not crtmp_df_copy['Fecha_Facturada'].isnull().all():
                creditos_fns = crtmp_df_copy[crtmp_df_copy['Credito'].str.startswith('DF', na=False)].copy()
                facturas_fns = crtmp_df_copy[~crtmp_df_copy['Credito'].str.startswith('DF', na=False)].copy()
                merged_df = pd.merge(creditos_fns, facturas_fns, on='Cedula_Cliente', suffixes=('_credito', '_factura'))
                merged_df['dias_diferencia'] = (merged_df['Fecha_Facturada_factura'] - merged_df['Fecha_Facturada_credito']).dt.days.abs()
                coincidencias_validas = merged_df[merged_df['dias_diferencia'] <= 30].copy()
                coincidencias_validas.sort_values(by=['Credito_credito', 'dias_diferencia'], inplace=True)
                mapeo_final = coincidencias_validas.drop_duplicates(subset='Credito_credito', keep='first')
                mapa_facturas_fns = pd.Series(mapeo_final['Credito_factura'].values, index=mapeo_final['Credito_credito']).to_dict()
                reporte_df.loc[filtro_fns, 'Factura_Venta'] = reporte_df.loc[filtro_fns, 'Credito'].map(mapa_facturas_fns)

        # --- Lógica para ARPESOD ---
        print("   - Procesando facturas de ARPESOD...")
        prefijos_busqueda = ['RTC', 'PR', 'NC', 'NT', 'NF']
        filtro_arpesod_especial = (reporte_df['Empresa'] == 'ARPESOD') & (reporte_df['Credito'].str.startswith(tuple(prefijos_busqueda), na=False))
        if filtro_arpesod_especial.any():
            reporte_df['Numero_Busqueda'] = reporte_df['Credito'].str.split('-').str[1].str.strip()
            crtmp_busqueda = crtmp_df[['Cedula_Cliente', 'Tipo_Credito', 'Numero_Credito']].copy()
            crtmp_busqueda['Numero_Credito'] = crtmp_busqueda['Numero_Credito'].astype(str).str.strip()
            crtmp_busqueda['Cedula_Cliente'] = crtmp_busqueda['Cedula_Cliente'].astype(str).str.strip()
            reporte_busqueda = reporte_df[filtro_arpesod_especial][['Credito', 'Cedula_Cliente', 'Numero_Busqueda']].copy()
            reporte_busqueda['Cedula_Cliente'] = reporte_busqueda['Cedula_Cliente'].astype(str).str.strip()
            merged_arpesod = pd.merge(reporte_busqueda, crtmp_busqueda, left_on=['Cedula_Cliente', 'Numero_Busqueda'], right_on=['Cedula_Cliente', 'Numero_Credito'], how='left')
            merged_arpesod['Factura_Encontrada'] = np.where(merged_arpesod['Numero_Credito'].notna(), merged_arpesod['Tipo_Credito'] + '-' + merged_arpesod['Numero_Credito'], np.nan)
            mapa_facturas_arpesod = pd.Series(merged_arpesod['Factura_Encontrada'].values, index=merged_arpesod['Credito']).to_dict()
            reporte_df.loc[filtro_arpesod_especial, 'Factura_Venta'] = reporte_df.loc[filtro_arpesod_especial, 'Credito'].map(mapa_facturas_arpesod)
            reporte_df.drop(columns=['Numero_Busqueda'], inplace=True, errors='ignore')

        filtro_arpesod_normal = (reporte_df['Empresa'] == 'ARPESOD') & (reporte_df['Factura_Venta'].isnull())
        reporte_df.loc[filtro_arpesod_normal, 'Factura_Venta'] = reporte_df['Credito']

        # ==========================================================================
        # PARTE 2: ENRIQUECIMIENTO CON DATOS DE LA FACTURA (NUEVO)
        # ==========================================================================
        print("🎁 Agregando productos, obsequios y detalles de la factura...")

        # 1. Preparar el DataFrame de consulta para el cruce de detalles.
        crtmp_detalles = crtmp_df.copy()
        # Crear la misma llave 'Factura_Venta' que tenemos en el reporte principal.
        crtmp_detalles['Factura_Venta'] = crtmp_detalles['Tipo_Credito'].astype(str) + '-' + crtmp_detalles['Numero_Credito'].astype(str)

        # 2. Clasificar items en Productos vs. Obsequios y preparar el texto.
        # Asumimos que los obsequios tienen un valor de venta de 0 o nulo.
        crtmp_detalles['Total_Venta'] = pd.to_numeric(crtmp_detalles['Total_Venta'], errors='coerce').fillna(0)
        crtmp_detalles['Cantidad_Item'] = pd.to_numeric(crtmp_detalles['Cantidad_Item'], errors='coerce').fillna(1)
        crtmp_detalles['Tipo_Item'] = np.where(crtmp_detalles['Total_Venta'] > 0, 'Producto', 'Obsequio')
        crtmp_detalles['Item_Texto'] = crtmp_detalles['Nombre_Producto'].astype(str) + ' (' + crtmp_detalles['Cantidad_Item'].astype(int).astype(str) + ')'

        # 3. Agrupar todos los items por factura para consolidarlos en una sola fila.
        # Se agrupan productos y obsequios por separado.
        productos = crtmp_detalles[crtmp_detalles['Tipo_Item'] == 'Producto'].groupby('Factura_Venta').agg(
            Productos=('Item_Texto', ', '.join),
            Cantidad_Productos=('Cantidad_Item', 'sum')
        )
        obsequios = crtmp_detalles[crtmp_detalles['Tipo_Item'] == 'Obsequio'].groupby('Factura_Venta').agg(
            Obsequios=('Item_Texto', ', '.join),
            Cantidad_Obsequios=('Cantidad_Item', 'sum')
        )
        # También obtenemos la fecha (solo necesitamos la primera que aparezca por factura).
        fechas = crtmp_detalles.groupby('Factura_Venta').agg(Fecha_Facturada=('Fecha_Facturada', 'first'))

        # 4. Unir todos los detalles agregados en una sola tabla de información.
        info_facturas = fechas.join(productos, how='outer').join(obsequios, how='outer')

        # 5. Cruzar (merge) la información de las facturas con el reporte principal.
        reporte_df = pd.merge(reporte_df, info_facturas, on='Factura_Venta', how='left')

        # ==========================================================================
        # PARTE 3: LIMPIEZA FINAL
        # ==========================================================================
        reporte_df['Factura_Venta'].fillna('NO ASIGNADA', inplace=True)
        reporte_df['Productos'].fillna('NO REGISTRA', inplace=True)
        reporte_df['Obsequios'].fillna('SIN OBSEQUIOS', inplace=True)
        reporte_df['Cantidad_Productos'].fillna(0, inplace=True)
        reporte_df['Cantidad_Obsequios'].fillna(0, inplace=True)

        print("✅ Asignación de facturas y detalles completada.")
        return reporte_df