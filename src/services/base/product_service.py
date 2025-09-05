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
        reporte_df['Factura_Venta'] = reporte_df['Factura_Venta'].astype('object') # Convertir a tipo objeto para aceptar strings

        # --- Lógica para FINANSUEÑOS (sin cambios) ---
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
                facturas_asignadas = reporte_df.loc[filtro_fns, 'Credito'].map(mapa_facturas).astype(str)
                reporte_df.loc[filtro_fns, 'Factura_Venta'] = facturas_asignadas

        # --- Lógica para ARPESOD (CORREGIDA) ---
        print("   - Procesando facturas de ARPESOD...")
        prefijos = ['RTC', 'PR', 'NC', 'NT', 'NF']
        filtro_arp_especial = (reporte_df['Empresa'] == 'ARPESOD') & (reporte_df['Credito'].str.startswith(tuple(prefijos), na=False))
        
        # Paso 1: Procesar los créditos especiales. Si no encuentran factura, su valor quedará vacío (NaN).
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
            facturas_arp_asignadas = reporte_df.loc[filtro_arp_especial, 'Credito'].map(mapa_facturas_arp).astype(str)
            reporte_df.loc[filtro_arp_especial, 'Factura_Venta'] = facturas_arp_asignadas
            reporte_df.drop(columns=['Numero_Busqueda'], inplace=True, errors='ignore')

        # Paso 2: Procesar los créditos normales. Un crédito normal es de ARPESOD y NO es especial.
        filtro_arp_normal = (reporte_df['Empresa'] == 'ARPESOD') & (~filtro_arp_especial)
        reporte_df.loc[filtro_arp_normal, 'Factura_Venta'] = reporte_df['Credito'].astype(str)
        
        # Paso 3: Llenar vacíos restantes. Esto ahora solo afectará a los especiales que no encontraron factura.
        reporte_df['Factura_Venta'] = reporte_df['Factura_Venta'].fillna('NO ASIGNADA')
        
        print("✅ Asignación de facturas de venta completada.")
        return reporte_df

    def add_product_details(self, reporte_df, crtmp_df):
        """
        Usa la columna 'Factura_Venta' para añadir detalles de fecha, correo,
        productos y obsequios.
        Esta función ahora maneja tanto la creación inicial como la actualización.
        """
        print("🎁 Agregando detalles completos de la factura...")

        if crtmp_df.empty:
            # Asignar valores por defecto si no hay datos de detalle
            cols_defecto = {
                'Fecha_Facturada': 'NO DISPONIBLE', 'Correo': 'NO DISPONIBLE',
                'Nombre_Producto': 'NO DISPONIBLE', 'Cantidad_Producto': 0,
                'Obsequio': 'NO DISPONIBLE', 'Cantidad_Obsequio': 0
            }
            for col, value in cols_defecto.items():
                if col not in reporte_df.columns:
                    reporte_df[col] = value
            return reporte_df

        # --- PREPARACIÓN DE DATOS (sin cambios) ---
        crtmp_detalles = crtmp_df.copy()
        crtmp_detalles['Factura_Venta'] = crtmp_detalles['Tipo_Credito'].astype(str) + '-' + crtmp_detalles['Numero_Credito'].astype(str)
        crtmp_detalles['Nombre_Producto'] = crtmp_detalles['Nombre_Producto'].fillna('PRODUCTO NO REGISTRADO')
        crtmp_detalles['Cantidad_Item'] = crtmp_detalles['Cantidad_Item'].fillna(1)
        crtmp_detalles['Total_Venta'] = pd.to_numeric(crtmp_detalles['Total_Venta'], errors='coerce').fillna(0)
        es_obsequio = (crtmp_detalles['Total_Venta'] >= 1000) & (crtmp_detalles['Total_Venta'] <= 6000)
        crtmp_detalles['Tipo_Item'] = np.where(es_obsequio, 'Obsequio', 'Producto')
        crtmp_detalles['Item_Texto'] = crtmp_detalles['Nombre_Producto'].astype(str) + ' (' + crtmp_detalles['Cantidad_Item'].astype(int).astype(str) + ')'

        productos = crtmp_detalles[crtmp_detalles['Tipo_Item'] == 'Producto'].groupby('Factura_Venta').agg(
            Nombre_Producto=('Item_Texto', ', '.join), Cantidad_Producto=('Cantidad_Item', 'sum'))
        obsequios = crtmp_detalles[crtmp_detalles['Tipo_Item'] == 'Obsequio'].groupby('Factura_Venta').agg(
            Obsequio=('Item_Texto', ', '.join), Cantidad_Obsequio=('Cantidad_Item', 'sum'))
        detalles_adicionales = crtmp_detalles.groupby('Factura_Venta').agg(
            Fecha_Facturada=('Fecha_Facturada', 'first'), Correo=('Correo', 'first'))

        info_facturas = detalles_adicionales.join(productos, how='outer').join(obsequios, how='outer')

        # --- LÓGICA DE DETECCIÓN INTELIGENTE ---
        
        # 1. Antes de unir, identificamos las columnas que podrían causar conflicto.
        conflicting_cols = [col for col in info_facturas.columns if col in reporte_df.columns]
        
        # 2. Si hay columnas en conflicto, las eliminamos del reporte principal ANTES de unir.
        #    Esto asegura que la información nueva siempre reemplace a la vieja.
        if conflicting_cols:
            print(f"[LOG] Modo Actualización detectado. Eliminando columnas viejas: {conflicting_cols}")
            reporte_df = reporte_df.drop(columns=conflicting_cols)

        # 3. Realizamos la unión. Ahora NUNCA habrá conflicto de nombres.
        reporte_df = pd.merge(reporte_df, info_facturas, on='Factura_Venta', how='left')

        # 4. Limpieza final. Este bloque ahora funciona para AMBOS casos.
        print("[LOG] Realizando limpieza final de columnas de producto...")
        reporte_df['Nombre_Producto'] = reporte_df['Nombre_Producto'].fillna('NO REGISTRA')
        reporte_df['Obsequio'] = reporte_df['Obsequio'].fillna('SIN OBSEQUIOS')
        reporte_df['Cantidad_Producto'] = reporte_df['Cantidad_Producto'].fillna(0).astype(int)
        reporte_df['Cantidad_Obsequio'] = reporte_df['Cantidad_Obsequio'].fillna(0).astype(int)
        reporte_df['Correo'] = reporte_df['Correo'].fillna('NO REGISTRA')
        reporte_df['Fecha_Facturada'] = reporte_df['Fecha_Facturada'].fillna('NO DISPONIBLE')

        print("✅ Detalles completos de la factura agregados.")
        return reporte_df