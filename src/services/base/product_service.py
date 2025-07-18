import pandas as pd
import numpy as np

class ProductsSalesService:
    """Servicio para manejar productos, obsequios y facturas de venta"""
    
    def add_products_and_gifts(self, reporte_df, crtmp_df):
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
        # Limpiar datos de venta y cantidad para los cálculos
        df_items['Total_Venta'] = pd.to_numeric(df_items['Total_Venta'], errors='coerce')
        df_items['Cantidad_Item'] = pd.to_numeric(df_items['Cantidad_Item'], errors='coerce').fillna(0)

        def join_unique(series):
            items = series.dropna().astype(str).unique()
            return ', '.join(items) if len(items) > 0 else 'NO APLICA'

        # 1. Crear mapas de nombres y cantidades desde CRTMP
        # La llave de estos mapas es el ID del documento ('Credito' o 'Factura_Venta')
        es_producto = df_items['Total_Venta'] > 6000
        es_obsequio = df_items['Total_Venta'] <= 6000

        mapa_nombres_productos = df_items[es_producto].groupby('Credito')['Nombre_Producto'].apply(join_unique)
        mapa_nombres_obsequios = df_items[es_obsequio].groupby('Credito')['Nombre_Producto'].apply(join_unique)
        
        mapa_cantidad_productos = df_items[es_producto].groupby('Credito')['Cantidad_Item'].sum()
        mapa_cantidad_obsequios = df_items[es_obsequio].groupby('Credito')['Cantidad_Item'].sum()

        # 2. Asignar los valores al reporte final usando la llave correcta por empresa
        es_arpesod = reporte_df['Empresa'] == 'ARPESOD'
        es_finansuenos = reporte_df['Empresa'] == 'FINANSUEÑOS'

        # Asignar nombres
        reporte_df.loc[es_arpesod, 'Nombre_Producto'] = reporte_df.loc[es_arpesod, 'Credito'].map(mapa_nombres_productos)
        reporte_df.loc[es_arpesod, 'Obsequio'] = reporte_df.loc[es_arpesod, 'Credito'].map(mapa_nombres_obsequios)
        reporte_df.loc[es_finansuenos, 'Nombre_Producto'] = reporte_df.loc[es_finansuenos, 'Factura_Venta'].map(mapa_nombres_productos)
        reporte_df.loc[es_finansuenos, 'Obsequio'] = reporte_df.loc[es_finansuenos, 'Factura_Venta'].map(mapa_nombres_obsequios)
        
        # Asignar cantidades
        reporte_df.loc[es_arpesod, 'Cantidad_Producto'] = reporte_df.loc[es_arpesod, 'Credito'].map(mapa_cantidad_productos)
        reporte_df.loc[es_arpesod, 'Cantidad_Obsequio'] = reporte_df.loc[es_arpesod, 'Credito'].map(mapa_cantidad_obsequios)
        reporte_df.loc[es_finansuenos, 'Cantidad_Producto'] = reporte_df.loc[es_finansuenos, 'Factura_Venta'].map(mapa_cantidad_productos)
        reporte_df.loc[es_finansuenos, 'Cantidad_Obsequio'] = reporte_df.loc[es_finansuenos, 'Factura_Venta'].map(mapa_cantidad_obsequios)
        
        # 3. Rellenar vacíos y calcular el total
        reporte_df['Nombre_Producto'].fillna('NO APLICA', inplace=True)
        reporte_df['Obsequio'].fillna('NO APLICA', inplace=True)
        reporte_df['Cantidad_Producto'].fillna(0, inplace=True)
        reporte_df['Cantidad_Obsequio'].fillna(0, inplace=True)
        
        # Convertir a enteros para que no se muestren decimales (ej. 1.0)
        reporte_df['Cantidad_Producto'] = reporte_df['Cantidad_Producto'].astype(int)
        reporte_df['Cantidad_Obsequio'] = reporte_df['Cantidad_Obsequio'].astype(int)
        
        # Calcular la cantidad total
        reporte_df['Cantidad_Total_Producto'] = reporte_df['Cantidad_Producto'] + reporte_df['Cantidad_Obsequio']
        
        print("✅ Productos, obsequios y cantidades asignados correctamente.")
        return reporte_df

    def assign_sales_invoice(self, reporte_df, crtmp_df):
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