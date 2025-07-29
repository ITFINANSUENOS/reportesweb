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
        - Para FINANSUEÑOS, busca la factura por proximidad de fecha.
        - Para ARPESOD, diferencia entre créditos normales y especiales.
            - Créditos especiales (RTC, PR, NC, NT, NF): Busca la factura en CRTMPCONSULTA1
            coincidiendo por número de crédito y cédula.
            - Créditos normales: La factura es el mismo número de crédito.
        """
        print("🧾 Asignando facturas de venta...")

        # --- Verificación inicial del DataFrame de búsqueda ---
        if crtmp_df.empty:
            print("⚠️ Archivo CRTMPCONSULTA1 no encontrado o vacío. No se pueden asignar facturas para FINANSUEÑOS ni para créditos especiales de ARPESOD.")
            # Asigna 'NO DISPONIBLE' a todos y 'Credito' a ARPESOD como fallback.
            reporte_df['Factura_Venta'] = np.where(reporte_df['Empresa'] == 'ARPESOD', reporte_df['Credito'], 'NO DISPONIBLE')
            return reporte_df

        # Inicializar la columna de facturas
        reporte_df['Factura_Venta'] = np.nan

        # --- Lógica para FINANSUEÑOS (sin cambios) ---
        filtro_fns = reporte_df['Empresa'] == 'FINANSUEÑOS'
        if filtro_fns.any():
            print("   - Procesando FINANSUEÑOS...")
            crtmp_df_copy = crtmp_df.copy()
            crtmp_df_copy['Fecha_Facturada'] = pd.to_datetime(crtmp_df_copy['Fecha_Facturada'], dayfirst=True, errors='coerce')

            if crtmp_df_copy['Fecha_Facturada'].isnull().all():
                print("❌ Error crítico: No se pudo interpretar ninguna fecha en CRTMPCONSULTA1. Verifique el formato.")
                reporte_df.loc[filtro_fns, 'Factura_Venta'] = 'ERROR DE FECHA'
            else:
                creditos_fns = crtmp_df_copy[crtmp_df_copy['Credito'].str.startswith('DF', na=False)].copy()
                facturas_fns = crtmp_df_copy[~crtmp_df_copy['Credito'].str.startswith('DF', na=False)].copy()
                merged_df = pd.merge(creditos_fns, facturas_fns, on='Cedula_Cliente', suffixes=('_credito', '_factura'))
                merged_df['dias_diferencia'] = (merged_df['Fecha_Facturada_factura'] - merged_df['Fecha_Facturada_credito']).dt.days.abs()
                coincidencias_validas = merged_df[merged_df['dias_diferencia'] <= 30].copy()
                coincidencias_validas.sort_values(by=['Credito_credito', 'dias_diferencia'], inplace=True)
                mapeo_final = coincidencias_validas.drop_duplicates(subset='Credito_credito', keep='first')
                mapa_facturas_fns = pd.Series(mapeo_final['Credito_factura'].values, index=mapeo_final['Credito_credito']).to_dict()
                reporte_df.loc[filtro_fns, 'Factura_Venta'] = reporte_df.loc[filtro_fns, 'Credito'].map(mapa_facturas_fns)

        # --- Lógica avanzada para ARPESOD (créditos especiales) ---
        print("   - Procesando ARPESOD...")
        prefijos_busqueda = ['RTC', 'PR', 'NC', 'NT', 'NF']
        filtro_arpesod_especial = (reporte_df['Empresa'] == 'ARPESOD') & \
                                (reporte_df['Credito'].str.startswith(tuple(prefijos_busqueda), na=False))

        if filtro_arpesod_especial.any():
            print(f"      -> Encontrados {filtro_arpesod_especial.sum()} créditos especiales de ARPESOD para buscar.")
            # 1. Preparar datos para el cruce
            # Extraemos el número del crédito (ej. 'RTC-12576' -> '12576')
            reporte_df['Numero_Busqueda'] = reporte_df['Credito'].str.split('-').str[1].str.strip()
            
            # Copia de crtmp_df para no modificar el original, asegurando tipos de datos
            crtmp_busqueda = crtmp_df[['Cedula_Cliente', 'Tipo_Credito', 'Numero_Credito']].copy()
            crtmp_busqueda['Numero_Credito'] = crtmp_busqueda['Numero_Credito'].astype(str).str.strip()
            crtmp_busqueda['Cedula_Cliente'] = crtmp_busqueda['Cedula_Cliente'].astype(str).str.strip()
            
            # Preparar el reporte base para el merge
            reporte_busqueda = reporte_df[filtro_arpesod_especial][['Credito', 'Cedula_Cliente', 'Numero_Busqueda']].copy()
            reporte_busqueda['Cedula_Cliente'] = reporte_busqueda['Cedula_Cliente'].astype(str).str.strip()

            # 2. Cruzar (merge) los créditos especiales con la data de CRTMP
            # Se cruza usando la cédula y el número de crédito extraído
            merged_arpesod = pd.merge(
                reporte_busqueda,
                crtmp_busqueda,
                left_on=['Cedula_Cliente', 'Numero_Busqueda'],
                right_on=['Cedula_Cliente', 'Numero_Credito'],
                how='left'
            )

            # 3. Construir la factura encontrada (ej. 'FF03-12576')
            # Usamos np.where para manejar los casos que no encontraron correspondencia
            merged_arpesod['Factura_Encontrada'] = np.where(
                merged_arpesod['Numero_Credito'].notna(),
                merged_arpesod['Tipo_Credito'] + '-' + merged_arpesod['Numero_Credito'],
                np.nan # Dejar como NaN si no se encontró
            )

            # 4. Crear un mapa (diccionario) para la asignación final: {Credito: Factura_Encontrada}
            mapa_facturas_arpesod = pd.Series(
                merged_arpesod['Factura_Encontrada'].values,
                index=merged_arpesod['Credito']
            ).to_dict()

            # 5. Asignar los valores al reporte final usando el mapa
            reporte_df.loc[filtro_arpesod_especial, 'Factura_Venta'] = reporte_df.loc[filtro_arpesod_especial, 'Credito'].map(mapa_facturas_arpesod)
            
            # Limpiar la columna temporal
            reporte_df.drop(columns=['Numero_Busqueda'], inplace=True)

        # --- Lógica para ARPESOD (créditos normales) ---
        # Aquellos de ARPESOD que NO son especiales y aún no tienen factura asignada
        filtro_arpesod_normal = (reporte_df['Empresa'] == 'ARPESOD') & (reporte_df['Factura_Venta'].isnull())
        reporte_df.loc[filtro_arpesod_normal, 'Factura_Venta'] = reporte_df['Credito']

        # --- Rellenar valores no encontrados para todas las empresas ---
        reporte_df['Factura_Venta'].fillna('NO ASIGNADA', inplace=True)
        
        print("✅ Asignación de facturas de venta completada.")
        return reporte_df