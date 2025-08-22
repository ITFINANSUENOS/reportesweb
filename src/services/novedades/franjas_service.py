import pandas as pd

class ReporteFranjasService:
    def generar_reporte(self, df_analisis_cartera):
        """
        Genera un reporte de franjas consolidado por Zona y Regional,
        con una estructura de columnas detallada.
        """
        print("🔄 Generando reporte de franjas...")

        # --- Paso 1: Limpieza y Aseguramiento de Tipos de Datos (Versión Final) ---
        print("🧹 Limpiando, estandarizando y asegurando tipos de datos...")
        
        # Primero, asegurar que las columnas a sumar sean numéricas
        columnas_numericas = ['Meta_$', 'Recaudo_Meta', 'Total_Recaudo', 'Recaudo_Anticipado']
        for col in columnas_numericas:
            if col in df_analisis_cartera.columns:
                # errors='coerce' convierte texto no numérico en Nulo (NaN)
                df_analisis_cartera[col] = pd.to_numeric(df_analisis_cartera[col], errors='coerce').fillna(0)

        # Segundo, limpiar las columnas de texto que usaremos para agrupar
        columnas_texto = ['Zona', 'Regional_Venta', 'Franja_Mora']
        for col in columnas_texto:
            if col in df_analisis_cartera.columns:
                df_analisis_cartera[col] = df_analisis_cartera[col].astype(str).str.strip().str.lower()
        
        # --- Paso 2: Filtrar solo las franjas relevantes ---
        print("🔍 Filtrando franjas de mora para el reporte...")
        franjas_validas = ['1 a 30', '31 a 90', '91 a 180', '181 a 360']
        df_filtrado = df_analisis_cartera[df_analisis_cartera['Franja_Mora'].isin(franjas_validas)].copy()

        # --- El resto del código permanece igual, ya que ahora opera sobre datos limpios ---

        # Paso 3: Agrupar y Sumarizar
        df_agrupado = df_filtrado.groupby(['Zona', 'Regional_Venta', 'Franja_Mora']).agg({
            'Meta_$': 'sum',
            'Recaudo_Meta': 'sum'
        }).reset_index()

        # Paso 4: Pivotar la Tabla
        df_pivot = df_agrupado.pivot_table(
            index=['Zona', 'Regional_Venta'], columns='Franja_Mora',
            values=['Meta_$', 'Recaudo_Meta']
        ).fillna(0)
        df_pivot.columns = [f'{val}_{franja}' for val, franja in df_pivot.columns]
        df_pivot = df_pivot.reset_index()

        # Paso 5: Crear la Estructura del DataFrame Final
        header = [
            ('ZONA', ''), ('REGIONAL', ''),
            ('1 A 30', 'META_$'), ('1 A 30', 'Recaudo_Meta'), ('1 A 30', 'Cumplimiento_%'),
            ('31 A 90', 'META_$'), ('31 A 90', 'Recaudo_Meta'), ('31 A 90', 'Cumplimiento_%'),
            ('91 A 180', 'META_$'), ('91 A 180', 'Recaudo_Meta'), ('91 A 180', 'Cumplimiento_%'),
            ('181 A 360', 'META_$'), ('181 A 360', 'Recaudo_Meta'), ('181 A 360', 'Cumplimiento_%'),
            ('Total_Recaudo', ''), ('Recaudo_Anticipo', '')
        ]
        df_franjas = pd.DataFrame(columns=pd.MultiIndex.from_tuples(header))

        # Paso 6: Llenar el DataFrame
        if not df_pivot.empty:
            df_franjas[('ZONA', '')] = df_pivot['Zona']
            df_franjas[('REGIONAL', '')] = df_pivot['Regional_Venta']
        
        franjas_map = {
            '1 A 30': '1 a 30', '31 A 90': '31 a 90',
            '91 A 180': '91 a 180', '181 A 360': '181 a 360'
        }
        for franja_reporte, franja_pivot in franjas_map.items():
            meta_col = f'Meta_$_{franja_pivot}'
            recaudo_col = f'Recaudo_Meta_{franja_pivot}'
            if meta_col in df_pivot.columns:
                df_franjas[(franja_reporte, 'META_$')] = df_pivot[meta_col]
            if recaudo_col in df_pivot.columns:
                df_franjas[(franja_reporte, 'Recaudo_Meta')] = df_pivot[recaudo_col]
        df_franjas = df_franjas.fillna(0)

        # Paso 7: Calcular el Cumplimiento y formatear como porcentaje
        for franja in franjas_map.keys():
            meta = df_franjas[(franja, 'META_$')]
            recaudo = df_franjas[(franja, 'Recaudo_Meta')]
            
            # Calcular el porcentaje (valor decimal)
            porcentaje_decimal = (recaudo / meta).where(meta != 0, 0)
            
            # Formatear como string con el símbolo '%' y sin decimales
            df_franjas[(franja, 'Cumplimiento_%')] = porcentaje_decimal.apply(
                lambda x: f"{round(x * 100)}%" if pd.notnull(x) else "0%"
            )

        # Paso 8: Asignar los Totales
        df_totales = df_filtrado.groupby(['Zona', 'Regional_Venta']).agg(
            Total_Recaudo=('Total_Recaudo', 'sum'),
            Recaudo_Anticipado=('Recaudo_Anticipado', 'sum')
        )
        if not df_franjas.empty:
            idx_franjas = pd.MultiIndex.from_frame(df_franjas[[('ZONA', ''), ('REGIONAL', '')]])
            df_franjas[('Total_Recaudo', '')] = idx_franjas.map(df_totales['Total_Recaudo'])
            df_franjas[('Recaudo_Anticipo', '')] = idx_franjas.map(df_totales['Recaudo_Anticipado'])
        df_franjas = df_franjas.fillna(0)

        print("✅ Reporte de franjas generado.")
        return df_franjas