import pandas as pd
import numpy as np

class ReporteFranjasService:
    def generar_reporte(self, df_analisis_cartera):
        """
        Genera un reporte de franjas consolidado por Zona y Regional,
        con una estructura de columnas detallada.
        """
        print("🔄 Generando reporte de franjas...")

        # --- Paso 1: Limpieza y Aseguramiento de Tipos de Datos ---
        print("🧹 Limpiando, estandarizando y asegurando tipos de datos...")
        df_analisis_cartera = df_analisis_cartera.copy()
        
        columnas_numericas = ['Meta_$', 'Recaudo_Meta', 'Total_Recaudo', 'Meta_T.R_$', 'Recaudo_Anticipado', 'Total_Recaudo_Sin_Anti']
        for col in columnas_numericas:
            if col in df_analisis_cartera.columns:
                df_analisis_cartera[col] = pd.to_numeric(df_analisis_cartera[col], errors='coerce').fillna(0)

        columnas_texto = ['Zona', 'Regional_Cobro', 'Franja_Meta']
        for col in columnas_texto:
            if col in df_analisis_cartera.columns:
                df_analisis_cartera[col] = df_analisis_cartera[col].astype(str).str.strip().str.lower()
        
        # --- Paso 2: Filtrar solo las franjas relevantes ---
        print("🔍 Filtrando franjas de mora para el reporte...")
        franjas_validas = ['1 a 30', '31 a 90', '91 a 180', '181 a 360']
        df_filtrado = df_analisis_cartera[df_analisis_cartera['Franja_Meta'].isin(franjas_validas)].copy()

        # --- Paso 3: Agrupar y Sumarizar por franjas ---
        print("📊 Agrupando datos por franjas...")
        df_agrupado = df_filtrado.groupby(['Zona', 'Regional_Cobro', 'Franja_Meta']).agg({
            'Meta_$': 'sum',
            'Recaudo_Meta': 'sum'
        }).reset_index()

        # --- Paso 4: Pivotar la Tabla ---
        print("🔄 Pivotando tabla...")
        try:
            df_pivot = df_agrupado.pivot_table(
                index=['Zona', 'Regional_Cobro'], 
                columns='Franja_Meta',
                values=['Meta_$', 'Recaudo_Meta'],
                aggfunc='sum',
                fill_value=0
            )
            df_pivot.columns = [f'{val}_{franja}' for val, franja in df_pivot.columns]
            df_pivot = df_pivot.reset_index()
        except Exception as e:
            print(f"❌ Error al pivotar: {e}")
            df_pivot = pd.DataFrame(columns=['Zona', 'Regional_Cobro'])
            for franja in franjas_validas:
                df_pivot[f'Meta_$_{franja}'] = 0
                df_pivot[f'Recaudo_Meta_{franja}'] = 0

        # --- Paso 5: Crear la Estructura del DataFrame Final ---
        print("🏗️ Creando estructura del reporte final...")
        header = [
            ('ZONA', ''), ('REGIONAL', ''),
            ('1 A 30', 'META_$'), ('1 A 30', 'Recaudo_Meta'), ('1 A 30', 'Cumplimiento_%'),
            ('31 A 90', 'META_$'), ('31 A 90', 'Recaudo_Meta'), ('31 A 90', 'Cumplimiento_%'),
            ('91 A 180', 'META_$'), ('91 A 180', 'Recaudo_Meta'), ('91 A 180', 'Cumplimiento_%'),
            ('181 A 360', 'META_$'), ('181 A 360', 'Recaudo_Meta'), ('181 A 360', 'Cumplimiento_%'),
            ('Total_Recaudo', ''), ('Total_Recaudo_Sin_Anti', ''), ('Recaudo_Anticipo', '')
        ]
        df_franjas = pd.DataFrame(columns=pd.MultiIndex.from_tuples(header))

        # --- Paso 6: Llenar el DataFrame con datos de franjas ---
        if not df_pivot.empty and 'Zona' in df_pivot.columns:
            df_franjas[('ZONA', '')] = df_pivot['Zona']
            df_franjas[('REGIONAL', '')] = df_pivot['Regional_Cobro']
            
        franjas_map = {
            '1 A 30': '1 a 30', '31 A 90': '31 a 90',
            '91 A 180': '91 a 180', '181 A 360': '181 a 360'
        }
        for franja_reporte, franja_pivot in franjas_map.items():
            meta_col, recaudo_col = f'Meta_$_{franja_pivot}', f'Recaudo_Meta_{franja_pivot}'
            df_franjas[(franja_reporte, 'META_$')] = df_pivot.get(meta_col, 0)
            df_franjas[(franja_reporte, 'Recaudo_Meta')] = df_pivot.get(recaudo_col, 0)
            
        df_franjas = df_franjas.fillna(0)
        
        for franja in franjas_map.keys():
            meta = df_franjas[(franja, 'META_$')]
            recaudo = df_franjas[(franja, 'Recaudo_Meta')]
            porcentaje_decimal = np.where(meta != 0, recaudo / meta, 0)
            # --- LÍNEA MODIFICADA ---
            df_franjas[(franja, 'Cumplimiento_%')] = [f"{round(x * 100, 2)}%".replace('.', ',') for x in porcentaje_decimal]

        # --- PASO 7: CALCULAR TOTALES POR ZONA ---
        print("📊 Calculando y asignando totales por Zona...")
        df_totales_zona = df_filtrado.groupby('Zona').agg(
            Suma_Total_Recaudo=('Total_Recaudo', 'sum'),
            Suma_Total_Recaudo_Sin_Anti=('Total_Recaudo_Sin_Anti', 'sum'),
            Suma_Recaudo_Anticipado=('Recaudo_Anticipado', 'sum')
        ).reset_index()

        # --- PASO 8: ASIGNAR LOS TOTALES ---
        if not df_franjas.empty and not df_totales_zona.empty:
            total_recaudo_map = df_totales_zona.set_index('Zona')['Suma_Total_Recaudo'].to_dict()
            total_sin_anti_map = df_totales_zona.set_index('Zona')['Suma_Total_Recaudo_Sin_Anti'].to_dict()
            recaudo_anticipo_map = df_totales_zona.set_index('Zona')['Suma_Recaudo_Anticipado'].to_dict()
            
            df_franjas[('Total_Recaudo', '')] = df_franjas[('ZONA', '')].map(total_recaudo_map).fillna(0)
            df_franjas[('Total_Recaudo_Sin_Anti', '')] = df_franjas[('ZONA', '')].map(total_sin_anti_map).fillna(0)
            df_franjas[('Recaudo_Anticipo', '')] = df_franjas[('ZONA', '')].map(recaudo_anticipo_map).fillna(0)
        else:
            df_franjas[('Total_Recaudo', '')] = 0
            df_franjas[('Total_Recaudo_Sin_Anti', '')] = 0
            df_franjas[('Recaudo_Anticipo', '')] = 0
        df_franjas = df_franjas.fillna(0)
        df_franjas = df_franjas.sort_values(by=[('ZONA', ''), ('REGIONAL', '')]).reset_index(drop=True)

        # --- PASO 9: FORMATEAR COLUMNAS DE MONEDA ---
        print("💰 Aplicando formato de moneda...")
        
        # Lista de columnas que se deben formatear como moneda
        columnas_moneda = [
            ('1 A 30', 'META_$'), ('1 A 30', 'Recaudo_Meta'),
            ('31 A 90', 'META_$'), ('31 A 90', 'Recaudo_Meta'),
            ('91 A 180', 'META_$'), ('91 A 180', 'Recaudo_Meta'),
            ('181 A 360', 'META_$'), ('181 A 360', 'Recaudo_Meta'),
            ('Total_Recaudo', ''), ('Total_Recaudo_Sin_Anti', ''), ('Recaudo_Anticipo', '')
        ]

        # Se itera sobre las columnas para aplicar el formato
        for col in columnas_moneda:
            if col in df_franjas.columns:
                # Se asegura de que la columna sea numérica antes de formatear
                df_franjas[col] = pd.to_numeric(df_franjas[col], errors='coerce').fillna(0)
                # Se aplica el formato con '$', separadores de miles con '.' y sin decimales
                df_franjas[col] = df_franjas[col].apply(lambda x: f"$ {int(round(x, 0)):,}".replace(',', '.'))

        print("✅ Reporte de franjas generado.")
        return df_franjas
