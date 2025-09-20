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
        
        # Hacer una copia para evitar warnings
        df_analisis_cartera = df_analisis_cartera.copy()
        
        # Primero, asegurar que las columnas a sumar sean numéricas
        columnas_numericas = ['Meta_$', 'Recaudo_Meta', 'Total_Recaudo', 'Meta_T.R_$']
        for col in columnas_numericas:
            if col in df_analisis_cartera.columns:
                # errors='coerce' convierte texto no numérico en Nulo (NaN)
                df_analisis_cartera[col] = pd.to_numeric(df_analisis_cartera[col], errors='coerce').fillna(0)

        # Segundo, limpiar las columnas de texto que usaremos para agrupar
        columnas_texto = ['Zona', 'Regional_Cobro', 'Franja_Meta']
        for col in columnas_texto:
            if col in df_analisis_cartera.columns:
                df_analisis_cartera[col] = df_analisis_cartera[col].astype(str).str.strip().str.lower()
        
        # --- Paso 2: Filtrar solo las franjas relevantes para el pivot ---
        print("🔍 Filtrando franjas de mora para el reporte...")
        franjas_validas = ['1 a 30', '31 a 90', '91 a 180', '181 a 360']
        df_filtrado = df_analisis_cartera[df_analisis_cartera['Franja_Meta'].isin(franjas_validas)].copy()

        # --- Paso 3: Calcular totales por Zona (incluyendo 'al día') ---
        print("🧮 Calculando totales por Zona (incluyendo 'al día')...")
        
        # Verificar que las columnas necesarias existen
        if 'Total_Recaudo' not in df_analisis_cartera.columns:
            df_analisis_cartera['Total_Recaudo'] = 0
        if 'Meta_T.R_$' not in df_analisis_cartera.columns:
            df_analisis_cartera['Meta_T.R_$'] = 0
            
        # Crear un DataFrame con todos los datos (incluyendo 'al día') para calcular totales por Zona
        df_totales_zona = df_analisis_cartera.groupby(['Zona']).agg({
            'Total_Recaudo': 'sum',
            'Meta_T.R_$': 'sum'
        }).reset_index()

        # --- Paso 4: Agrupar y Sumarizar por franjas ---
        print("📊 Agrupando datos por franjas...")
        
        # Verificar que las columnas necesarias existen
        if 'Meta_$' not in df_filtrado.columns:
            df_filtrado['Meta_$'] = 0
        if 'Recaudo_Meta' not in df_filtrado.columns:
            df_filtrado['Recaudo_Meta'] = 0
            
        df_agrupado = df_filtrado.groupby(['Zona', 'Regional_Cobro', 'Franja_Meta']).agg({
            'Meta_$': 'sum',
            'Recaudo_Meta': 'sum'
        }).reset_index()

        # --- Paso 5: Pivotar la Tabla ---
        print("🔄 Pivotando tabla...")
        try:
            df_pivot = df_agrupado.pivot_table(
                index=['Zona', 'Regional_Cobro'], 
                columns='Franja_Meta',
                values=['Meta_$', 'Recaudo_Meta'],
                aggfunc='sum',
                fill_value=0
            )
            
            # Aplanar las columnas multi-index
            df_pivot.columns = [f'{val}_{franja}' for val, franja in df_pivot.columns]
            df_pivot = df_pivot.reset_index()
        except Exception as e:
            print(f"❌ Error al pivotar: {e}")
            # Crear un DataFrame vacío con la estructura esperada
            df_pivot = pd.DataFrame(columns=['Zona', 'Regional_Cobro'])
            for franja in franjas_validas:
                df_pivot[f'Meta_$_{franja}'] = 0
                df_pivot[f'Recaudo_Meta_{franja}'] = 0

        # --- Paso 6: Crear la Estructura del DataFrame Final ---
        print("🏗️ Creando estructura del reporte final...")
        header = [
            ('ZONA', ''), ('REGIONAL', ''),
            ('1 A 30', 'META_$'), ('1 A 30', 'Recaudo_Meta'), ('1 A 30', 'Cumplimiento_%'),
            ('31 A 90', 'META_$'), ('31 A 90', 'Recaudo_Meta'), ('31 A 90', 'Cumplimiento_%'),
            ('91 A 180', 'META_$'), ('91 A 180', 'Recaudo_Meta'), ('91 A 180', 'Cumplimiento_%'),
            ('181 A 360', 'META_$'), ('181 A 360', 'Recaudo_Meta'), ('181 A 360', 'Cumplimiento_%'),
            ('Total_Recaudo', ''), ('Recaudo_Anticipo', '')
        ]
        
        df_franjas = pd.DataFrame(columns=pd.MultiIndex.from_tuples(header))

        # --- Paso 7: Llenar el DataFrame ---
        if not df_pivot.empty and 'Zona' in df_pivot.columns:
            df_franjas[('ZONA', '')] = df_pivot['Zona']
            df_franjas[('REGIONAL', '')] = df_pivot['Regional_Cobro']  
        franjas_map = {
            '1 A 30': '1 a 30', 
            '31 A 90': '31 a 90',
            '91 A 180': '91 a 180', 
            '181 A 360': '181 a 360'
        }
        
        for franja_reporte, franja_pivot in franjas_map.items():
            meta_col = f'Meta_$_{franja_pivot}'
            recaudo_col = f'Recaudo_Meta_{franja_pivot}'
            
            if meta_col in df_pivot.columns:
                df_franjas[(franja_reporte, 'META_$')] = df_pivot[meta_col]
            else:
                df_franjas[(franja_reporte, 'META_$')] = 0
                
            if recaudo_col in df_pivot.columns:
                df_franjas[(franja_reporte, 'Recaudo_Meta')] = df_pivot[recaudo_col]
            else:
                df_franjas[(franja_reporte, 'Recaudo_Meta')] = 0
                
        df_franjas = df_franjas.fillna(0)

        for franja in franjas_map.keys():
            meta = df_franjas[(franja, 'META_$')]
            recaudo = df_franjas[(franja, 'Recaudo_Meta')]
            porcentaje_decimal = np.where(meta != 0, recaudo / meta, 0)
            df_franjas[(franja, 'Cumplimiento_%')] = [
                f"{round(x * 100)}%" if pd.notnull(x) else "0%" for x in porcentaje_decimal
            ]

        # --- PASO 8: CALCULAR TOTALES POR ZONA (VERSIÓN CORREGIDA) ---
        print("📊 Calculando totales por Zona con la lógica correcta...")
        
        # IMPORTANTE: Usamos df_filtrado para que los totales coincidan con los datos de las franjas.
        # Agrupamos por ZONA y sumamos 'Recaudo_Meta' y 'Recaudo_Anticipado'.
        df_totales_zona = df_filtrado.groupby('Zona').agg(
            Suma_Recaudo_Meta=('Recaudo_Meta', 'sum'),
            Suma_Recaudo_Anticipado=('Recaudo_Anticipado', 'sum')
        ).reset_index()

        # --- PASO 9: ASIGNAR LOS TOTALES CORRECTOS (VERSIÓN CORREGIDA) ---
        if not df_franjas.empty and not df_totales_zona.empty:
            # Mapeo para la suma de 'Recaudo_Meta'
            total_recaudo_map = df_totales_zona.set_index('Zona')['Suma_Recaudo_Meta'].to_dict()
            
            # Mapeo para la suma de 'Recaudo_Anticipado'
            recaudo_anticipo_map = df_totales_zona.set_index('Zona')['Suma_Recaudo_Anticipado'].to_dict()
            
            # Asignar los valores correctos a las columnas del reporte
            df_franjas[('Total_Recaudo', '')] = df_franjas[('ZONA', '')].map(total_recaudo_map).fillna(0)
            df_franjas[('Recaudo_Anticipo', '')] = df_franjas[('ZONA', '')].map(recaudo_anticipo_map).fillna(0)
        else:
            df_franjas[('Total_Recaudo', '')] = 0
            df_franjas[('Recaudo_Anticipo', '')] = 0
        
        df_franjas = df_franjas.fillna(0)
        
        # Ordenar por Zona para facilitar el formato en Excel
        df_franjas = df_franjas.sort_values(by=[('ZONA', ''), ('REGIONAL', '')]).reset_index(drop=True)

        print("✅ Reporte de franjas generado.")
        return df_franjas