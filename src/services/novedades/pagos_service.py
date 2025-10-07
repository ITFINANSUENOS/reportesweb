import pandas as pd
import numpy as np

class PagosService:
    def generar_reporte_pagos(self, df_analisis_cartera):
        """
        Genera un reporte de pagos por franjas, consolidado por Zona y Regional,
        incluyendo totales de recaudo y metas, y una fila de resumen por Gestor.
        """
        print("🔄 Generando reporte de pagos con totales TR...")

        # --- Paso 1: Limpieza y Aseguramiento de Tipos de Datos ---
        print("🧹 Limpiando datos para el reporte de pagos...")
        df_analisis_cartera = df_analisis_cartera.copy()
        
        # --- MODIFICADO: Añadimos las nuevas columnas a la lista de numéricas ---
        columnas_numericas = ['Meta_$', 'Recaudo_Meta', 'Total_Recaudo_Sin_Anti', 'Meta_T.R_$']
        for col in columnas_numericas:
            if col in df_analisis_cartera.columns:
                df_analisis_cartera[col] = pd.to_numeric(df_analisis_cartera[col], errors='coerce').fillna(0)

        columnas_texto = ['Zona', 'Regional_Cobro', 'Franja_Meta', 'Cobrador', 'Gestor']
        for col in columnas_texto:
            if col in df_analisis_cartera.columns:
                df_analisis_cartera[col] = df_analisis_cartera[col].astype(str).str.strip().str.upper().replace('NAN', '')

        # --- Paso 2: Filtrar franjas y ZONAS relevantes ---
        print("🔍 Filtrando franjas de mora y zonas no deseadas...")
        franjas_validas = ['1 A 30', '31 A 90', '91 A 180', '181 A 360']
        df_filtrado = df_analisis_cartera[df_analisis_cartera['Franja_Meta'].isin(franjas_validas)].copy()
        
        zonas_a_omitir = ['1CE', 'CEC', 'CL1', 'CL2', 'CL3', 'CL4']
        df_filtrado = df_filtrado[~df_filtrado['Zona'].isin(zonas_a_omitir)]
        
        # --- Paso 3: Agrupar datos ---
        print("📊 Agrupando datos por franjas y calculando totales...")
        
        # Agrupación para las franjas (para pivotar)
        df_agrupado_franjas_zonas = df_filtrado.groupby(['Regional_Cobro', 'Zona', 'Cobrador', 'Franja_Meta']).agg({
            'Meta_$': 'sum',
            'Recaudo_Meta': 'sum'
        }).reset_index()

        df_agrupado_franjas_gestores = df_filtrado.groupby(['Regional_Cobro', 'Gestor', 'Franja_Meta']).agg({
            'Meta_$': 'sum',
            'Recaudo_Meta': 'sum'
        }).reset_index()

        # --- NUEVO: Agrupación para los totales (sin pivotar) ---
        # Como un mismo crédito puede aparecer en varias franjas, necesitamos obtener los valores únicos por crédito
        # antes de sumar, para no contar el mismo 'Meta_T.R_$' varias veces.
        df_unico_credito = df_filtrado.drop_duplicates(subset=['Credito'])

        df_totales_zonas = df_unico_credito.groupby(['Regional_Cobro', 'Zona', 'Cobrador']).agg({
            'Total_Recaudo_Sin_Anti': 'sum',
            'Meta_T.R_$': 'sum'
        }).reset_index()

        df_totales_gestores = df_unico_credito.groupby(['Regional_Cobro', 'Gestor']).agg({
            'Total_Recaudo_Sin_Anti': 'sum',
            'Meta_T.R_$': 'sum'
        }).reset_index()


        # --- Paso 4: Pivotar tablas de franjas ---
        # (El resto del código se adapta a esta nueva estructura)
        print("🔄 Pivotando tablas...")
        df_pivot_zonas = df_agrupado_franjas_zonas.pivot_table(
            index=['Regional_Cobro', 'Zona', 'Cobrador'], columns='Franja_Meta',
            values=['Meta_$', 'Recaudo_Meta'], aggfunc='sum', fill_value=0
        )
        df_pivot_zonas.columns = [f'{val}_{franja}' for val, franja in df_pivot_zonas.columns]
        df_pivot_zonas.reset_index(inplace=True)

        df_pivot_gestores = df_agrupado_franjas_gestores.pivot_table(
            index=['Regional_Cobro', 'Gestor'], columns='Franja_Meta',
            values=['Meta_$', 'Recaudo_Meta'], aggfunc='sum', fill_value=0
        )
        df_pivot_gestores.columns = [f'{val}_{franja}' for val, franja in df_pivot_gestores.columns]
        df_pivot_gestores.reset_index(inplace=True)

        # --- Paso 5: Crear la Estructura del DataFrame Final ---
        print("🏗️ Creando estructura del reporte final...")
        header = [
            ('ZONA', ''), ('REGIONAL', ''), ('NOMBRE', ''),
            ('1 A 30', 'META_$'), ('1 A 30', 'Recaudo_Meta'), ('1 A 30', 'Cumplimiento_%'),
            ('31 A 90', 'META_$'), ('31 A 90', 'Recaudo_Meta'), ('31 A 90', 'Cumplimiento_%'),
            ('91 A 180', 'META_$'), ('91 A 180', 'Recaudo_Meta'), ('91 A 180', 'Cumplimiento_%'),
            ('181 A 360', 'META_$'), ('181 A 360', 'Recaudo_Meta'), ('181 A 360', 'Cumplimiento_%'),
            # --- NUEVO: Añadimos las nuevas columnas al header ---
            ('Recaudo_Meta_TR', ''),
            ('META_TR', '$'),
            ('Cumplimiento_TR%', '')
        ]
        
        franjas_map = {'1 A 30': '1 A 30', '31 A 90': '31 A 90', '91 A 180': '91 A 180', '181 A 360': '181 A 360'}

        # --- Paso 6: Construir y Unir datos de Zonas ---
        df_zonas_final = pd.merge(df_pivot_zonas, df_totales_zonas, on=['Regional_Cobro', 'Zona', 'Cobrador'], how='left')

        # --- Paso 7: Construir y Unir datos de Gestores ---
        df_gestores_final = pd.merge(df_pivot_gestores, df_totales_gestores, on=['Regional_Cobro', 'Gestor'], how='left')

        # --- Paso 8: Combinar, Formatear y Calcular Cumplimiento ---
        print("🧩 Combinando y formateando el reporte final...")
        
        # Combinamos todo en una sola tabla
        df_final = pd.concat([df_zonas_final, df_gestores_final], ignore_index=True)

        # Renombramos las columnas para que coincidan con el header final
        df_final.rename(columns={
            'Zona': 'ZONA',
            'Regional_Cobro': 'REGIONAL',
            'Cobrador': 'NOMBRE_COBRADOR',
            'Gestor': 'NOMBRE_GESTOR',
            'Total_Recaudo_Sin_Anti': 'Recaudo_Meta_TR',
            'Meta_T.R_$': 'META_TR$'
        }, inplace=True)

        # Unificamos las columnas de nombre y llenamos 'ZONA' para los gestores
        df_final['NOMBRE'] = df_final['NOMBRE_COBRADOR'].fillna(df_final['NOMBRE_GESTOR'])
        df_final['ZONA'] = df_final['ZONA'].fillna('GESTOR')
        df_final.drop(columns=['NOMBRE_COBRADOR', 'NOMBRE_GESTOR'], inplace=True)

        # Creamos el DataFrame final con la estructura MultiIndex y asignamos los datos
        df_reporte = pd.DataFrame(columns=pd.MultiIndex.from_tuples(header))

        df_reporte[('ZONA', '')] = df_final['ZONA']
        df_reporte[('REGIONAL', '')] = df_final['REGIONAL']
        df_reporte[('NOMBRE', '')] = df_final['NOMBRE']
        
        # Llenar datos de franjas
        for franja_reporte, franja_pivot in franjas_map.items():
            meta_col, recaudo_col = f'Meta_$_{franja_pivot}', f'Recaudo_Meta_{franja_pivot}'
            df_reporte[(franja_reporte, 'META_$')] = df_final.get(meta_col, 0)
            df_reporte[(franja_reporte, 'Recaudo_Meta')] = df_final.get(recaudo_col, 0)
        
        # Llenar datos de totales TR
        df_reporte[('TOTALES', 'Recaudo_Meta_TR')] = df_final.get('Recaudo_Meta_TR', 0)
        df_reporte[('TOTALES', 'META_TR$')] = df_final.get('META_TR$', 0)
        
        # Llenar valores nulos con 0 solo en columnas numéricas para no dañar textos
        df_reporte = df_reporte.fillna(0)

        # Calcular Cumplimientos
        for franja in franjas_map.keys():
            meta = pd.to_numeric(df_reporte[(franja, 'META_$')], errors='coerce')
            recaudo = pd.to_numeric(df_reporte[(franja, 'Recaudo_Meta')], errors='coerce')
            porcentaje_decimal = np.where(meta != 0, recaudo / meta, 0)
            df_reporte[(franja, 'Cumplimiento_%')] = [f"{round(x * 100)}%" for x in porcentaje_decimal]
        
        # --- NUEVO: Calcular Cumplimiento TR con dos decimales ---
        meta_tr = pd.to_numeric(df_reporte[('TOTALES', 'META_TR$')], errors='coerce')
        recaudo_tr = pd.to_numeric(df_reporte[('TOTALES', 'Recaudo_Meta_TR')], errors='coerce')
        porcentaje_tr = np.where(meta_tr != 0, recaudo_tr / meta_tr, 0)
        df_reporte[('TOTALES', 'Cumplimiento_TR%')] = [f"{format(x * 100, '.2f')}%".replace('.', ',') for x in porcentaje_tr]

        # Ordenamiento final
        df_reporte['sort_key'] = np.where(df_reporte[('ZONA', '')] == 'GESTOR', 1, 0)
        df_reporte = df_reporte.sort_values(by=[('REGIONAL', ''), 'sort_key', ('ZONA', '')]).reset_index(drop=True)
        df_reporte.drop(columns='sort_key', inplace=True)
        
        print("✅ Reporte de pagos por franjas y totales generado.")
        return df_reporte
