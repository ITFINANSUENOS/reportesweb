import pandas as pd
import numpy as np
from src.services.base.dataloader_service import DataLoaderService

class ReportProcessorService:
    def __init__(self, config):
        self.config = config
        self.data_loader = DataLoaderService(config)

    """Servicio para el procesamiento final del reporte consolidado"""
    
    def calculate_balances(self, reporte_df, fnz003_df):
        """Calcula los diferentes saldos y los agrega al reporte."""
        print("📊 Calculando saldos...")
        reporte_df['Saldo_Capital'] = np.where(reporte_df['Empresa'] == 'ARPESOD', reporte_df.get('Saldo_Factura'), np.nan)
        if not fnz003_df.empty:
            fnz003_df = self.data_loader.create_credit_key(fnz003_df)
            fnz003_df['Credito'] = fnz003_df['Credito'].astype(str)
            mapa_capital = fnz003_df[fnz003_df['Concepto'].isin(['CAPITAL', 'ABONO DIF TASA'])].groupby('Credito')['Saldo'].sum()
            mapa_avales = fnz003_df[fnz003_df['Concepto'] == 'AVAL'].groupby('Credito')['Saldo'].sum()
            mapa_interes = fnz003_df[fnz003_df['Concepto'] == 'INTERES CORRIENTE'].groupby('Credito')['Saldo'].sum()
            
            reporte_df['Saldo_Capital'] = reporte_df['Credito'].map(mapa_capital).combine_first(reporte_df['Saldo_Capital'])
            reporte_df['Saldo_Avales'] = reporte_df['Credito'].map(mapa_avales)
            reporte_df['Saldo_Interes_Corriente'] = reporte_df['Credito'].map(mapa_interes)
        
        reporte_df['Saldo_Capital'] = pd.to_numeric(reporte_df['Saldo_Capital'], errors='coerce').fillna(0).astype(int)
        reporte_df['Saldo_Avales'] = np.where(reporte_df['Empresa'] == 'FINANSUEÑOS', reporte_df.get('Saldo_Avales').fillna(0).astype(int), 'NO APLICA')
        reporte_df['Saldo_Interes_Corriente'] = np.where(reporte_df['Empresa'] == 'FINANSUEÑOS', reporte_df.get('Saldo_Interes_Corriente').fillna(0).astype(int), 'NO APLICA')
        return reporte_df

    def calculate_goal_metrics(self, reporte_df, metas_franjas_df):
        """
        Calcula las diferentes métricas de metas, interpretando correctamente
        los porcentajes y limpiando las columnas intermedias al final.
        """
        print("🎯 Calculando métricas de metas...")

        # 1. Calcular Meta_General (sin cambios)
        for col in ['Meta_DC_Al_Dia', 'Meta_DC_Atraso', 'Meta_Atraso']:
            if col in reporte_df.columns:
                reporte_df[col] = pd.to_numeric(reporte_df[col], errors='coerce').fillna(0)
        reporte_df['Meta_General'] = reporte_df['Meta_DC_Al_Dia'] + reporte_df['Meta_DC_Atraso'] + reporte_df['Meta_Atraso']

        if metas_franjas_df.empty:
            print("⚠️ Archivo de Metas por Franja no encontrado. Se omiten los cálculos de metas % y $.")
            return reporte_df

        # 2. Unir metas por franja al reporte principal por 'Zona'
        metas_franjas_df['Zona'] = metas_franjas_df['Zona'].astype(str).str.strip()
        reporte_df['Zona'] = reporte_df['Zona'].astype(str).str.strip()
        columnas_metas_a_borrar = [col for col in metas_franjas_df.columns if col != 'Zona']
        reporte_df = pd.merge(reporte_df, metas_franjas_df, on='Zona', how='left')

        # 3. Convertir las columnas de porcentaje a números decimales correctamente
        print("   - Convirtiendo porcentajes a números...")
        columnas_porcentaje = ['Meta_1_A_30', 'Meta_31_A_90', 'Meta_91_A_180', 'Meta_181_A_360', 'Total_Recaudo']
        for col in columnas_porcentaje:
            if col in reporte_df.columns:
                # Primero convertimos a string y limpiamos
                reporte_df[col] = reporte_df[col].astype(str).str.replace('%', '').str.strip()
                # Luego convertimos a numérico
                numeric_col = pd.to_numeric(reporte_df[col], errors='coerce')
                # Dividimos por 100 solo si el valor es >1 (para manejar ambos casos)
                reporte_df[col] = np.where(
                    numeric_col > 1,
                    numeric_col / 100,
                    numeric_col
                )
                reporte_df[col] = reporte_df[col].fillna(0)


        # 4. Calcular 'Meta_%' dinámicamente
        dias_atraso = reporte_df['Dias_Atraso']
        condiciones = [
            dias_atraso.between(1, 30),
            dias_atraso.between(31, 90),
            dias_atraso.between(91, 180),
            dias_atraso.between(181, 360)
        ]
        valores = [
            reporte_df['Meta_1_A_30'],
            reporte_df['Meta_31_A_90'],
            reporte_df['Meta_91_A_180'],
            reporte_df['Meta_181_A_360']
        ]
        reporte_df['Meta_%'] = np.select(condiciones, valores, default=0)

        # 5. Calcular 'Meta_$'
        reporte_df['Meta_$'] = reporte_df['Meta_General'] * reporte_df['Meta_%']
        
        # 6. Calcular 'Meta_T.R_%' y 'Meta_T.R_$'
        reporte_df['Meta_T.R_%'] = reporte_df['Total_Recaudo']

        saldo_capital_num = pd.to_numeric(reporte_df['Saldo_Capital'], errors='coerce').fillna(0)
        saldo_avales_num = pd.to_numeric(reporte_df['Saldo_Avales'], errors='coerce').fillna(0)
        saldo_interes_num = pd.to_numeric(reporte_df['Saldo_Interes_Corriente'], errors='coerce').fillna(0)
        
        total_saldo_fns = saldo_capital_num + saldo_avales_num + saldo_interes_num
        
        reporte_df['Meta_T.R_$'] = np.where(
            reporte_df['Empresa'] == 'FINANSUEÑOS', 
            total_saldo_fns, 
            saldo_capital_num
        ) * reporte_df['Meta_T.R_%']

        # --- NUEVO: 7. Eliminar las columnas intermedias de metas ---
        print("   - Limpiando columnas de metas intermedias...")
        reporte_df.drop(columns=columnas_metas_a_borrar, inplace=True, errors='ignore')

        return reporte_df

    def map_call_center_data(self, reporte_df):
        """
        Crea las columnas 'Franja_Mora' y de Call Center consolidadas,
        basándose en los días de atraso.
        """
        print("Mappings de datos de Call Center...")

        # 1. Asegurarse que 'Dias_Atraso' sea numérico para poder comparar
        if 'Dias_Atraso' not in reporte_df.columns:
            print("⚠️ Columna 'Dias_Atraso' no encontrada. No se puede mapear la franja de mora.")
            return reporte_df
            
        reporte_df['Dias_Atraso'] = pd.to_numeric(reporte_df['Dias_Atraso'], errors='coerce')

        # 2. Definir condiciones y valores para 'Franja_Mora'
        condiciones_mora = [
            reporte_df['Dias_Atraso'] == 0,
            reporte_df['Dias_Atraso'].between(1, 30),
            reporte_df['Dias_Atraso'].between(31, 90),
            reporte_df['Dias_Atraso'] > 90
        ]
        valores_mora = ['AL DIA', '1 A 30', '31 A 90', '91 A 360']
        reporte_df['Franja_Mora'] = np.select(condiciones_mora, valores_mora, default='SIN INFO')

        # 3. Mapear datos del Call Center según la 'Franja_Mora'
        # Se definen las columnas de origen para cada franja
        mapa_columnas = {
            '1 A 30': ('call_center_1_30_dias', 'call_center_nombre_1_30', 'call_center_telefono_1_30'),
            '31 A 90': ('call_center_31_90_dias', 'call_center_nombre_31_90', 'call_center_telefono_31_90'),
            '91 A 360': ('call_center_91_360_dias', 'call_center_nombre_91_360', 'call_center_telefono_91_360')
        }

        # Se crean las nuevas columnas vacías
        reporte_df['Call_Center_Apoyo'] = np.nan
        reporte_df['Nombre_Call_Center'] = np.nan
        reporte_df['Telefono_Call_Center'] = np.nan

        # Se llenan las nuevas columnas iterando sobre el mapa
        for franja, cols in mapa_columnas.items():
            # cols[0] = apoyo, cols[1] = nombre, cols[2] = telefono
            # Solo se actualizan las filas que coinciden con la franja actual
            mask = reporte_df['Franja_Mora'] == franja
            if cols[0] in reporte_df.columns:
                reporte_df.loc[mask, 'Call_Center_Apoyo'] = reporte_df.loc[mask, cols[0]]
            if cols[1] in reporte_df.columns:
                reporte_df.loc[mask, 'Nombre_Call_Center'] = reporte_df.loc[mask, cols[1]]
            if cols[2] in reporte_df.columns:
                 reporte_df.loc[mask, 'Telefono_Call_Center'] = reporte_df.loc[mask, cols[2]]

        # 4. Eliminar las columnas originales de la matriz para limpiar el reporte
        cols_a_borrar_matriz = [
            'call_center_1_30_dias', 'call_center_nombre_1_30', 'call_center_telefono_1_30', 
            'call_center_31_90_dias', 'call_center_nombre_31_90', 'call_center_telefono_31_90', 
            'call_center_91_360_dias', 'call_center_nombre_91_360', 'call_center_telefono_91_360'
        ]
        # Nos aseguramos de no borrar 'Zona' que es la clave de cruce
        columnas_existentes_a_borrar = [col for col in cols_a_borrar_matriz if col in reporte_df.columns]
        reporte_df.drop(columns=columnas_existentes_a_borrar, inplace=True, errors='ignore')
        
        return reporte_df

    def filter_by_date_range(self, reporte_df, start_date, end_date):
        """
        Filtra el reporte final por un rango de fechas en la columna 'Fecha_Cuota_Vigente'.
        Este filtro es opcional.
        """
        # Si no se proveen fechas, no se hace nada.
        if not start_date or not end_date:
            return reporte_df

        print(f"🔍 Aplicando filtro de fecha: desde {start_date} hasta {end_date}")

        df = reporte_df.copy()

        # Asegurarse de que la columna de fecha sea del tipo correcto
        # Se usa 'coerce' para convertir errores en NaT (Not a Time)
        df['Fecha_Cuota_Vigente'] = pd.to_datetime(df['Fecha_Cuota_Vigente'], format='%d/%m/%Y', errors='coerce')

        # Convertir las fechas de entrada a datetime
        start_date_dt = pd.to_datetime(start_date, format='%d/%m/%Y', errors='coerce')
        end_date_dt = pd.to_datetime(end_date, format='%d/%m/%Y', errors='coerce')

        if pd.isna(start_date_dt) or pd.isna(end_date_dt):
            print("⚠️ Formato de fecha inválido. Se omite el filtro.")
            return reporte_df # Devuelve el df original si las fechas son inválidas

        # Crear la máscara de filtrado
        mask = (df['Fecha_Cuota_Vigente'] >= start_date_dt) & (df['Fecha_Cuota_Vigente'] <= end_date_dt)
        
        filtered_df = df[mask]
        print(f"✅ Filtro aplicado. {len(filtered_df)} registros encontrados en el rango.")
        
        return filtered_df

    def finalize_report(self, reporte_df, orden_columnas):
        """Realiza la limpieza, formato y reordenamiento final del reporte."""
        print("🧹 Realizando transformaciones y limpieza final...")
        
        # Formatear y rellenar columnas de vencimientos
        columnas_vencimiento = {
            'Fecha_Cuota_Vigente': 'VIGENCIA EXPIRADA',
            'Cuota_Vigente': 'VIGENCIA EXPIRADA',
            'Valor_Cuota_Vigente': 'VIGENCIA EXPIRADA',
            'Fecha_Cuota_Atraso': 'SIN MORA',
            'Primera_Cuota_Mora': 'SIN MORA',
            'Valor_Cuota_Atraso': 0,
            'Valor_Vencido': 0
        }
        for col, default_value in columnas_vencimiento.items():
            if col not in reporte_df.columns:
                reporte_df[col] = default_value
            else:
                if 'Fecha' in col:
                    reporte_df[col] = pd.to_datetime(reporte_df[col], errors='coerce').dt.strftime('%d/%m/%Y')
                reporte_df[col].fillna(default_value, inplace=True)

        # Formatear otras fechas
        if 'Fecha_Facturada' in reporte_df.columns:
            reporte_df['Fecha_Facturada'] = pd.to_datetime(reporte_df['Fecha_Facturada'], errors='coerce').dt.strftime('%d/%m/%Y').fillna('')

        # Formatear columnas de porcentaje (SOLUCIÓN MODIFICADA)
        print("✨ Formateando columnas de porcentaje...")
        columnas_porcentaje = ['Meta_%', 'Meta_T.R_%']
        for col in columnas_porcentaje:
            if col in reporte_df.columns:
                # Convertimos a string y limpiamos
                str_col = reporte_df[col].astype(str).str.replace('%', '').str.strip()
                # Convertimos a numérico
                numeric_col = pd.to_numeric(str_col, errors='coerce')
                # Aseguramos que sea decimal correcto (19 -> 0.19)
                numeric_col = np.where(
                    numeric_col > 1,
                    numeric_col / 100,
                    numeric_col
                ).round(4)  # Redondeamos a 4 decimales para precisión
                
                # Guardamos el valor formateado directamente en la columna original
                reporte_df[col] = (numeric_col * 100).round(0).astype(int).astype(str) + '%'
                
                # Para los cálculos que necesiten el valor decimal, usamos numeric_col directamente
                # (pero no lo guardamos en el dataframe final)

        # Eliminar columnas temporales y reordenar
        print("🏗️  Reordenando columnas según la configuración...")
        columnas_a_eliminar = [
            'Saldo_Factura',
            *[col for col in reporte_df.columns if '_Analisis' in col or '_R03' in col or '_Venc' in col],
            *[col for col in reporte_df.columns if col.endswith('_display')]  # Eliminar columnas display si existen
        ]
        reporte_df.drop(columns=columnas_a_eliminar, inplace=True, errors='ignore')
        
        columnas_actuales = reporte_df.columns.tolist()
        columnas_ordenadas = [col for col in orden_columnas if col in columnas_actuales]
        columnas_restantes = [col for col in columnas_actuales if col not in columnas_ordenadas]
        
        return reporte_df[columnas_ordenadas + columnas_restantes]