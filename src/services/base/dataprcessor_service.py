import pandas as pd
import numpy as np
from src.services.base.dataloader_service import DataLoaderService

class ReportProcessorService:
    def __init__(self, config):
        self.config = config
        self.data_loader = DataLoaderService(config)

    """Servicio para el procesamiento final del reporte consolidado"""
    
    def calculate_balances(self, reporte_df, fnz003_df):
        """
        Calcula saldos, los agrega al reporte, y devuelve una lista de créditos
        con saldos negativos encontrados en FNZ003.
        """
        print("📊 Calculando saldos...")
        
        creditos_negativos_fnz003 = pd.DataFrame() # Inicia df de negativos vacío

        reporte_df['Saldo_Capital'] = np.where(reporte_df['Empresa'] == 'ARPESOD', reporte_df.get('Saldo_Factura'), np.nan)
        
        if not fnz003_df.empty:
            # Aseguramos que 'Saldo' sea numérico para los cálculos
            fnz003_df['Saldo'] = pd.to_numeric(fnz003_df['Saldo'], errors='coerce').fillna(0)
            
            # --- NUEVO: Detección de saldos negativos en FNZ003 ---
            negativos_df = fnz003_df[fnz003_df['Saldo'] < 0].copy()
            if not negativos_df.empty:
                print(f"   - ⚠️ Se encontraron {len(negativos_df)} saldos negativos en FNZ003.")
                
                # Creamos la llave 'Credito' para poder unir los datos
                # Asegúrate de que tu data_loader esté disponible como self.data_loader
                negativos_df = self.data_loader.create_credit_key(negativos_df) 
                
                # Creamos la columna 'Observacion' usando el 'Concepto'
                negativos_df['Observacion'] = 'Saldo negativo en: ' + negativos_df['Concepto'].astype(str)
                creditos_negativos_fnz003 = negativos_df[['Credito', 'Observacion']].drop_duplicates()
            # --- FIN DEL BLOQUE NUEVO ---

            # El resto de la lógica de cálculo de saldos no cambia
            mapa_capital = fnz003_df[fnz003_df['Concepto'].isin(['CAPITAL', 'ABONO DIF TASA'])].groupby('Credito')['Saldo'].sum()
            mapa_avales = fnz003_df[fnz003_df['Concepto'] == 'AVAL'].groupby('Credito')['Saldo'].sum()
            mapa_interes = fnz003_df[fnz003_df['Concepto'] == 'INTERES CORRIENTE'].groupby('Credito')['Saldo'].sum()
            
            mask_fns = reporte_df['Empresa'] == 'FINANSUEÑOS'
            reporte_df.loc[mask_fns, 'Saldo_Capital'] = reporte_df.loc[mask_fns, 'Credito'].map(mapa_capital)
            reporte_df.loc[mask_fns, 'Saldo_Avales'] = reporte_df.loc[mask_fns, 'Credito'].map(mapa_avales)
            reporte_df.loc[mask_fns, 'Saldo_Interes_Corriente'] = reporte_df.loc[mask_fns, 'Credito'].map(mapa_interes)
        
        # Limpieza final de las columnas de saldo (sin cambios)
        reporte_df['Saldo_Capital'] = pd.to_numeric(reporte_df['Saldo_Capital'], errors='coerce').fillna(0).astype(int)
        reporte_df['Saldo_Avales'] = np.where(reporte_df['Empresa'] == 'FINANSUEÑOS', reporte_df.get('Saldo_Avales').fillna(0).astype(int), 'NO APLICA')
        reporte_df['Saldo_Interes_Corriente'] = np.where(reporte_df['Empresa'] == 'FINANSUEÑOS', reporte_df.get('Saldo_Interes_Corriente').fillna(0).astype(int), 'NO APLICA')
        
        # La función ahora devuelve el reporte y la lista de negativos
        return reporte_df, creditos_negativos_fnz003

    def calculate_goal_metrics(self, reporte_df, metas_franjas_df):
        """
        Calcula las diferentes métricas de metas.
        """
        print("🎯 Calculando métricas de metas...")

        # ... (El inicio de la función no cambia) ...
        for col in ['Meta_DC_Al_Dia', 'Meta_DC_Atraso', 'Meta_Atraso']:
            if col in reporte_df.columns:
                reporte_df[col] = pd.to_numeric(reporte_df[col], errors='coerce').fillna(0)
        reporte_df['Meta_General'] = reporte_df['Meta_DC_Al_Dia'] + reporte_df['Meta_DC_Atraso'] + reporte_df['Meta_Atraso']

        if metas_franjas_df.empty:
            return reporte_df

        reporte_df = pd.merge(reporte_df, metas_franjas_df, on='Zona', how='left')
        columnas_metas_a_borrar = [col for col in metas_franjas_df.columns if col != 'Zona']
        
        columnas_porcentaje = ['Meta_1_A_30', 'Meta_31_A_90', 'Meta_91_A_180', 'Meta_181_A_360', 'Total_Recaudo']
        for col in columnas_porcentaje:
            if col in reporte_df.columns:
                reporte_df[col] = reporte_df[col].astype(str).str.replace('%', '').str.strip()
                numeric_col = pd.to_numeric(reporte_df[col], errors='coerce')
                reporte_df[col] = np.where(numeric_col > 1, numeric_col / 100, numeric_col)
                reporte_df[col] = reporte_df[col].fillna(0)

        # 4. Calcular 'Meta_%' dinámicamente
        dias_atraso = reporte_df['Dias_Atraso']
        condiciones = [
            dias_atraso.between(1, 30), dias_atraso.between(31, 90),
            dias_atraso.between(91, 180), dias_atraso > 180
        ]
        valores = [
            reporte_df['Meta_1_A_30'], reporte_df['Meta_31_A_90'],
            reporte_df['Meta_91_A_180'], reporte_df['Meta_181_A_360']
        ]
        reporte_df['Meta_%'] = np.select(condiciones, valores, default=0)
        reporte_df['Meta_$'] = reporte_df['Meta_General'] * reporte_df['Meta_%']
        reporte_df['Meta_T.R_%'] = reporte_df['Total_Recaudo']

        # --- INICIA BLOQUE DE DEPURACIÓN PROFUNDA ---
        # Este bloque revisará las columnas justo antes de la línea que da el error.
        
        print("\n--- DEPURACIÓN PROFUNDA ANTES DE LA MULTIPLICACIÓN FINAL ---")
        columnas_a_revisar = ['Saldo_Capital', 'Saldo_Avales', 'Saldo_Interes_Corriente', 'Meta_T.R_%']
        for col in columnas_a_revisar:
            if col in reporte_df.columns:
                print(f"\nRevisando columna: '{col}'")
                print(f"  - Tipo de dato (dtype): {reporte_df[col].dtype}")
                
                # Esta línea buscará si alguna celda en la columna es una lista
                try:
                    celdas_con_listas = reporte_df[col].apply(lambda x: isinstance(x, list))
                    if celdas_con_listas.any():
                        print(f"  - ¡¡¡ALERTA!!! Se encontraron celdas que contienen LISTAS en esta columna.")
                        print("  - Mostrando las primeras 5 filas con listas:")
                        print(reporte_df[celdas_con_listas][['Credito', col]].head())
                    else:
                        print("  - OK. No se encontraron listas en esta columna.")
                except Exception as e:
                    print(f"  - No se pudo revisar la columna por el error: {e}")
        print("--- FIN DE LA DEPURACIÓN ---\n")
        # --- TERMINA BLOQUE DE DEPURACIÓN PROFUNDA ---

        # Esta es la línea donde probablemente ocurre el error
        saldo_capital_num = pd.to_numeric(reporte_df['Saldo_Capital'], errors='coerce').fillna(0)
        saldo_avales_num = pd.to_numeric(reporte_df['Saldo_Avales'], errors='coerce').fillna(0)
        saldo_interes_num = pd.to_numeric(reporte_df['Saldo_Interes_Corriente'], errors='coerce').fillna(0)
        total_saldo_fns = saldo_capital_num + saldo_avales_num + saldo_interes_num
        
        reporte_df['Meta_T.R_$'] = np.where(
            reporte_df['Empresa'] == 'FINANSUEÑOS', 
            total_saldo_fns, 
            saldo_capital_num
        ) * reporte_df['Meta_T.R_%']

        reporte_df.drop(columns=columnas_metas_a_borrar, inplace=True, errors='ignore')
        return reporte_df

    def map_call_center_data(self, reporte_df):
        """
        Limpia la columna 'Gestor' y crea las columnas 'Franja_Mora' y de
        Call Center consolidadas, basándose en los días de atraso.
        """
        print("📞 Mapeando datos de Gestor y Call Center...")

        # 1. Limpieza de la columna 'Gestor' (NUEVO)
        if 'Gestor' in reporte_df.columns:
            print("   - Limpiando columna 'Gestor'...")
            # Reemplazar 'SIN GESTOR' por 'CALL CENTER'
            reporte_df.loc[reporte_df['Gestor'] == 'SIN GESTOR', 'Gestor'] = 'CALL CENTER'
            # Rellenar valores vacíos (NaN) con 'OTRAS ZONAS'
            reporte_df['Gestor'].fillna('OTRAS ZONAS', inplace=True)
        else:
            print("   - ⚠️ Columna 'Gestor' no encontrada. Se omite la limpieza.")

        # 2. Asegurarse que 'Dias_Atraso' sea numérico para poder comparar
        if 'Dias_Atraso' not in reporte_df.columns:
            print("⚠️ Columna 'Dias_Atraso' no encontrada. No se puede mapear la franja de mora.")
            return reporte_df
            
        reporte_df['Dias_Atraso'] = pd.to_numeric(reporte_df['Dias_Atraso'], errors='coerce')

        # 3. Definir condiciones y valores para 'Franja_Mora' (sin cambios)
        condiciones_mora = [
            reporte_df['Dias_Atraso'] == 0,
            reporte_df['Dias_Atraso'].between(1, 30),
            reporte_df['Dias_Atraso'].between(31, 90),
            reporte_df['Dias_Atraso'].between(91, 180),
            reporte_df['Dias_Atraso'] > 180
        ]
        valores_mora = ['AL DIA', '1 A 30', '31 A 90', '91 A 180','181 A 360']
        reporte_df['Franja_Mora'] = np.select(condiciones_mora, valores_mora, default='SIN INFO')
        
        mapa_franjas = {
            '1 A 30': ('call_center_1_30_dias', 'call_center_nombre_1_30', 'call_center_telefono_1_30'),
            '31 A 90': ('call_center_31_90_dias', 'call_center_nombre_31_90', 'call_center_telefono_31_90'),
            '91 A 180': ('call_center_91_360_dias', 'call_center_nombre_91_360', 'call_center_telefono_91_360'),
            '181 A 360': ('call_center_91_360_dias', 'call_center_nombre_91_360', 'call_center_telefono_91_360')
        }

        # Se crean las nuevas columnas consolidadas vacías
        reporte_df['Call_Center_Apoyo'] = np.nan
        reporte_df['Nombre_Call_Center'] = np.nan
        reporte_df['Telefono_Call_Center'] = np.nan

        # Se llenan las nuevas columnas iterando sobre el mapa de franjas
        print("   - Asignando datos de Call Center por franja de mora...")
        for franja, cols in mapa_franjas.items():
            # cols[0]=apoyo, cols[1]=nombre, cols[2]=telefono
            mask = reporte_df['Franja_Mora'] == franja
            if cols[0] in reporte_df.columns:
                reporte_df.loc[mask, 'Call_Center_Apoyo'] = reporte_df.loc[mask, cols[0]]
            if cols[1] in reporte_df.columns:
                reporte_df.loc[mask, 'Nombre_Call_Center'] = reporte_df.loc[mask, cols[1]]
            if cols[2] in reporte_df.columns:
                reporte_df.loc[mask, 'Telefono_Call_Center'] = reporte_df.loc[mask, cols[2]]

        # 5. Eliminar las columnas originales de la matriz para limpiar el reporte (sin cambios)
        cols_a_borrar_matriz = [
            'call_center_1_30_dias', 'call_center_nombre_1_30', 'call_center_telefono_1_30',
            'call_center_31_90_dias', 'call_center_nombre_31_90', 'call_center_telefono_31_90',
            'call_center_91_360_dias', 'call_center_nombre_91_360', 'call_center_telefono_91_360'
        ]
        columnas_existentes_a_borrar = [col for col in cols_a_borrar_matriz if col in reporte_df.columns]
        reporte_df.drop(columns=columnas_existentes_a_borrar, inplace=True, errors='ignore')
        
        print("✅ Mapeo completado.")
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
        
         # Asegurarse que 'Valor_Vencido' existe y tiene el formato correcto
        if 'Valor_Vencido' not in reporte_df.columns:
            reporte_df['Valor_Vencido'] = 0  # Crear columna si no existe
        
        # Formatear y rellenar columnas de vencimientos
        columnas_vencimiento = {
            'Fecha_Cuota_Vigente': 'VIGENCIA EXPIRADA',
            'Cuota_Vigente': 'VIGENCIA EXPIRADA',
            'Valor_Cuota_Vigente': 'VIGENCIA EXPIRADA',
            'Fecha_Cuota_Atraso': 'SIN MORA',
            'Primera_Cuota_Mora': 'SIN MORA',
            'Valor_Cuota_Atraso': 0,
            'Valor_Vencido': 0  # Asegurar que tiene un valor por defecto
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

        if 'Fecha_Desembolso' in reporte_df.columns:
            reporte_df['Fecha_Desembolso'] = pd.to_datetime(reporte_df['Fecha_Desembolso'], errors='coerce').dt.strftime('%d/%m/%Y').fillna('')    
 
        print("👔 Limpiando y completando la columna 'Lider_Zona' y 'Movil_Lider'")
       
        if 'Lider_Zona' in reporte_df.columns and 'Regional_Venta' in reporte_df.columns:
            print("👔 Limpiando y completando 'Lider_Zona' y 'Movil_Lider'...")

            # --- PASO 1: Limpiar los datos de origen ---
            # Eliminar valores numéricos de 'Lider_Zona' para que no se consideren nombres válidos
            is_numeric_mask = pd.to_numeric(reporte_df['Lider_Zona'], errors='coerce').notna()
            reporte_df.loc[is_numeric_mask, 'Lider_Zona'] = np.nan
            
            # --- PASO 2: Crear un mapa de Líder -> Móvil ---
            # Se crea antes de rellenar los vacíos para capturar las relaciones originales.
            mapa_moviles = {}
            if 'Movil_Lider' in reporte_df.columns:
                # Nos aseguramos de que solo mapeamos líderes que tienen un móvil válido
                mapa_df = reporte_df.dropna(subset=['Lider_Zona', 'Movil_Lider']) \
                                    .drop_duplicates(subset=['Lider_Zona'])
                mapa_moviles = pd.Series(mapa_df['Movil_Lider'].values, index=mapa_df['Lider_Zona']).to_dict()

            # --- PASO 3: Rellenar 'Lider_Zona' con la moda de su respectiva regional ---
            def fill_with_mode(series):
                mode_val = series.mode()
                if not mode_val.empty:
                    return series.fillna(mode_val.iloc[0])
                return series
                
            reporte_df['Lider_Zona'] = reporte_df.groupby('Regional_Venta')['Lider_Zona'].transform(fill_with_mode)

            # --- PASO 4: Usar el 'Lider_Zona' ya completo para asignar su móvil ---
            if 'Movil_Lider' in reporte_df.columns:
                # El método .map usará el 'mapa_moviles' que creamos en el paso 2
                reporte_df['Movil_Lider'] = reporte_df['Lider_Zona'].map(mapa_moviles)

            # --- PASO 5: Rellenar cualquier vacío restante en ambas columnas ---
            reporte_df['Lider_Zona'].fillna('NO ASIGNADO', inplace=True)
            if 'Movil_Lider' in reporte_df.columns:
                reporte_df['Movil_Lider'].fillna('NO ASIGNADO', inplace=True)
        else:
            print("   - ⚠️ Columnas 'Lider_Zona' o 'Regional_Venta' no encontradas. Se omite este paso.")
            
            
        print("🔍 Buscando registros con datos críticos faltantes...")
    
        # Define las columnas que son esenciales para tu operación
        columnas_criticas = [
            'Nombre_Vendedor', 'Zona', 'Saldo_Capital', 'Valor_Desembolso', 'Nombre_Cliente'
        ]
        
        # Filtra el DataFrame para encontrar filas donde CUALQUIERA de las columnas críticas esté vacía o sea 0
        # Usamos una copia para evitar advertencias de pandas
        df_para_revision = reporte_df[columnas_criticas].copy()
        df_para_revision.replace('NO ASIGNADO', np.nan, inplace=True) # Considerar 'NO ASIGNADO' como vacío
        df_para_revision.replace('SIN INFO', np.nan, inplace=True)   # Considerar 'SIN INFO' como vacío
        df_para_revision['Saldo_Capital'] = pd.to_numeric(df_para_revision['Saldo_Capital'], errors='coerce')
        
        # La máscara identifica filas con problemas
        mascara_problemas = df_para_revision.isnull().any(axis=1) | (df_para_revision['Saldo_Capital'] == 0)

        # Creamos la hoja de correcciones con el 'Credito' y las columnas problemáticas
        df_a_corregir = reporte_df.loc[mascara_problemas, ['Credito', 'Cedula_Cliente'] + columnas_criticas]
        
        if not df_a_corregir.empty:
            print(f"   - ⚠️ Se encontraron {len(df_a_corregir)} registros que necesitan corrección manual.")
        # --- FIN DEL BLOQUE NUEVO ---       


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
        mask_arp = reporte_df['Empresa'] == 'ARPESOD'
        for col in ['Saldo_Avales', 'Saldo_Interes_Corriente']:
            if col in reporte_df.columns:
                reporte_df[col] = reporte_df[col].astype(object) # Permite mezclar tipos
                reporte_df.loc[mask_arp, col] = 'NO APLICA' 
                

        if 'Valor_Vencido' not in orden_columnas:
            orden_columnas.append('Valor_Vencido')          

        # Eliminar columnas temporales y reordenar
        print("🏗️  Reordenando columnas según la configuración...")
        columnas_a_eliminar = [
            'Saldo_Factura', 'Tipo_Credito', 'Numero_Credito', 'Meta_DC_Al_Dia', 'Meta_DC_Atraso', 'Meta_Atraso',
            *[col for col in reporte_df.columns if col.endswith('_Analisis') or col.endswith('_R03') or col.endswith('_Venc')],
            *[col for col in reporte_df.columns if col.endswith('_display')]
        ]
        
        reporte_df.drop(columns=columnas_a_eliminar, inplace=True, errors='ignore')
        
        columnas_actuales = reporte_df.columns.tolist()
        columnas_ordenadas = [col for col in orden_columnas if col in columnas_actuales]
        columnas_restantes = [col for col in columnas_actuales if col not in columnas_ordenadas]
        
        return reporte_df[columnas_ordenadas + columnas_restantes]