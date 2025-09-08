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
        Calcula saldos de forma robusta, asegurando que las columnas sean siempre numéricas.
        """
        print("📊 Calculando saldos...")
        creditos_negativos_fnz003 = pd.DataFrame()

        for col in ['Saldo_Capital', 'Saldo_Avales', 'Saldo_Interes_Corriente']:
            if col not in reporte_df.columns:
                reporte_df[col] = 0

        reporte_df['Saldo_Capital'] = np.where(reporte_df['Empresa'] == 'ARPESOD', reporte_df.get('Saldo_Factura'), np.nan)
        
        if not fnz003_df.empty:
            fnz003_df['Saldo'] = pd.to_numeric(fnz003_df['Saldo'], errors='coerce').fillna(0)
            
            negativos_df = fnz003_df[fnz003_df['Saldo'] < 0].copy()
            if not negativos_df.empty:
                print(f"   - ⚠️ Se encontraron {len(negativos_df)} saldos negativos en FNZ003.")
                negativos_df = self.data_loader.create_credit_key(negativos_df) 
                negativos_df['Observacion'] = 'Saldo negativo en: ' + negativos_df['Concepto'].astype(str)
                creditos_negativos_fnz003 = negativos_df[['Credito', 'Observacion']].drop_duplicates()

            mapa_capital = fnz003_df[fnz003_df['Concepto'].isin(['CAPITAL', 'ABONO DIF TASA'])].groupby('Credito')['Saldo'].sum()
            mapa_avales = fnz003_df[fnz003_df['Concepto'] == 'AVAL'].groupby('Credito')['Saldo'].sum()
            mapa_interes = fnz003_df[fnz003_df['Concepto'] == 'INTERES CORRIENTE'].groupby('Credito')['Saldo'].sum()
            
            mask_fns = reporte_df['Empresa'] == 'FINANSUEÑOS'
            reporte_df.loc[mask_fns, 'Saldo_Capital'] = reporte_df.loc[mask_fns, 'Credito'].map(mapa_capital)
            reporte_df.loc[mask_fns, 'Saldo_Avales'] = reporte_df.loc[mask_fns, 'Credito'].map(mapa_avales)
            reporte_df.loc[mask_fns, 'Saldo_Interes_Corriente'] = reporte_df.loc[mask_fns, 'Credito'].map(mapa_interes)
        
        # Limpieza final: Rellenamos vacíos con 0 y convertimos todo a entero.
        for col in ['Saldo_Capital', 'Saldo_Avales', 'Saldo_Interes_Corriente']:
            if col in reporte_df.columns:
                reporte_df[col] = pd.to_numeric(reporte_df[col], errors='coerce').fillna(0).astype(int)
        
        return reporte_df, creditos_negativos_fnz003

    def calculate_goal_metrics(self, reporte_df, metas_franjas_df = None):
        """
        Calcula las diferentes métricas de metas.
        """
        print("🎯 Calculando métricas de metas...")


        for col in ['Meta_DC_Al_Dia', 'Meta_DC_Atraso', 'Meta_Atraso']:
            if col in reporte_df.columns:
                reporte_df[col] = pd.to_numeric(reporte_df[col], errors='coerce').fillna(0)
        reporte_df['Meta_General'] = reporte_df['Meta_DC_Al_Dia'] + reporte_df['Meta_DC_Atraso'] + reporte_df['Meta_Atraso']

        columnas_metas_a_borrar = []

        if 'Meta_1_A_30' not in reporte_df.columns:
            print("   - Columnas de metas no encontradas. Uniendo desde el archivo de metas por franjas...")
            if metas_franjas_df is not None and not metas_franjas_df.empty:
                reporte_df = pd.merge(reporte_df, metas_franjas_df, on='Zona', how='left')
                columnas_metas_a_borrar = [col for col in metas_franjas_df.columns if col != 'Zona']
            else:
                print("   - ⚠️ ADVERTENCIA: No se pudo realizar la unión. Los cálculos de metas por franja serán 0.")
                for col in ['Meta_1_A_30', 'Meta_31_A_90', 'Meta_91_A_180', 'Meta_181_A_360', 'Total_Recaudo', 'Meta_%', 'Meta_$']:
                    reporte_df[col] = 0
     
        
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
            dias_atraso.between(91, 180), dias_atraso.between(181, 360)
        ]
        valores = [
            reporte_df['Meta_1_A_30'], reporte_df['Meta_31_A_90'],
            reporte_df['Meta_91_A_180'], reporte_df['Meta_181_A_360']
        ]
        reporte_df['Meta_%'] = np.select(condiciones, valores, default=0)
        reporte_df['Meta_$'] = reporte_df['Meta_General'] * reporte_df['Meta_%']
        # Asignamos Total_Recaudo a Meta_T.R_% solo si la columna existe
        if 'Total_Recaudo' in reporte_df.columns:
            reporte_df['Meta_T.R_%'] = reporte_df['Total_Recaudo']
        else:
            reporte_df['Meta_T.R_%'] = 0

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
            reporte_df['Dias_Atraso'].between(181, 360),
            reporte_df['Dias_Atraso'] > 360
        ]
        valores_mora = ['AL DIA', '1 A 30', '31 A 90', '91 A 180','181 A 360','MAS DE 360']
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
        if not start_date or not end_date:
            return reporte_df

        print(f"🔍 Aplicando filtro de fecha: desde {start_date} hasta {end_date}")

        df = reporte_df.copy()
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
    

    def _detectar_problemas_calidad(self, df):
        """
        Crea un reporte de auditoría de calidad de datos, garantizando una
        única fila por crédito y aplicando las reglas de negocio correctamente.
        """
        print("🔍 Generando reporte de auditoría de calidad de datos...")
        
        # 1. Usamos una copia del DataFrame con créditos únicos como base de todo.
        df_auditoria = df.drop_duplicates(subset=['Credito']).copy()
        
        print("    - Consolidando columnas 'Celular' en conflicto...")
        if 'Celular_y' in df_auditoria.columns and 'Celular_x' in df_auditoria.columns:
            # Damos prioridad a Celular_y (de VENCIMIENTOS) y rellenamos vacíos con Celular_x
            df_auditoria['Celular'] = df_auditoria['Celular_y'].fillna(df_auditoria['Celular_x'])
        elif 'Celular_y' in df_auditoria.columns:
            df_auditoria['Celular'] = df_auditoria['Celular_y']
        elif 'Celular_x' in df_auditoria.columns:
            df_auditoria['Celular'] = df_auditoria['Celular_x']
        
        if 'Celular' not in df_auditoria.columns:
            df_auditoria['Celular'] = np.nan

        
        # --- Reglas de Nulos o Valores Específicos ---
        df_auditoria['Estado_Fecha_Desembolso'] = np.where(pd.to_datetime(df_auditoria['Fecha_Desembolso'], errors='coerce').isnull(), 'CORREGIR', 'BIEN')
        df_auditoria['Estado_Fecha_Facturada'] = np.where(pd.to_datetime(df_auditoria['Fecha_Facturada'], errors='coerce').isnull(), 'CORREGIR', 'BIEN')
        df_auditoria['Estado_Factura'] = np.where(df_auditoria['Factura_Venta'] == 'NO ASIGNADA', 'CORREGIR', 'BIEN')
        
        df_auditoria['Estado_Producto'] = np.where(df_auditoria['Nombre_Producto'] == 'NO REGISTRA', 'CORREGIR', 'BIEN')
        df_auditoria['Estado_Obsequio'] = np.where((df_auditoria['Obsequio'] == 'SIN OBSEQUIOS') & (df_auditoria['Nombre_Producto'] == 'NO REGISTRA'), 'CORREGIR', 'BIEN')
        df_auditoria['Estado_Cant_Producto'] = np.where((pd.to_numeric(df_auditoria['Cantidad_Producto'], errors='coerce') == 0) & (df_auditoria['Factura_Venta'] == 'NO ASIGNADA'), 'CORREGIR', 'BIEN')
        df_auditoria['Estado_Cant_Obsequio'] = np.where((pd.to_numeric(df_auditoria['Cantidad_Obsequio'], errors='coerce') == 0) & (df_auditoria['Factura_Venta'] == 'NO ASIGNADA'), 'CORREGIR', 'BIEN')
        df_auditoria['Estado_Direccion'] = np.where(df_auditoria['Direccion'].isnull() | (df_auditoria['Direccion'] == ''), 'CORREGIR', 'BIEN')
        df_auditoria['Estado_Barrio'] = np.where(df_auditoria['Barrio'].isnull() | (df_auditoria['Barrio'] == ''), 'CORREGIR', 'BIEN')
        df_auditoria['Estado_Nombre_Ciudad'] = np.where(df_auditoria['Nombre_Ciudad'].isnull() | (df_auditoria['Nombre_Ciudad'] == ''), 'CORREGIR', 'BIEN')

        # --- Reglas de Formato (Regex) ---
        email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        mask_correo_ok = df_auditoria['Correo'].astype(str).str.match(email_regex, na=False)
        df_auditoria['Estado_Correo'] = np.where(mask_correo_ok, 'BIEN', 'CORREGIR')

        celular_regex = r'^(3\d{9}|60\d{8})$'
        celulares_str = df_auditoria['Celular'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        mask_celular_ok = celulares_str.str.match(celular_regex, na=False)
        df_auditoria['Estado_Celular'] = np.where(mask_celular_ok, 'BIEN', 'CORREGIR')

        # --- Reglas Numéricas ---
        for col in ['Valor_Desembolso', 'Total_Cuotas', 'Valor_Cuota']:
            df_auditoria[f'Estado_{col}'] = np.where(pd.to_numeric(df_auditoria[col], errors='coerce').fillna(0) == 0, 'CORREGIR', 'BIEN')
        df_auditoria['Estado_Dias_Atraso'] = np.where(pd.to_numeric(df_auditoria['Dias_Atraso'], errors='coerce').isnull(), 'CORREGIR', 'BIEN')

        # --- Regla para Codeudores ---
        for i in ['1', '2']:
            for col_base in ['Codeudor', 'Nombre_Codeudor', 'Telefono_Codeudor', 'Ciudad_Codeudor']:
                col = f"{col_base}{i}"
                df_auditoria[f'Estado_{col}'] = np.where(df_auditoria[col].isnull() | (df_auditoria[col] == 'SIN CODEUDOR'), 'CORREGIR', 'BIEN')

        # 4. Filtramos para quedarnos solo con las filas que tienen al menos un problema
        columnas_de_estado = [col for col in df_auditoria.columns if col.startswith('Estado_')]
        mascara_final = (df_auditoria[columnas_de_estado] == 'CORREGIR').any(axis=1)
        
        # 5. Seleccionamos las columnas a mostrar en el reporte final
        columnas_a_mostrar = ['Credito', 'Cedula_Cliente', 'Nombre_Cliente'] + sorted(columnas_de_estado)
        df_a_corregir = df_auditoria.loc[mascara_final, columnas_a_mostrar]
        
        if not df_a_corregir.empty:
            print(f"   - ✅ Se encontraron {len(df_a_corregir)} créditos únicos con problemas de calidad para revisar.")
        
        return df_a_corregir
        

    def finalize_report(self, reporte_df, orden_columnas):
        """Realiza la limpieza, formato y reordenamiento final del reporte."""
        print("🧹 Realizando transformaciones y limpieza final...")
        
        # --- PASO 1: Detectar problemas ANTES de cualquier formateo ---
        df_a_corregir = self._detectar_problemas_calidad(reporte_df)

        # --- PASO 2: Formatear TODAS las columnas de fecha ---
        print("📅 Formateando fechas a solo día/mes/año...")
        
        columnas_de_fecha = [
            'Fecha_Cuota_Vigente', 'Fecha_Cuota_Atraso', 
            'Fecha_Facturada', 'Fecha_Desembolso', 'Fecha_Ultima_Novedad'
        ]
        
        for col in columnas_de_fecha:
            if col in reporte_df.columns:
                # --- CAMBIO CLAVE: de .dt.normalize() a .dt.date ---
                # .dt.date extrae solo la fecha, eliminando la hora por completo.
                # Excel lo reconocerá como un tipo de dato de fecha puro.
                reporte_df[col] = pd.to_datetime(reporte_df[col], errors='coerce').dt.date

        # --- PASO 3: Rellenar vacíos y formatear el resto de columnas ---
        
        columnas_vencimiento = {
            'Fecha_Cuota_Vigente': 'VIGENCIA EXPIRADA', 'Cuota_Vigente': 'VIGENCIA EXPIRADA',
            'Valor_Cuota_Vigente': 'VIGENCIA EXPIRADA', 'Fecha_Cuota_Atraso': 'SIN MORA',
            'Primera_Cuota_Mora': 'SIN MORA', 'Valor_Cuota_Atraso': 0, 'Valor_Vencido': 0
        }
        
        for col, default_value in columnas_vencimiento.items():
            if col not in reporte_df.columns:
                reporte_df[col] = default_value
            else:
                # Para las fechas, si después de procesar siguen vacías (NaT),
                # las llenamos con el texto correspondiente.
                if 'Fecha' in col:
                    reporte_df[col] = reporte_df[col].fillna(default_value)
                else:
                    reporte_df[col].fillna(default_value, inplace=True)
        
        
        print("💅 Aplicando formato de presentación final ('NO APLICA')...")
    
        # Definimos la condición (créditos que no son de FINANSUEÑOS)
        mask_no_fns = reporte_df['Empresa'] != 'FINANSUEÑOS'
        columnas_a_formatear = ['Saldo_Avales', 'Saldo_Interes_Corriente']

        for col in columnas_a_formatear:
            if col in reporte_df.columns:
                # Primero, convertimos la columna a tipo 'object' para que pueda aceptar texto
                reporte_df[col] = reporte_df[col].astype(object)
                # Donde la condición se cumple y el valor es 0, lo reemplazamos por 'NO APLICA'
                reporte_df.loc[mask_no_fns, col] = 'NO APLICA'
        # --- FIN DEL NUEVO BLOQUE ---


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
        
        return reporte_df[columnas_ordenadas + columnas_restantes],df_a_corregir