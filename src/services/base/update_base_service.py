import pandas as pd
import numpy as np
from src.models.base_model import ORDEN_COLUMNAS_FINAL

class UpdateBaseService:
    """
    Servicio especializado en actualizar un reporte base existente 
    con nuevos archivos de datos, en lugar de reconstruirlo desde cero.
    """
    def __init__(self, report_service):
        self.report_service = report_service
        self.data_loader = report_service.data_loader

    def _unir_y_actualizar_columnas(self, df_principal, df_actualizacion, llave, columnas_a_actualizar):
        """
        Une dos DataFrames por una llave y actualiza de forma inteligente las columnas en común.
        """
        if llave not in df_principal.columns or llave not in df_actualizacion.columns:
            print(f"   - ⚠️ Advertencia: La llave '{llave}' no se encontró en ambos DataFrames. Se omite esta actualización.")
            return df_principal

        # --- Limpieza de llaves (la que ya tenías y funciona bien) ---
        df_actualizacion_limpio = df_actualizacion.drop_duplicates(subset=[llave]).copy()
        df_principal[llave] = df_principal[llave].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
        df_actualizacion_limpio[llave] = df_actualizacion_limpio[llave].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
        df_principal = df_principal[df_principal[llave].notna() & (df_principal[llave] != '')]
        df_actualizacion_limpio = df_actualizacion_limpio[df_actualizacion_limpio[llave].notna() & (df_actualizacion_limpio[llave] != '')]

         # --- LÓGICA DE UNIÓN Y ACTUALIZACIÓN DEFINITIVA ---
    
        # 1. Identificar columnas a actualizar
        if columnas_a_actualizar == '__ALL__':
            cols_para_actualizar = df_actualizacion_limpio.columns.drop(llave).tolist()
        else:
            cols_para_actualizar = [col for col in columnas_a_actualizar if col in df_actualizacion_limpio.columns]

        # 2. Identificar columnas que existen en ambos DataFrames (potenciales duplicados)
        columnas_en_comun = df_principal.columns.intersection(df_actualizacion_limpio.columns).drop(llave).tolist()

        # 3. Hacer el merge. Se crearán columnas con sufijo '_nuevo' para las que estén en común.
        #    Las columnas que solo existen en df_actualizacion_limpio se añadirán directamente.
        df_fusionado = pd.merge(df_principal, df_actualizacion_limpio, on=llave, how='left', suffixes=('', '_nuevo'))

        # 4. Consolidar columnas. Para cada columna que se deba actualizar y que estaba en común:
        for col in cols_para_actualizar:
            if col in columnas_en_comun:
                col_nuevo = col + '_nuevo'
                # Usamos el valor nuevo si no es nulo; si es nulo, conservamos el valor original.
                df_fusionado[col] = df_fusionado[col_nuevo].fillna(df_fusionado[col])
                # Eliminamos la columna temporal con el sufijo.
                df_fusionado.drop(columns=[col_nuevo], inplace=True)
                
        return df_fusionado

    def _actualizar_datos_existentes(self, df_base, dataframes_nuevos):
        print("\n🔄 Actualizando información de créditos existentes...")
        print(f"--- [DEBUG] Iniciando _actualizar_datos_existentes con {len(df_base)} filas.") # PUNTO DE CONTROL

        # --- Limpieza Preventiva ---
        print("   - 🛡️  Limpiando preventivamente columnas numéricas que contienen texto...")
        columnas_a_limpiar = ['Saldo_Avales', 'Saldo_Interes_Corriente', 'Saldo_Capital', 'Saldo_Factura']
        for col in columnas_a_limpiar:
            if col in df_base.columns:
                df_base[col] = pd.to_numeric(df_base[col], errors='coerce').fillna(0)
        
        config_actualizaciones = { 
                    "ANALISIS":{
                        "llave": "Credito", "columnas": ['Dias_Atraso', 'Cuotas_Pagadas', 'Saldo_Factura']},
                    "R03": {
                        "llave": "Cedula_Cliente", "columnas": '__ALL__'},
                    "MATRIZ_CARTERA": {
                        "llave": "Zona", "columnas": '__ALL__'},
                    "METAS_FRANJAS": {
                        "llave": "Zona", "columnas": '__ALL__'},
                    "VENCIMIENTOS": {
                        "llave": "Credito", "columnas": '__ALL__'},
                    "ASESORES": {"sheets": [{"sheet_name": "ASESORES", "llave": "Codigo_Vendedor", "columnas": ['Nombre_Vendedor', 'Estado_Vendedor']}, {"sheet_name": "Centro Costos", "llave": "Codigo_Centro_Costos", "columnas": ['Nombre_Centro_Costos']}]},
                    # ---- AÑADIDOS AL BUCLE PRINCIPAL ----
                    "CRTMPCONSULTA1": {"llave": "Credito", "columnas": '__ALL__'},
                    "FNZ001": {"llave": "Credito", "columnas": '__ALL__'},
                    "SC04": {"llave": "Factura_Venta", "columnas": '__ALL__'}
                    }

        # --- PASO 1: Actualizar con datos de archivos nuevos (tu lógica actual) ---
        for tipo_archivo, config in config_actualizaciones.items():
            # ... (este bucle for se mantiene exactamente como lo tienes, no hay que cambiarlo) ...
            archivos_nuevos = dataframes_nuevos.get(tipo_archivo, [])
            if not archivos_nuevos: continue
            print(f"   - Aplicando actualizaciones desde '{tipo_archivo}'...")
            if "sheets" in config:
                for sheet_config in config["sheets"]:
                    df_hoja = next((item['data'] for item in archivos_nuevos if item.get('sheet_name') == sheet_config['sheet_name']), pd.DataFrame())
                    if not df_hoja.empty:
                        df_base = self._unir_y_actualizar_columnas(df_base, df_hoja, sheet_config['llave'], sheet_config['columnas'])
            else:
                df_nuevo_datos = self.data_loader.safe_concat(archivos_nuevos)
                if tipo_archivo == 'R03' and 'CEDULA' in df_nuevo_datos.columns:
                    df_nuevo_datos.rename(columns={'CEDULA': 'Cedula_Cliente'}, inplace=True)
                if not df_nuevo_datos.empty:
                    if config['llave'] == 'Credito' and 'Credito' not in df_nuevo_datos.columns:
                        df_nuevo_datos = self.data_loader.create_credit_key(df_nuevo_datos)
                    df_base = self._unir_y_actualizar_columnas(df_base, df_nuevo_datos, config['llave'], config['columnas'])


        # --- PASO 2: Aplicar la SECUENCIA COMPLETA de transformaciones (LA PARTE QUE FALTABA) ---
        print("\n🚀 Aplicando secuencia completa de transformaciones y enriquecimientos...")

        # Cargar los dataframes necesarios para las transformaciones
        crtmp_df = self.data_loader.create_credit_key(self.data_loader.safe_concat(dataframes_nuevos.get("CRTMPCONSULTA1", [])))
        sc04_df = self.data_loader.safe_concat(dataframes_nuevos.get("SC04", []))
        fnz001_df = self.data_loader.create_credit_key(self.data_loader.safe_concat(dataframes_nuevos.get("FNZ001", [])))
        fnz003_df = self.data_loader.create_credit_key(self.data_loader.safe_concat(dataframes_nuevos.get("FNZ003", [])))
        metas_franjas_df = self.data_loader.safe_concat(dataframes_nuevos.get("METAS_FRANJAS", []))
        
        # Ejecutar cada paso en el orden correcto
        df_base['Empresa'] = np.where(df_base['Tipo_Credito'] == 'DF', 'FINANSUEÑOS', 'ARPESOD')
        df_base = self.report_service.products_sales.assign_sales_invoice(df_base, crtmp_df)
        df_base = self.report_service.products_sales.add_product_details(df_base, crtmp_df)
        df_base = self.report_service.credit_details.enrich_credit_details(df_base, sc04_df, fnz001_df)
        df_base = self.report_service.credit_details.clean_installment_data(df_base)
        df_base = self.report_service.report_processor.map_call_center_data(df_base)
        df_base, negativos_fnz003 = self.report_service.report_processor.calculate_balances(df_base, fnz003_df)
        df_base = self.report_service.report_processor.calculate_goal_metrics(df_base, metas_franjas_df)
        df_base = self.report_service.credit_details.adjust_arrears_status(df_base)
        
        return df_base, negativos_fnz003
    
    def sincronizar_reporte(self, df_base_anterior, dataframes_nuevos):
        print("🚀 Iniciando modo de actualización directa y simplificada...")

        # --- PASO 1: Preparar las fuentes de datos ---
        
        # a) Limpiar el reporte anterior para usarlo como una tabla de consulta única y fiable.
        print(f"   - Limpiando reporte base anterior... Registros iniciales: {len(df_base_anterior)}")
        df_base_anterior.dropna(subset=['Credito'], inplace=True)
        df_base_anterior['Credito'] = df_base_anterior['Credito'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
        df_base_anterior = df_base_anterior[df_base_anterior['Credito'] != '']
        df_base_anterior.drop_duplicates(subset=['Credito'], keep='first', inplace=True)
        print(f"   - Registros únicos en base anterior para consulta: {len(df_base_anterior)}")

        # b) Cargar el R91, que es nuestra estructura final y "verdad absoluta".
        reporte_final_sync = self.data_loader.create_credit_key(self.data_loader.safe_concat(dataframes_nuevos.get("R91", [])))
        if reporte_final_sync.empty:
            raise ValueError("No se encontraron datos en los nuevos archivos R91.")
        reporte_final_sync['Credito'] = reporte_final_sync['Credito'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
        print(f"   - Reporte base creado desde R91. El número de filas final será: {len(reporte_final_sync)}")

        # --- PASO 2: Enriquecer la base del R91 con la información histórica ---
        print("   - Rescatando información histórica del reporte anterior...")
        # Identificamos las columnas del reporte viejo que no están en el R91 para traerlas.
        columnas_a_rescatar = df_base_anterior.columns.difference(reporte_final_sync.columns).tolist()
        columnas_a_rescatar.append('Credito') # Añadimos la llave para el cruce.

        # Hacemos un 'left merge'. Esto mantiene las 18,716 filas del R91 y rellena la data.
        reporte_final_sync = pd.merge(
            reporte_final_sync,
            df_base_anterior[columnas_a_rescatar],
            on='Credito',
            how='left'
        )

        reporte_final_sync, negativos_reporte = self._actualizar_datos_existentes(reporte_final_sync, dataframes_nuevos)
        
        # --- PASO 4: Finalizar el reporte ---
        print("\nConsolidando y finalizando el reporte...")
        reporte_final_sync, df_a_corregir = self.report_service.report_processor.finalize_report(
            reporte_final_sync,
            ORDEN_COLUMNAS_FINAL
        )

        # --- Lógica para crear el reporte de negativos ---
        print("📊 Unificando reportes de créditos con valores negativos...")
        reporte_negativos_final = pd.DataFrame()
        if not negativos_reporte.empty:
            info_clientes = reporte_final_sync[['Credito', 'Cedula_Cliente', 'Nombre_Cliente']].drop_duplicates(subset=['Credito'])
            reporte_negativos_final = pd.merge(negativos_reporte, info_clientes, on='Credito', how='left')
            columnas_finales = ['Credito', 'Cedula_Cliente', 'Nombre_Cliente', 'Observacion']
            columnas_existentes = [col for col in columnas_finales if col in reporte_negativos_final.columns]
            reporte_negativos_final = reporte_negativos_final[columnas_existentes].drop_duplicates()

        print(f"\n✅ Proceso de sincronización completado. Registros finales: {len(reporte_final_sync)}")
        return reporte_final_sync, reporte_negativos_final, df_a_corregir
    
    def _filtrar_dataframes_por_creditos(self, dataframes_por_tipo, creditos_a_procesar, df_r91_nuevos):
        dataframes_filtrados = {}
        df_r91_filtrado = df_r91_nuevos[df_r91_nuevos['Credito'].isin(creditos_a_procesar)]
        cedulas_nuevas = set(df_r91_filtrado['Cedula_Cliente'].unique())

        for tipo, lista_dfs in dataframes_por_tipo.items():
            if not lista_dfs: continue
            
            # Manejo especial para ASESORES
            if tipo == "ASESORES":
                sheets_filtradas = []
                for item in lista_dfs:
                    df_hoja = item['data']
                    # Aquí no filtramos porque las hojas de asesores no se relacionan directamente con 'Credito' o 'Cedula'
                    sheets_filtradas.append({"data": df_hoja, "config": item["config"], "sheet_name": item.get("sheet_name")})
                if sheets_filtradas:
                    dataframes_filtrados[tipo] = sheets_filtradas
                continue

            df_concatenado = self.data_loader.safe_concat(lista_dfs)
            df_filtrado = pd.DataFrame()
            
            if 'Credito' in df_concatenado.columns:
                df_filtrado = df_concatenado[df_concatenado['Credito'].isin(creditos_a_procesar)]
            elif 'Cedula_Cliente' in df_concatenado.columns:
                 df_filtrado = df_concatenado[df_concatenado['Cedula_Cliente'].isin(cedulas_nuevas)]
            else:
                df_filtrado = df_concatenado
            
            if not df_filtrado.empty:
                config = lista_dfs[0].get("config") if lista_dfs else None
                dataframes_filtrados[tipo] = [{"data": df_filtrado, "config": config}]

        return dataframes_filtrados