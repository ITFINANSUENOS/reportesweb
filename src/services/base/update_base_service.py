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
        Une dos DataFrames por una llave y actualiza de forma robusta,
        eliminando primero las columnas viejas para evitar conflictos.
        """
        if llave not in df_principal.columns or llave not in df_actualizacion.columns:
            print(f"   - ⚠️ Advertencia: La llave '{llave}' no se encontró. Se omite la actualización.")
            return df_principal

        # --- Limpieza de llaves (sin cambios) ---
        df_actualizacion_limpio = df_actualizacion.drop_duplicates(subset=[llave]).copy()
        df_principal[llave] = df_principal[llave].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
        df_actualizacion_limpio[llave] = df_actualizacion_limpio[llave].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
        df_principal = df_principal[df_principal[llave].notna() & (df_principal[llave] != '')]
        df_actualizacion_limpio = df_actualizacion_limpio[df_actualizacion_limpio[llave].notna() & (df_actualizacion_limpio[llave] != '')]

        # --- LÓGICA DE UNIÓN SIMPLIFICADA Y CORREGIDA ---
        
        # 1. Determinar las columnas a reemplazar del archivo de actualización
        if columnas_a_actualizar == '__ALL__':
            columnas_a_reemplazar = df_actualizacion_limpio.columns.drop(llave).tolist() # Convertimos a lista aquí
        else:
            columnas_a_reemplazar = [col for col in columnas_a_actualizar if col in df_actualizacion_limpio.columns and col != llave]

        # 2. Eliminar esas columnas del DataFrame principal para hacer espacio limpio
        df_principal_limpio = df_principal.drop(columns=columnas_a_reemplazar, errors='ignore')
        
        # 3. Construir la lista final de columnas del dataframe de actualización
        #    Esta es la forma más segura de construir la lista para el merge.
        columnas_para_unir = [llave] + columnas_a_reemplazar
        
        # 4. Hacer un merge simple. 
        df_actualizado = pd.merge(
            df_principal_limpio,
            df_actualizacion_limpio[columnas_para_unir],
            on=llave,
            how='left'
        )
        
        return df_actualizado

    def _actualizar_datos_existentes(self, df_base, dataframes_nuevos):
        """
        Aplica la secuencia COMPLETA de enriquecimiento, replicando el flujo de 'generate_consolidated_report'
        para garantizar la consistencia total de los datos.
        """
        print("\n🔄 Sincronizando datos con el flujo de creación de reporte...")

        # --- PASO 1: Cargar todos los dataframes de datos nuevos ---
        print("   - Cargando todos los archivos de datos para la actualización...")
        analisis_df = self.data_loader.create_credit_key(self.data_loader.safe_concat(dataframes_nuevos.get("ANALISIS", [])))
        vencimientos_df = self.data_loader.create_credit_key(self.data_loader.safe_concat(dataframes_nuevos.get("VENCIMIENTOS", [])))
        crtmp_df = self.data_loader.create_credit_key(self.data_loader.safe_concat(dataframes_nuevos.get("CRTMPCONSULTA1", [])))
        sc04_df = self.data_loader.safe_concat(dataframes_nuevos.get("SC04", []))
        fnz001_df = self.data_loader.create_credit_key(self.data_loader.safe_concat(dataframes_nuevos.get("FNZ001", [])))
        fnz003_df = self.data_loader.create_credit_key(self.data_loader.safe_concat(dataframes_nuevos.get("FNZ003", [])))
        metas_franjas_df = self.data_loader.safe_concat(dataframes_nuevos.get("METAS_FRANJAS", []))
        r03_df = self.data_loader.safe_concat(dataframes_nuevos.get("R03", []))
        matriz_cartera_df = self.data_loader.safe_concat(dataframes_nuevos.get("MATRIZ_CARTERA", []))
        asesores_sheets = dataframes_nuevos.get("ASESORES", [])

        # --- PASO 2: Procesar y unir datos en el orden correcto ---
        print("\n🔍 Uniendo y actualizando datos en secuencia...")

        # Vencimientos
        processed_vencimientos, negativos_vencimientos = self.report_service.credit_details.process_vencimientos_data(vencimientos_df)
        if not processed_vencimientos.empty:
            df_base = df_base.drop(columns=processed_vencimientos.columns.drop('Credito'), errors='ignore')
            df_base = pd.merge(df_base, processed_vencimientos, on='Credito', how='left')
        
        # Analisis
        if not analisis_df.empty:
            df_base = df_base.drop(columns=analisis_df.columns.drop('Credito'), errors='ignore')
            df_base = pd.merge(df_base, analisis_df.drop_duplicates('Credito'), on='Credito', how='left')

        # R03 (Codeudores)
        if not r03_df.empty:
            if 'CEDULA' in r03_df.columns: r03_df.rename(columns={'CEDULA': 'Cedula_Cliente'}, inplace=True)
            old_cols = r03_df.columns.drop('Cedula_Cliente')
            df_base = df_base.drop(columns=old_cols, errors='ignore')
            df_base = pd.merge(df_base, r03_df.drop_duplicates('Cedula_Cliente'), on='Cedula_Cliente', how='left')

        # Matriz Cartera
        if not matriz_cartera_df.empty:
            old_cols = matriz_cartera_df.columns.drop('Zona')
            df_base = df_base.drop(columns=old_cols, errors='ignore')
            df_base = pd.merge(df_base, matriz_cartera_df.drop_duplicates('Zona'), on='Zona', how='left')

        # Asesores (y demás uniones que necesites replicar)
        if asesores_sheets:
            for item in asesores_sheets:
                info_df = item["data"]
                merge_key = item["config"]["merge_on"]
                if not info_df.empty and merge_key in df_base.columns:
                    old_cols = info_df.columns.drop(merge_key)
                    df_base = df_base.drop(columns=old_cols, errors='ignore')
                    df_base = pd.merge(df_base, info_df.drop_duplicates(subset=merge_key), on=merge_key, how='left')

        # --- PASO 3: Aplicar la secuencia de transformaciones ---
        print("\n🚀 Aplicando secuencia completa de transformaciones y enriquecimientos...")
        df_base['Empresa'] = np.where(df_base['Tipo_Credito'] == 'DF', 'FINANSUEÑOS', 'ARPESOD')
        df_base = self.report_service.products_sales.assign_sales_invoice(df_base, crtmp_df)
        
        # Esta es la línea que fallaba. Ahora funcionará porque el merge de CRTMPCONSULTA1
        # no se hace aquí, sino que la función usa el crtmp_df que ya cargamos.
        df_base = self.report_service.products_sales.add_product_details(df_base, crtmp_df)
        
        df_base = self.report_service.credit_details.enrich_credit_details(df_base, sc04_df, fnz001_df)
        df_base = self.report_service.credit_details.clean_installment_data(df_base)
        df_base = self.report_service.report_processor.map_call_center_data(df_base)
        df_base, negativos_fnz003 = self.report_service.report_processor.calculate_balances(df_base, fnz003_df)
        df_base = self.report_service.report_processor.calculate_goal_metrics(df_base, metas_franjas_df)
        df_base = self.report_service.credit_details.adjust_arrears_status(df_base)
        
        # Combinar todos los dataframes de negativos que se generaron
        negativos_finales = pd.DataFrame()
        lista_negativos = [df for df in [negativos_vencimientos, negativos_fnz003] if not df.empty]
        if lista_negativos:
            negativos_finales = pd.concat(lista_negativos, ignore_index=True)
            
        return df_base, negativos_finales
    
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