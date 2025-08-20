import pandas as pd
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
        Une dos DataFrames por una llave y actualiza columnas específicas o todas.
        Ahora estandariza las llaves para evitar errores de tipo de dato.
        """
        # Verificación de seguridad: si las llaves no existen, no hacer nada.
        if llave not in df_principal.columns or llave not in df_actualizacion.columns:
            print(f"   - ⚠️ Advertencia: La llave '{llave}' no se encontró en ambos DataFrames. Se omite esta actualización.")
            return df_principal

        df_actualizacion_limpio = df_actualizacion.drop_duplicates(subset=[llave]).copy()
        
         # --- LÓGICA DE LIMPIEZA AVANZADA ---
        # 1. Convertir a numérico primero, forzando errores a NaN (no un número)
        # Esto elimina textos como 'NO APLICA'
        df_principal[llave] = pd.to_numeric(df_principal[llave], errors='coerce')
        df_actualizacion_limpio.loc[:, llave] = pd.to_numeric(df_actualizacion_limpio[llave], errors='coerce')

        # 2. Eliminar filas donde la llave no sea un número válido
        df_principal.dropna(subset=[llave], inplace=True)
        df_actualizacion_limpio.dropna(subset=[llave], inplace=True)

        # 3. Convertir a entero para quitar decimales, y luego a texto para la unión.
        # Esto estandariza todo a un formato de texto limpio (ej. "12345")
        df_principal[llave] = df_principal[llave].astype('int64').astype(str)
        df_actualizacion_limpio.loc[:, llave] = df_actualizacion_limpio.loc[:, llave].astype('int64').astype(str)
        
        if columnas_a_actualizar == '__ALL__':
            cols_a_reemplazar = df_actualizacion_limpio.columns.drop(llave)
            df_principal_sin_viejos_datos = df_principal.drop(columns=cols_a_reemplazar, errors='ignore')
            df_actualizado = pd.merge(df_principal_sin_viejos_datos, df_actualizacion_limpio, on=llave, how='left')
            return df_actualizado

        df_fusionado = pd.merge(df_principal, df_actualizacion_limpio, on=llave, how='left', suffixes=('_anterior', '_nuevo'))
        for col in columnas_a_actualizar:
            col_anterior = col + '_anterior'
            col_nuevo = col + '_nuevo'
            if col_nuevo in df_fusionado.columns:
                df_fusionado[col] = df_fusionado[col_nuevo].fillna(df_fusionado[col_anterior])
                df_fusionado.drop(columns=[col_anterior, col_nuevo], inplace=True)
        
        return df_fusionado

    def _actualizar_datos_existentes(self, df_base, dataframes_nuevos):
        """
        Aplica las transformaciones y cruces de datos necesarios sobre la base existente.
        """
        print("\n🔄 Actualizando información de créditos existentes...")
        
        config_actualizaciones = {
            "ANALISIS": {
                "llave": "Credito",
                "columnas": ['Dias_Atraso', 'Cuotas_Pagadas', 'Saldo_Factura']
            },
            "R03": {
                "llave": "Cedula_Cliente",
                "columnas": '__ALL__'  # Reemplaza toda la info de codeudores
            },
            "MATRIZ_CARTERA": {
                "llave": "Zona",
                "columnas": '__ALL__'
            },
            "METAS_FRANJAS":{
                "llave":"Zona",
                "columnas":'__ALL__'
            },
            "VENCIMIENTOS":{
                "llave":"Credito",
                "columnas":'__ALL__'
            },
            "ASESORES": {
                # Caso especial con múltiples hojas
                "sheets": [
                    { "sheet_name": "ASESORES", "llave": "Codigo_Vendedor", "columnas": '__ALL__' },
                    { "sheet_name": "Centro Costos", "llave": "Codigo_Centro_Costos", "columnas": '__ALL__' }
                ]
            },
            # Añade aquí otros archivos que necesiten actualización
        }

        # --- Proceso automatizado de actualización ---
        for tipo_archivo, config in config_actualizaciones.items():
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
                 # Solución para el problema de R03: Si la columna es 'CEDULA', la renombramos aquí.
                if tipo_archivo == 'R03' and 'CEDULA' in df_nuevo_datos.columns:
                    df_nuevo_datos.rename(columns={'CEDULA': 'Cedula_Cliente'}, inplace=True)
                
                if not df_nuevo_datos.empty:
                    if config['llave'] == 'Credito' and 'Credito' not in df_nuevo_datos.columns:
                         df_nuevo_datos = self.data_loader.create_credit_key(df_nuevo_datos)
                    df_base = self._unir_y_actualizar_columnas(df_base, df_nuevo_datos, config['llave'], config['columnas'])
        
        print("   - ✨ Enriqueciendo detalles de crédito (SC04 y FNZ001)...")
        sc04_df = self.data_loader.safe_concat(dataframes_nuevos.get("SC04", []))
        fnz001_df = self.data_loader.create_credit_key(self.data_loader.safe_concat(dataframes_nuevos.get("FNZ001", [])))
        df_base = self.report_service.credit_details.enrich_credit_details(df_base, sc04_df, fnz001_df)

        # vencimientos_df = self.data_loader.create_credit_key(self.data_loader.safe_concat(dataframes_nuevos.get("VENCIMIENTOS", [])))
        # if not vencimientos_df.empty:
        #     print("   - Recalculando desde 'VENCIMIENTOS'...")
        #     processed_vencimientos, _ = self.report_service.credit_details.process_vencimientos_data(vencimientos_df)
        #     if not processed_vencimientos.empty:
        #         cols_vencimientos = processed_vencimientos.columns.drop('Credito')
        #         df_base = df_base.drop(columns=cols_vencimientos, errors='ignore')
        #         df_base = pd.merge(df_base, processed_vencimientos, on='Credito', how='left')
        
        fnz003_df = self.data_loader.create_credit_key(self.data_loader.safe_concat(dataframes_nuevos.get("FNZ003", [])))
        if not fnz003_df.empty:
            print("   - Recalculando balances desde 'FNZ003'...")
            df_base, _ = self.report_service.report_processor.calculate_balances(df_base, fnz003_df)

        print("   - Ejecutando transformaciones finales...")
        df_base = self.report_service.credit_details.adjust_arrears_status(df_base)
        return df_base
    
    def sincronizar_reporte(self, df_base_anterior, dataframes_nuevos):
        print("🚀 Iniciando modo de actualización rápida...")

        df_r91_nuevo = self.data_loader.create_credit_key(self.data_loader.safe_concat(dataframes_nuevos.get("R91", [])))
        if df_r91_nuevo.empty:
            raise ValueError("No se encontraron datos en los nuevos archivos R91. No se puede actualizar.")

        creditos_anteriores = set(df_base_anterior['Credito'].unique())
        creditos_nuevos_r91 = set(df_r91_nuevo['Credito'].unique())

        creditos_a_eliminar = creditos_anteriores - creditos_nuevos_r91
        creditos_a_agregar = creditos_nuevos_r91 - creditos_anteriores
        creditos_a_mantener_y_actualizar = creditos_anteriores.intersection(creditos_nuevos_r91)

        print(f"   - Créditos a eliminar (pagados): {len(creditos_a_eliminar)}")
        print(f"   - Créditos nuevos a agregar: {len(creditos_a_agregar)}")
        print(f"   - Créditos a mantener y actualizar: {len(creditos_a_mantener_y_actualizar)}")

        df_actualizado = df_base_anterior[df_base_anterior['Credito'].isin(creditos_a_mantener_y_actualizar)].copy()
        df_actualizado = self._actualizar_datos_existentes(df_actualizado, dataframes_nuevos)
        
        df_nuevos_procesados = pd.DataFrame()
        if creditos_a_agregar:
            print("\nProcesando solo los créditos nuevos...")
            dataframes_solo_nuevos = self._filtrar_dataframes_por_creditos(dataframes_nuevos, creditos_a_agregar, df_r91_nuevo)
            df_nuevos_procesados, _, _ = self.report_service.generate_consolidated_report(
                file_paths=None, 
                orden_columnas=[],
                dataframes_preloaded=dataframes_solo_nuevos  # <-- SOLUCIONADO
            )
        
        print("\nConsolidando reporte final...")
        reporte_final_sync = pd.concat([df_actualizado, df_nuevos_procesados], ignore_index=True)
        
        reporte_final_sync, df_a_corregir = self.report_service.report_processor.finalize_report(
            reporte_final_sync,
            ORDEN_COLUMNAS_FINAL 
        )

        return reporte_final_sync, df_a_corregir
    
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