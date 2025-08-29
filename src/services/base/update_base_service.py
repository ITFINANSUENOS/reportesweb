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

        # --- LÓGICA DE ACTUALIZACIÓN MEJORADA ---
        # 1. Identificar las columnas a actualizar. Si es '__ALL__', son todas las del nuevo archivo.
        #    Si no, usamos la lista que nos pasan.
        if columnas_a_actualizar == '__ALL__':
            cols_para_actualizar = df_actualizacion_limpio.columns.drop(llave)
        else:
            cols_para_actualizar = [col for col in columnas_a_actualizar if col in df_actualizacion_limpio.columns]

        # 2. Preparamos el DF principal para la actualización.
        #    Nos aseguramos de que la llave sea el índice para una actualización eficiente.
        df_principal = df_principal.set_index(llave)
        df_actualizacion_limpio = df_actualizacion_limpio.set_index(llave)

        # 3. Usamos update(): una forma potente de "pegar" datos nuevos sobre los viejos.
        #    Esto reemplaza los valores de df_principal con los de df_actualizacion_limpio
        #    para las columnas y filas (índice) que coincidan. No crea sufijos.
        df_principal.update(df_actualizacion_limpio[cols_para_actualizar])

        # 4. Devolvemos el DataFrame con el índice reseteado a como estaba.
        return df_principal.reset_index()

    def _actualizar_datos_existentes(self, df_base, dataframes_nuevos):
        """
        Aplica las transformaciones y cruces de datos necesarios sobre la base existente.
        """
        print("\n🔄 Actualizando información de créditos existentes...")

        # --- INICIO DEL BLOQUE DE LIMPIEZA PREVENTIVA ---
        # Movimos este bloque al inicio para asegurar que df_base esté limpio
        # ANTES de pasarlo a cualquier otra función. Esta es la clave.
        print("   - 🛡️  Limpiando preventivamente columnas numéricas que contienen texto...")
        columnas_a_limpiar = ['Saldo_Avales', 'Saldo_Interes_Corriente', 'Saldo_Capital', 'Saldo_Factura']
        for col in columnas_a_limpiar:
            if col in df_base.columns:
                # Forzamos la conversión a número, los errores se vuelven Nulos (NaN)
                # y luego rellenamos esos nulos con 0, dejando la columna 100% numérica.
                df_base[col] = pd.to_numeric(df_base[col], errors='coerce').fillna(0)
        # --- FIN DEL BLOQUE DE LIMPIEZA ---

        config_actualizaciones = {
            "ANALISIS": {
                "llave": "Credito",
                "columnas": ['Dias_Atraso', 'Cuotas_Pagadas', 'Saldo_Factura']
            },
            "R03": {
                "llave": "Cedula_Cliente",
                "columnas": '__ALL__'
            },
            "MATRIZ_CARTERA": {
                "llave": "Zona",
                "columnas": '__ALL__'
            },
            "METAS_FRANJAS": {
                "llave": "Zona",
                "columnas": '__ALL__'
            },
            "VENCIMIENTOS": {
                "llave": "Credito",
                "columnas": '__ALL__'
            },
            "ASESORES": {
                "sheets": [
                    { "sheet_name": "ASESORES", "llave": "Codigo_Vendedor", "columnas": '__ALL__' },
                    { "sheet_name": "Centro Costos", "llave": "Codigo_Centro_Costos", "columnas": '__ALL__' }
                ]
            },
        }

        # --- Proceso automatizado de actualización (sin cambios) ---
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
        
        fnz003_df = self.data_loader.create_credit_key(self.data_loader.safe_concat(dataframes_nuevos.get("FNZ003", [])))
        # Inicializamos negativos_fnz003 por si el if no se cumple
        negativos_fnz003 = pd.DataFrame()
        if not fnz003_df.empty:
            print("   - Recalculando balances desde 'FNZ003'...")
            # Ahora esta función recibirá un df_base limpio y no debería fallar.
            df_base, negativos_fnz003 = self.report_service.report_processor.calculate_balances(df_base, fnz003_df)

        print("   - Ejecutando transformaciones finales...")
        df_base = self.report_service.credit_details.adjust_arrears_status(df_base)
        return df_base, negativos_fnz003
    
    def sincronizar_reporte(self, df_base_anterior, dataframes_nuevos):
        print("🚀 Iniciando modo de actualización rápida...")
        
         # --- INICIO DEL NUEVO BLOQUE DE LIMPIEZA INICIAL ---
        print(f"   - Limpiando y validando el reporte base anterior... Registros iniciales: {len(df_base_anterior)}")
        df_base_anterior.dropna(subset=['Credito'], inplace=True)
        df_base_anterior['Credito'] = df_base_anterior['Credito'].astype(str)
        df_base_anterior = df_base_anterior[df_base_anterior['Credito'].str.strip() != '']
        print(f"   - Registros válidos en el reporte base: {len(df_base_anterior)}")
        # --- FIN DEL NUEVO BLOQUE DE LIMPIEZA INICIAL ---

        df_r91_nuevo = self.data_loader.create_credit_key(self.data_loader.safe_concat(dataframes_nuevos.get("R91", [])))
        if df_r91_nuevo.empty:
            raise ValueError("No se encontraron datos en los nuevos archivos R91. No se puede actualizar.")

        creditos_anteriores = set(df_base_anterior['Credito'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True))
        creditos_nuevos_r91 = set(df_r91_nuevo['Credito'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True))

        creditos_a_eliminar = creditos_anteriores - creditos_nuevos_r91
        creditos_a_agregar = creditos_nuevos_r91 - creditos_anteriores
        creditos_a_mantener_y_actualizar = creditos_anteriores.intersection(creditos_nuevos_r91)

        print(f"   - Créditos a eliminar (pagados): {len(creditos_a_eliminar)}")
        print(f"   - Créditos nuevos a agregar: {len(creditos_a_agregar)}")
        print(f"   - Créditos a mantener y actualizar: {len(creditos_a_mantener_y_actualizar)}")

        df_actualizado = df_base_anterior[df_base_anterior['Credito'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True).isin(creditos_a_mantener_y_actualizar)].copy()
        
        # AHORA CAPTURAMOS LOS NEGATIVOS DE LOS CRÉDITOS EXISTENTES
        df_actualizado, negativos_existentes = self._actualizar_datos_existentes(df_actualizado, dataframes_nuevos)
        
        df_nuevos_procesados = pd.DataFrame()
        negativos_nuevos = pd.DataFrame() # Inicia vacío
        if creditos_a_agregar:
            print("\nProcesando solo los créditos nuevos...")
            dataframes_solo_nuevos = self._filtrar_dataframes_por_creditos(dataframes_nuevos, creditos_a_agregar, df_r91_nuevo)
            
            # AHORA CAPTURAMOS LOS NEGATIVOS DE LOS CRÉDITOS NUEVOS
            df_nuevos_procesados, negativos_nuevos, _ = self.report_service.generate_consolidated_report(
                file_paths=None,
                orden_columnas=[],
                dataframes_preloaded=dataframes_solo_nuevos
            )
        
        print("\nConsolidando reporte final...")
        reporte_final_sync = pd.concat([df_actualizado, df_nuevos_procesados], ignore_index=True)
        
        reporte_final_sync, df_a_corregir = self.report_service.report_processor.finalize_report(
            reporte_final_sync,
            ORDEN_COLUMNAS_FINAL
        )

        # --- LÓGICA FINAL PARA CREAR REPORTE DE NEGATIVOS ---
        print("📊 Unificando reportes de créditos con valores negativos...")
        reporte_negativos_final = pd.DataFrame()
        lista_de_negativos = [df for df in [negativos_existentes, negativos_nuevos] if not df.empty]

        if lista_de_negativos:
            todos_los_negativos = pd.concat(lista_de_negativos, ignore_index=True)
            
            info_clientes = reporte_final_sync[['Credito', 'Cedula_Cliente', 'Nombre_Cliente']].drop_duplicates(subset=['Credito'])
            info_clientes['Credito'] = info_clientes['Credito'].astype(str)
            todos_los_negativos['Credito'] = todos_los_negativos['Credito'].astype(str)

            reporte_negativos_final = pd.merge(
                todos_los_negativos,
                info_clientes,
                on='Credito',
                how='left'
            )
            columnas_finales_negativos = ['Credito', 'Cedula_Cliente', 'Nombre_Cliente', 'Observacion']
            columnas_existentes = [col for col in columnas_finales_negativos if col in reporte_negativos_final.columns]
            reporte_negativos_final = reporte_negativos_final[columnas_existentes].drop_duplicates()

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