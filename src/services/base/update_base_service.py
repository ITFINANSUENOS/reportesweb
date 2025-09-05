import pandas as pd
import numpy as np
from src.models.base_model import ORDEN_COLUMNAS_FINAL

class UpdateBaseService:
    """
    Servicio que actualiza un reporte base reutilizando la lógica de ReportService,
    ahora con validaciones más robustas para evitar errores por DataFrames incompletos.
    """
    def __init__(self, report_service):
        self.report_service = report_service
        self.data_loader = report_service.data_loader

    def sincronizar_reporte(self, df_base_anterior, dataframes_nuevos):
        print("🚀 Iniciando modo de actualización inteligente...")

        print("\n[LOG] DataFrames nuevos recibidos:")
        for tipo, lista in dataframes_nuevos.items():
            if lista:
                df = self.data_loader.safe_concat(lista)
                print(f"   - {tipo}: {df.shape} columnas {list(df.columns)}")

        # --- PASO 1: Identificar cambios ---
        print("   - Analizando cambios en los créditos...")
        df_r91_nuevo = self.data_loader.create_credit_key(
            self.data_loader.safe_concat(dataframes_nuevos.get("R91", []))
        )
        if df_r91_nuevo.empty:
            raise ValueError("El archivo R91 es obligatorio para la actualización.")

        df_base_anterior_limpia = df_base_anterior.drop_duplicates(subset=['Credito']).copy()
        creditos_viejos = set(df_base_anterior_limpia['Credito'].dropna())
        creditos_nuevos_en_r91 = set(df_r91_nuevo['Credito'].dropna())

        creditos_a_agregar = creditos_nuevos_en_r91 - creditos_viejos
        creditos_a_mantener = creditos_nuevos_en_r91.intersection(creditos_viejos)
        
        print(f"   - Créditos nuevos a procesar: {len(creditos_a_agregar)}")
        print(f"   - Créditos a mantener y actualizar: {len(creditos_a_mantener)}")

        # --- PASO 2: Créditos nuevos ---
        df_nuevos_procesados, negativos_nuevos, df_correcciones_nuevos = (
            self._procesar_creditos_nuevos(creditos_a_agregar, dataframes_nuevos)
        )

        # --- PASO 3: Créditos mantenidos ---
        df_mantenidos_actualizados, negativos_mantenidos, df_correcciones_mantenidos = (
            self._procesar_creditos_mantenidos(creditos_a_mantener, df_base_anterior, df_r91_nuevo, dataframes_nuevos)
        )
        
        # --- PASO 4: Consolidar ---
        print("\nConsolidando reporte final...")
        reporte_df = pd.concat([df_mantenidos_actualizados, df_nuevos_procesados], ignore_index=True)
        reporte_negativos_final = pd.concat([negativos_nuevos, negativos_mantenidos], ignore_index=True)
        df_a_corregir_final = pd.concat([df_correcciones_nuevos, df_correcciones_mantenidos], ignore_index=True)

        print(f"\n✅ Proceso de sincronización completado. Registros finales: {len(reporte_df)}")
        return reporte_df, reporte_negativos_final, df_a_corregir_final

    def _procesar_creditos_nuevos(self, creditos_a_agregar, dataframes_nuevos):
        if not creditos_a_agregar:
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

        print("\n🆕 Procesando créditos nuevos...")
        dataframes_filtrados = self._filtrar_dataframes_por_creditos(dataframes_nuevos, creditos_a_agregar)
        
        return self.report_service.generate_consolidated_report(
            file_paths=None, 
            orden_columnas=ORDEN_COLUMNAS_FINAL,
            dataframes_preloaded=dataframes_filtrados
        )

    def _procesar_creditos_mantenidos(self, creditos_a_mantener, df_base_anterior, df_r91_nuevo, dataframes_nuevos):
        if not creditos_a_mantener:
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

        print("\n🔄 Actualizando créditos existentes...")
        df_mantenidos_base = df_r91_nuevo[df_r91_nuevo['Credito'].isin(creditos_a_mantener)].copy()

        columnas_a_rescatar = df_base_anterior.columns.difference(df_mantenidos_base.columns).tolist()
        columnas_a_rescatar.append('Credito')
        df_mantenidos_enriquecido = pd.merge(
            df_mantenidos_base,
            df_base_anterior[columnas_a_rescatar].drop_duplicates(subset=['Credito']),
            on='Credito',
            how='left'
        )

        reporte_actualizado, negativos, correcciones = self._aplicar_transformaciones(
            df_mantenidos_enriquecido, dataframes_nuevos
        )
        
        reporte_actualizado_final, correcciones_final = (
            self.report_service.report_processor.finalize_report(reporte_actualizado, ORDEN_COLUMNAS_FINAL)
        )

        return reporte_actualizado_final, negativos, correcciones_final

    def _aplicar_transformaciones(self, reporte_df, dataframes_nuevos):
        print("\n🔎 [LOG] Entrando a _aplicar_transformaciones")
        print(f"   - Registros iniciales en reporte_df: {len(reporte_df)}")
        print(f"   - Columnas iniciales: {list(reporte_df.columns)}")
        
        # Cargar todos los DataFrames
        analisis_df = self.data_loader.create_credit_key(self.data_loader.safe_concat(dataframes_nuevos.get("ANALISIS", [])))
        print(f"   📂 ANALISIS -> {analisis_df.shape} columnas: {list(analisis_df.columns)}")

        vencimientos_df = self.data_loader.create_credit_key(self.data_loader.safe_concat(dataframes_nuevos.get("VENCIMIENTOS", [])))
        print(f"   📂 VENCIMIENTOS -> {vencimientos_df.shape} columnas: {list(vencimientos_df.columns)}")

        crtmp_df = self.data_loader.create_credit_key(self.data_loader.safe_concat(dataframes_nuevos.get("CRTMPCONSULTA1", [])))
        print(f"   📂 CRTMP -> {crtmp_df.shape} columnas: {list(crtmp_df.columns)}")

        fnz003_df = self.data_loader.create_credit_key(self.data_loader.safe_concat(dataframes_nuevos.get("FNZ003", [])))
        print(f"   📂 FNZ003 -> {fnz003_df.shape} columnas: {list(fnz003_df.columns)}")

        sc04_df = self.data_loader.safe_concat(dataframes_nuevos.get("SC04", []))
        print(f"   📂 SC04 -> {sc04_df.shape} columnas: {list(sc04_df.columns)}")

        fnz001_df = self.data_loader.create_credit_key(self.data_loader.safe_concat(dataframes_nuevos.get("FNZ001", [])))
        print(f"   📂 FNZ001 -> {fnz001_df.shape} columnas: {list(fnz001_df.columns)}")

        r03_df = self.data_loader.safe_concat(dataframes_nuevos.get("R03", []))
        print(f"   📂 R03 -> {r03_df.shape} columnas: {list(r03_df.columns)}")

        matriz_cartera_df = self.data_loader.safe_concat(dataframes_nuevos.get("MATRIZ_CARTERA", []))
        print(f"   📂 MATRIZ_CARTERA -> {matriz_cartera_df.shape} columnas: {list(matriz_cartera_df.columns)}")

        metas_franjas_df = self.data_loader.safe_concat(dataframes_nuevos.get("METAS_FRANJAS", []))
        print(f"   📂 METAS_FRANJAS -> {metas_franjas_df.shape} columnas: {list(metas_franjas_df.columns)}")

        asesores_sheets = dataframes_nuevos.get("ASESORES", [])
        print(f"   📂 ASESORES -> {len(asesores_sheets)} hojas cargadas")
        
        # --- Enriquecimientos iniciales ---
        processed_vencimientos, negativos_vencimientos = self.report_service.credit_details.process_vencimientos_data(vencimientos_df)
        if not processed_vencimientos.empty:
            reporte_df = pd.merge(reporte_df, processed_vencimientos, on='Credito', how='left')
        
        if not analisis_df.empty:
            reporte_df = pd.merge(reporte_df, analisis_df.drop_duplicates('Credito'), on='Credito', how='left', suffixes=('', '_Analisis'))
        
        if not r03_df.empty:
            reporte_df = pd.merge(reporte_df, r03_df.drop_duplicates('Cedula_Cliente'), on='Cedula_Cliente', how='left', suffixes=('', '_R03'))

        if not matriz_cartera_df.empty:
            reporte_df['Zona'] = reporte_df['Zona'].astype(str).str.strip()
            matriz_cartera_df['Zona'] = matriz_cartera_df['Zona'].astype(str).str.strip()
            reporte_df = pd.merge(reporte_df, matriz_cartera_df.drop_duplicates('Zona'), on='Zona', how='left')
        
        # --- Vendedores activos ---
        if asesores_sheets:
            codigos_activos = []
            for item in asesores_sheets:
                if 'Codigo_Vendedor' in item["data"].columns:
                    codigos = item["data"]['Codigo_Vendedor'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
                    codigos_activos.extend(codigos.dropna().unique())
            
            codigos_activos = set(codigos_activos)
            print(f"🔍 Total de vendedores activos encontrados: {len(codigos_activos)}")
            
            reporte_df['Codigo_Vendedor_clean'] = (
                reporte_df['Codigo_Vendedor'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
            )
            reporte_df['Vendedor_Activo'] = np.where(
                reporte_df['Codigo_Vendedor_clean'].isin(codigos_activos),
                'ACTIVO', 'INACTIVO'
            )
            reporte_df.drop('Codigo_Vendedor_clean', axis=1, inplace=True)
    
        # --- Transformaciones principales ---
        reporte_df['Empresa'] = np.where(reporte_df['Tipo_Credito'] == 'DF', 'FINANSUEÑOS', 'ARPESOD')
        
        print("👉 Llamando a assign_sales_invoice")
        reporte_df = self.report_service.products_sales.assign_sales_invoice(reporte_df, crtmp_df)
        print(f"✔️ Después de assign_sales_invoice: {reporte_df.shape}")

        print("👉 Llamando a add_product_details")
        print(f"   - crtmp_df columnas: {list(crtmp_df.columns)}")
        reporte_df = self.report_service.products_sales.add_product_details(reporte_df, crtmp_df)
        print(f"✔️ Después de add_product_details: {reporte_df.shape}")

        reporte_df = self.report_service.credit_details.enrich_credit_details(reporte_df, sc04_df, fnz001_df)
        reporte_df = self.report_service.credit_details.clean_installment_data(reporte_df)
        reporte_df = self.report_service.report_processor.map_call_center_data(reporte_df)
        reporte_df, negativos_fnz003 = self.report_service.report_processor.calculate_balances(reporte_df, fnz003_df)
        reporte_df = self.report_service.report_processor.calculate_goal_metrics(reporte_df, metas_franjas_df)
        reporte_df = self.report_service.credit_details.adjust_arrears_status(reporte_df)

        negativos_finales = pd.concat([negativos_vencimientos, negativos_fnz003], ignore_index=True)

        return reporte_df, negativos_finales, pd.DataFrame()

    def _filtrar_dataframes_por_creditos(self, dataframes_por_tipo, creditos_a_procesar):
        dataframes_filtrados = {}
        r91_df_completo = self.data_loader.create_credit_key(
            self.data_loader.safe_concat(dataframes_por_tipo.get("R91", []))
        )
        cedulas_a_procesar = set(r91_df_completo[r91_df_completo['Credito'].isin(creditos_a_procesar)]['Cedula_Cliente'].unique())

        for tipo, lista_items in dataframes_por_tipo.items():
            if not lista_items: 
                continue
            
            df_concatenado = self.data_loader.safe_concat(lista_items)
            df_filtrado = pd.DataFrame()

            if 'Credito' in df_concatenado.columns:
                df_filtrado = df_concatenado[df_concatenado['Credito'].isin(creditos_a_procesar)]
            elif 'Cedula_Cliente' in df_concatenado.columns:
                df_filtrado = df_concatenado[df_concatenado['Cedula_Cliente'].isin(cedulas_a_procesar)]
            else:
                df_filtrado = df_concatenado
            
            if not df_filtrado.empty:
                config = lista_items[0].get("config")
                dataframes_filtrados[tipo] = [{"data": df_filtrado, "config": config}]
                
        return dataframes_filtrados
