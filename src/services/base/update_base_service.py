import pandas as pd
import numpy as np 
from src.models.base_model import ORDEN_COLUMNAS_FINAL, configuracion

class UpdateBaseService:
    """
    Servicio que actualiza un reporte base usando el enfoque híbrido:
    1. Usa el R91 nuevo como la "fuente de la verdad" para el esqueleto del reporte.
    2. Enriquece el esqueleto consolidando los datos nuevos del mes con los del reporte anterior.
    3. Reutiliza las funciones de transformación y cálculo del servicio principal.
    """
    def __init__(self, report_service):
        self.report_service = report_service
        self.data_loader = report_service.data_loader

    def sincronizar_reporte(self, df_base_anterior, dataframes_nuevos):
        print("🚀 Iniciando modo de actualización con enfoque híbrido...")

        # --- PASO 1: Usar R91 como el esqueleto INTOCABLE ---
        df_r91_nuevo = self.data_loader.safe_concat(dataframes_nuevos.get("R91", []))
        if df_r91_nuevo.empty:
            raise ValueError("El archivo R91 es obligatorio para la actualización.")
        
        esqueleto_df = self.data_loader.create_credit_key(df_r91_nuevo)
        print(f"\n[LOG] Esqueleto creado a partir de R91 con {len(esqueleto_df)} registros.")

        # --- CAMBIO CLAVE 1: Definimos la llave compuesta ---
        JOIN_KEYS = ['Credito', 'Cedula_Cliente']

        # --- PASO 2: Consolidar y unir cada fuente de datos ---
        for tipo, config in configuracion.items():
            if tipo == "R91":
                continue
            
            columnas_del_tipo = list(config.get("rename_map", {}).values())
            if not columnas_del_tipo:
                continue

            # Nos aseguramos que las columnas de la llave siempre estén presentes
            for key in JOIN_KEYS:
                if key not in columnas_del_tipo:
                    columnas_del_tipo.append(key)

            df_nuevos_datos = self.data_loader.safe_concat(dataframes_nuevos.get(tipo, []))
            
            columnas_existentes_en_anterior = [col for col in columnas_del_tipo if col in df_base_anterior.columns]
            df_datos_viejos = df_base_anterior[columnas_existentes_en_anterior].copy()
            
            df_consolidado = pd.DataFrame()

            if not df_nuevos_datos.empty:
                if 'Credito' not in df_nuevos_datos.columns:
                     df_nuevos_datos = self.data_loader.create_credit_key(df_nuevos_datos)
                
                # Usamos ignore_index para evitar errores de Reindexing
                df_combinado = pd.concat([df_nuevos_datos, df_datos_viejos], ignore_index=True)
                
                # --- CAMBIO CLAVE 2: Usamos la llave compuesta para eliminar duplicados ---
                df_consolidado = df_combinado.drop_duplicates(subset=JOIN_KEYS, keep='first')
            elif not df_datos_viejos.empty:
                df_consolidado = df_datos_viejos.drop_duplicates(subset=JOIN_KEYS, keep='first')
            
            if df_consolidado.empty:
                continue

            print(f"   - Consolidando y uniendo datos de '{tipo}'...")
            
            # --- CAMBIO CLAVE 3: Usamos la llave compuesta para el merge ---
            esqueleto_df = pd.merge(esqueleto_df, df_consolidado, on=JOIN_KEYS, how='left', suffixes=('', f'_{tipo}_dup'))

        print("\n[LOG] Todas las fuentes de datos han sido unidas al esqueleto.")
        reporte_df = esqueleto_df.copy()

        # --- PASO 3: (Sin cambios) Reutilizar las funciones de transformación ---
        print("\n[LOG] Aplicando transformaciones y cálculos finales...")
        reporte_df, negativos_finales, _ = self._aplicar_transformaciones(reporte_df, dataframes_nuevos)
        reporte_final, reporte_correcciones = self.report_service.report_processor.finalize_report(reporte_df, ORDEN_COLUMNAS_FINAL)

        print(f"\n✅ Proceso de sincronización completado. Registros finales: {len(reporte_final)}")
        return reporte_final, negativos_finales, reporte_correcciones
    
    def _aplicar_transformaciones(self, reporte_df, dataframes_nuevos):
        # Esta función es un contenedor para las llamadas de servicio que ya tienes.
        # Es casi idéntica a la que tenías antes, pero ahora actúa sobre la base consolidada.
        
        # Cargar DataFrames necesarios para las funciones
        crtmp_df = self.data_loader.safe_concat(dataframes_nuevos.get("CRTMPCONSULTA1", []))
        sc04_df = self.data_loader.safe_concat(dataframes_nuevos.get("SC04", []))
        fnz001_df = self.data_loader.create_credit_key(self.data_loader.safe_concat(dataframes_nuevos.get("FNZ001", [])))
        fnz003_df = self.data_loader.create_credit_key(self.data_loader.safe_concat(dataframes_nuevos.get("FNZ003", [])))
        metas_franjas_df = self.data_loader.safe_concat(dataframes_nuevos.get("METAS_FRANJAS", []))
        vencimientos_df = self.data_loader.create_credit_key(self.data_loader.safe_concat(dataframes_nuevos.get("VENCIMIENTOS", [])))
        
        _, negativos_vencimientos = self.report_service.credit_details.process_vencimientos_data(vencimientos_df)
        
        # Llamadas a los servicios que reutilizamos
        reporte_df['Empresa'] = np.where(reporte_df['Tipo_Credito'] == 'DF', 'FINANSUEÑOS', 'ARPESOD')
        reporte_df = self.report_service.products_sales.assign_sales_invoice(reporte_df, crtmp_df)
        reporte_df = self.report_service.products_sales.add_product_details(reporte_df, crtmp_df)
        reporte_df = self.report_service.credit_details.enrich_credit_details(reporte_df, sc04_df, fnz001_df)
        reporte_df = self.report_service.credit_details.clean_installment_data(reporte_df)
        reporte_df = self.report_service.report_processor.map_call_center_data(reporte_df)
        reporte_df, negativos_fnz003 = self.report_service.report_processor.calculate_balances(reporte_df, fnz003_df)
        reporte_df = self.report_service.report_processor.calculate_goal_metrics(reporte_df, metas_franjas_df)
        reporte_df = self.report_service.credit_details.adjust_arrears_status(reporte_df)

        negativos_finales = pd.concat([negativos_vencimientos, negativos_fnz003], ignore_index=True)
        return reporte_df, negativos_finales, pd.DataFrame()