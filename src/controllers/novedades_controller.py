from tkinter import filedialog, messagebox
import pandas as pd
from pathlib import Path
from src.services.novedades.novedades_service import NovedadesService
from src.services.novedades.analisis_service import AnalisisService
from src.services.novedades.recaudo_service import RecaudoR91Service 
from src.services.novedades.franjas_service import ReporteFranjasService
from src.models.novedad_model import configuracion
from openpyxl import load_workbook
from openpyxl.styles import Alignment, Font
from openpyxl.utils.dataframe import dataframe_to_rows


class NovedadesAnalisisController:
    def __init__(self):
        self.view = None
        # La ruta al caché se define aquí para que el controlador la conozca
        self.cache_path = Path(__file__).resolve().parent.parent.parent / "cache" / "reporte_base_mensual.feather"

    def set_view(self, view):
        """Asigna la vista a este controlador."""
        self.view = view

     # --- MÉTODO NUEVO Y RECOMENDADO para cargar el reporte base ---
    def _cargar_reporte_base(self, ruta_base, config):
        """
        Carga el archivo de reporte base usando la configuración definida en el modelo.
        Aplica el mapa de tipos y convierte las columnas de fecha.
        """
        print("🔄 Cargando reporte base con configuración del modelo...")
        
        # 1. Obtiene la configuración específica para "BASE_MENSUAL" de forma segura
        config_base = config.get("BASE_MENSUAL", {})
        mapa_de_tipos = config_base.get("dtype_map", None)

        if not mapa_de_tipos:
            messagebox.showwarning("Configuración Faltante", "No se encontró el 'dtype_map' para 'BASE_MENSUAL' en la configuración.")
            # Como fallback, lo leemos como texto para evitar errores
            return pd.read_excel(ruta_base, dtype=mapa_de_tipos,parse_dates=False)

        # 2. Lee el Excel usando el mapa de tipos que definiste
        df_base = pd.read_excel(ruta_base, dtype=mapa_de_tipos)
        
         # --- LÍNEAS NUEVAS PARA LIMPIAR LA COLUMNA PROBLEMÁTICA ---
        if 'Valor_Cuota_Vigente' in df_base.columns:
            # Convierte a número, los errores (texto) se volverán NaN
            df_base['Valor_Cuota_Vigente'] = pd.to_numeric(df_base['Valor_Cuota_Vigente'], errors='coerce')
            # Rellena los NaN resultantes con 0 para tener una columna numérica limpia
            df_base['Valor_Cuota_Vigente'] = df_base['Valor_Cuota_Vigente'].fillna(0)

        # --- NUEVO BLOQUE DE LIMPIEZA PARA 'Primera_Cuota_Mora' ---
        if 'Primera_Cuota_Mora' in df_base.columns:
            df_base['Primera_Cuota_Mora'] = pd.to_numeric(df_base['Primera_Cuota_Mora'], errors='coerce').fillna(0)
            # Como es un entero, podemos convertir el tipo al final para que no tenga decimales
            df_base['Primera_Cuota_Mora'] = df_base['Primera_Cuota_Mora'].astype('Int64')    

        # 3. Convierte TODAS las columnas de fecha de una vez
        columnas_fecha = [
            'Fecha_Desembolso', 'Fecha_Facturada', 'Fecha_Cuota_Vigente', 'Fecha_Cuota_Atraso'
        ]
        for col in columnas_fecha:
            if col in df_base.columns:
                df_base[col] = pd.to_datetime(df_base[col], errors='coerce')
        
        print("✅ Reporte base cargado exitosamente usando el mapa de tipos.")
        return df_base   
    
    
    def _cargar_y_unir_archivos(self, file_paths, config_key):
        """
        Función interna para leer y concatenar múltiples archivos,
        seleccionando el motor de Excel correcto.
        """
        if not file_paths:
            return pd.DataFrame()
        
        df_list = []
        file_config = configuracion[config_key]
        for path in file_paths:
            # --- LÓGICA CORREGIDA PARA SELECCIONAR EL MOTOR ---
            engine_to_use = None
            # Convertimos la ruta a minúsculas para una comparación segura
            file_ext = path.lower() 

            if file_ext.endswith('.xlsx'):
                engine_to_use = 'openpyxl'
            elif file_ext.endswith('.xls'):
                engine_to_use = 'xlrd'
            
            # Le pasamos el motor correcto a pandas al leer el archivo
            df = pd.read_excel(
                path, 
                engine=engine_to_use, # <-- Se añade el motor
                usecols=file_config["usecols"]
            ).rename(columns=file_config["rename_map"])
            df_list.append(df)
        
        return pd.concat(df_list, ignore_index=True)


    def _save_multiindex_without_index(self, writer, df, sheet_name):
        """
        Guarda un DataFrame con MultiIndex columns sin la columna de índice numérico
        """
        # Crear nueva hoja
        workbook = writer.book
        if sheet_name in workbook.sheetnames:
            # Eliminar la hoja si ya existe
            std = workbook[sheet_name]
            workbook.remove(std)
        worksheet = workbook.create_sheet(sheet_name)
        
        # Escribir encabezados MultiIndex
        for col_idx, col_name in enumerate(df.columns, 1):
            if isinstance(col_name, tuple):
                # Escribir primer nivel
                worksheet.cell(row=1, column=col_idx, value=col_name[0])
                # Escribir segundo nivel
                worksheet.cell(row=2, column=col_idx, value=col_name[1])
            else:
                # Para columnas simples, mergear verticalmente
                worksheet.cell(row=1, column=col_idx, value=col_name)
                worksheet.merge_cells(start_row=1, start_column=col_idx, end_row=2, end_column=col_idx)
        
        # Escribir datos (empezando desde fila 3)
        for row_idx, row_data in enumerate(df.values, 3):
            for col_idx, value in enumerate(row_data, 1):
                worksheet.cell(row=row_idx, column=col_idx, value=value)

    def procesar_archivos(self, rutas_novedades, ruta_base, rutas_analisis, rutas_r91, ruta_usuarios):
        """
        Orquesta todo el proceso: carga el caché, aplica novedades, calcula el rodamiento
        y guarda un reporte multi-hoja.
        """
        if not rutas_novedades or not rutas_analisis or not rutas_r91:
            messagebox.showwarning("Archivos Faltantes", "Debes seleccionar los archivos de Novedades y Análisis.")
            return
            
        try:
            # --- CAMBIO: Cargar la base desde el archivo Excel seleccionado ---
            print(f"🔄 Cargando reporte base desde: {ruta_base}")
            # Usamos dtype=str para evitar problemas de formato de Excel con las cédulas
            df_base = self._cargar_reporte_base(ruta_base, configuracion)
            
            # Si la carga falla por alguna razón, el método nuevo podría retornar None
            if df_base is None or df_base.empty:
                messagebox.showerror("Error", "No se pudo cargar el reporte base correctamente.")
                return

            # 1. Cargar y unir todos los archivos de entrada
            df_novedades_unido = self._cargar_y_unir_archivos(rutas_novedades, "NOVEDADES")
            df_analisis_unido = self._cargar_y_unir_archivos(rutas_analisis, "ANALISIS")
            df_r91_unido = self._cargar_y_unir_archivos(rutas_r91, "R91")
            df_usuarios_unido = self._cargar_y_unir_archivos(ruta_usuarios, "USUARIOS")
            
            df_novedades_unido['Usuario_Novedad'] = df_novedades_unido['Usuario_Novedad'].astype(str).str.lower()
            df_usuarios_unido['Usuario_Novedad'] = df_usuarios_unido['Usuario_Novedad'].astype(str).str.lower()
            
            # 2. Aplicar Novedades
            novedades_service = NovedadesService(configuracion)
            df_base_enriquecido, df_novedades_detallado = novedades_service.aplicar_novedades(df_base, df_novedades_unido)
            
            df_novedades_detallado = pd.merge(
                df_novedades_detallado, 
                df_usuarios_unido, 
                on='Usuario_Novedad', 
                how='left'
            )
            
            columnas_a_mayusculas = ['Usuario_Novedad', 'Nombre_Usuario', 'Cargo_Usuario']
            for col in columnas_a_mayusculas:
                # Verificamos si la columna existe en el DataFrame para evitar errores
                if col in df_novedades_detallado.columns:
                    # Usamos .astype(str) por si hay algún valor no textual (como NaN) y luego .str.upper()
                    df_novedades_detallado[col] = df_novedades_detallado[col].astype(str).str.upper()
                
            # 3. Calcular Rodamiento
            analisis_service = AnalisisService(configuracion)
            df_con_rodamiento = analisis_service.calcular_rodamiento(df_base_enriquecido, df_analisis_unido)

            # 4. Calcular Recaudos
            recaudo_service = RecaudoR91Service()
            df_recaudos = recaudo_service.procesar_recaudos(df_r91_unido)

            # 5. Unir la información de recaudos al reporte final
            df_final = pd.merge(df_con_rodamiento, df_recaudos, on='Credito', how='left')
            for col in ['Recaudo_Anticipado', 'Recaudo_Meta', 'Total_Recaudo']:
                if col in df_final.columns:
                    df_final[col] = pd.to_numeric(df_final[col], errors='coerce').fillna(0)
            
            columnas_fecha_sin_hora = [
                'Fecha_Desembolso', 'Fecha_Facturada',
                'Fecha_Cuota_Vigente', 'Fecha_Cuota_Atraso'
            ]
            for col in columnas_fecha_sin_hora:
                if col in df_final.columns:
                    df_final[col] = pd.to_datetime(df_final[col], errors='coerce').dt.date
            
            orden_columnas_analisis = [
                'Empresa', 'Credito', 'Fecha_Desembolso', 'Factura_Venta', 'Fecha_Facturada',
                'Nombre_Producto', 'Cantidad_Producto', 'Obsequio', 'Cantidad_Obsequio',
                'Cantidad_Total_Producto', 'Cedula_Cliente', 'Nombre_Cliente', 'Correo',
                'Celular', 'Direccion', 'Barrio', 'Nombre_Ciudad', 'Zona', 'Cobrador',
                'Telefono_Cobrador', 'Zona_Cobro', 'Call_Center_Apoyo', 'Nombre_Call_Center',
                'Telefono_Call_Center', 'Regional_Cobro', 'Gestor', 'Telefono_Gestor',
                'Jefe_ventas', 'Codigo_Vendedor', 'Nombre_Vendedor', 'Movil_Vendedor',
                'Vendedor_Activo', 'Lider_Zona', 'Movil_Lider', 'Codigo_Centro_Costos',
                'Regional_Venta', 'Codeudor1', 'Nombre_Codeudor1', 'Telefono_Codeudor1',
                'Ciudad_Codeudor1', 'Codeudor2', 'Nombre_Codeudor2', 'Telefono_Codeudor2',
                'Ciudad_Codeudor2', 'Valor_Desembolso', 'Total_Cuotas', 'Valor_Cuota',
                'Dias_Atraso', 'Franja_Mora', 'Saldo_Capital', 'Saldo_Interes_Corriente',
                'Saldo_Avales', 'Meta_Intereses', 'Meta_General', 'Meta_%', 'Meta_$',
                'Meta_T.R_%', 'Meta_T.R_$', 'Cuotas_Pagadas', 'Cuota_Vigente',
                'Fecha_Cuota_Vigente', 'Valor_Cuota_Vigente', 'Fecha_Cuota_Atraso',
                'Primera_Cuota_Mora', 'Valor_Cuota_Atraso', 'Valor_Vencido',
                'Fecha_Ultima_Novedad', 'Cantidad_Novedades', 'Dias_Atraso_Final',
                'Franja_Mora_Final', 'Rodamiento', 'Recaudo_Anticipado', 'Recaudo_Meta',
                'Total_Recaudo'
            ]

            orden_columnas_detalle = [
                'Cedula_Cliente', 'Nombre_Cliente', 'Fecha_Novedad', 'Usuario_Novedad',
                'Nombre_Usuario', 'Cargo_Usuario', 'Celular_Corporativo', 'Tipo_Novedad',
                'Novedad', 'Fecha_Compromiso', 'Valor'
            ]

            df_final = df_final[[col for col in orden_columnas_analisis if col in df_final.columns]]
            df_novedades_detallado = df_novedades_detallado[[col for col in orden_columnas_detalle if col in df_novedades_detallado.columns]]
            
            franjas_service = ReporteFranjasService()
            df_reporte_franjas = franjas_service.generar_reporte(df_final)

            # 6. Guardar el reporte multi-hoja
            ruta_salida = filedialog.asksaveasfilename(
                defaultextension=".xlsx", 
                initialfile="Reporte_Novedades_y_Analisis.xlsx"
            )
            if not ruta_salida: return

            print("💾 Guardando datos en el archivo Excel...")
            with pd.ExcelWriter(ruta_salida, engine='openpyxl') as writer:
                # Guardar hojas normales con index=False
                df_final.to_excel(writer, sheet_name='Analisis_de_Cartera', index=False)
                df_novedades_detallado.to_excel(writer, sheet_name='Detalle_Novedades', index=False)
                
                # Guardar hoja con MultiIndex usando nuestro método personalizado
                self._save_multiindex_without_index(writer, df_reporte_franjas, 'Reporte_Franjas')
            
            print("✅ Datos guardados.")

            # Aplicar formato a la hoja Reporte_Franjas
            try:
                print("🎨 Aplicando formato de celda final...")
                
                workbook = load_workbook(ruta_salida)
                worksheet = workbook['Reporte_Franjas']
                
                alineacion_centrada = Alignment(horizontal='center', vertical='center')
                fuente_negrita = Font(bold=True)

                # a. Aplicar estilo a TODOS los encabezados (filas 1 y 2)
                for row in worksheet.iter_rows(min_row=1, max_row=2, max_col=worksheet.max_column):
                    for cell in row:
                        cell.alignment = alineacion_centrada
                        cell.font = fuente_negrita

                # b. Realizar merges HORIZONTALES leyendo la fila 1
                start_col_merge = -1
                for col_idx in range(1, worksheet.max_column + 2):
                    cell_value = worksheet.cell(row=1, column=col_idx).value
                    if cell_value is not None:
                        if start_col_merge != -1 and col_idx - start_col_merge > 1:
                            worksheet.merge_cells(start_row=1, start_column=start_col_merge, end_row=1, end_column=col_idx - 1)
                        start_col_merge = col_idx
                
                # c. Realizar merges VERTICALES leyendo la fila 2
                for col_idx in range(1, worksheet.max_column + 1):
                    if worksheet.cell(row=2, column=col_idx).value is None:
                        worksheet.merge_cells(start_row=1, start_column=col_idx, end_row=2, end_column=col_idx)

                workbook.save(ruta_salida)
                
                print("✅ Formato de celda aplicado exitosamente.")
                messagebox.showinfo("Éxito", f"Reporte unificado guardado exitosamente en:\n{ruta_salida}")

            except Exception as e:
                messagebox.showerror("Error de Formato", f"El reporte se guardó, pero ocurrió un error al aplicar el formato:\n{e}")

        except Exception as e:
            messagebox.showerror("Error en el Proceso", f"Ocurrió un error general:\n{e}")