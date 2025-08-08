from tkinter import filedialog, messagebox
import pandas as pd
from pathlib import Path
from src.services.novedades.novedades_service import NovedadesService
from src.services.novedades.analisis_service import AnalisisService
from src.models.novedad_model import configuracion

class NovedadesAnalisisController:
    def __init__(self):
        self.view = None
        # La ruta al caché se define aquí para que el controlador la conozca
        self.cache_path = Path(__file__).resolve().parent.parent.parent / "cache" / "reporte_base_mensual.feather"

    def set_view(self, view):
        """Asigna la vista a este controlador."""
        self.view = view

    def _cargar_base_desde_cache(self):
        """Método interno para cargar el reporte base desde el archivo Feather."""
        if not self.cache_path.exists():
            raise FileNotFoundError("No se encontró el archivo de caché (reporte_base_mensual.feather).\n\nPor favor, genera primero el 'Reporte Base' desde el módulo de 'Base Mensual'.")
        print(f"🔄 Cargando reporte base desde el caché: {self.cache_path}")
        return pd.read_feather(self.cache_path)
    
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

    def procesar_archivos(self, rutas_novedades, rutas_analisis):
        """
        Orquesta todo el proceso: carga el caché, aplica novedades y calcula el rodamiento.
        """
        if not rutas_novedades or not rutas_analisis:
            messagebox.showwarning("Archivos Faltantes", "Debes seleccionar ambos archivos para continuar.")
            return
            
        try:
            # 1. Cargar la base desde el caché
            df_base = self._cargar_base_desde_cache()

            # 2. Instanciar y usar el servicio de novedades
            df_novedades_unido = self._cargar_y_unir_archivos(rutas_novedades, "NOVEDADES")
            df_analisis_unido = self._cargar_y_unir_archivos(rutas_analisis, "ANALISIS")

            # 2. Aplicar Novedades (el servicio ahora recibe un DataFrame)
            novedades_service = NovedadesService(config=configuracion)
            df_con_novedades = novedades_service.aplicar_novedades(df_base, df_novedades_unido)
            
            # 3. Calcular Rodamiento (el servicio ahora recibe un DataFrame)
            analisis_service = AnalisisService(config=configuracion)
            df_final = analisis_service.calcular_rodamiento(df_con_novedades, df_analisis_unido)

            # 4. Pedir al usuario dónde guardar el nuevo reporte
            ruta_salida = filedialog.asksaveasfilename(
                defaultextension=".xlsx",
                filetypes=[("Archivos de Excel", "*.xlsx")],
                title="Guardar Reporte de Novedades y Análisis Como...",
                initialfile="Reporte_Novedades_y_Analisis.xlsx"
            )
            
            if not ruta_salida:
                messagebox.showinfo("Cancelado", "Proceso cancelado por el usuario.")
                return

            # 5. Guardar el resultado
            df_final.to_excel(ruta_salida, index=False)
            messagebox.showinfo("Éxito", f"Reporte unificado guardado exitosamente en:\n{ruta_salida}")

        except Exception as e:
            messagebox.showerror("Error en el Proceso", f"Ocurrió un error:\n{e}")
