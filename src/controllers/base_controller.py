from tkinter import filedialog, messagebox
import threading
import pandas as pd
from pathlib import Path

# Importaciones de tu proyecto
from src.views.base_view.base_view import BaseMensualView
from src.services.base.report_service import ReportService
from src.models.base_model import configuracion, ORDEN_COLUMNAS_FINAL
from src.services.base.update_base_service import UpdateBaseService

class BaseMensualController:
    def __init__(self,view=None):
        self.view = view
        self.rutas_archivos = {} # Diccionario para almacenar las rutas
        self.cache_path = Path(__file__).resolve().parent.parent.parent / "cache" / "reporte_base_mensual.feather"

    def abrir_vista(self, parent):
        """Crea y muestra la ventana para cargar la base mensual."""
        if self.view is None or not self.view.winfo_exists():
            self.view = BaseMensualView(parent, self)
        self.view.deiconify() # Muestra la ventana si estaba oculta

    def set_view(self, view):
        self.view = view

    def seleccionar_archivo(self, tipo_archivo):
        """Abre un diálogo para seleccionar uno o varios archivos."""
        filetypes = [("Excel files", "*.xlsx *.XLSX *.xls *.XLS")]
        
        if tipo_archivo in ["ANALISIS", "R91", "VENCIMIENTOS", "R03"]:
            rutas = filedialog.askopenfilenames(title=f"Seleccione archivos para {tipo_archivo}", filetypes=filetypes)
        else:
            ruta_unica = filedialog.askopenfilename(title=f"Seleccione archivo para {tipo_archivo}", filetypes=filetypes)
            rutas = [ruta_unica] if ruta_unica else []

        if rutas:
            self.rutas_archivos[tipo_archivo] = list(rutas)
            display_text = Path(rutas[0]).name
            if len(rutas) > 1:
                display_text = f"{len(rutas)} archivos seleccionados"

            if self.view:
               self.view.actualizar_ruta_label(tipo_archivo, display_text)
            else:
               print("Error: La vista no ha sido asignada al controlador.")    
            
            self.view.actualizar_ruta_label(tipo_archivo, display_text)
            print(f"Archivos para {tipo_archivo}: {self.rutas_archivos[tipo_archivo]}")

    def procesar_archivos(self):
        """Inicia el procesamiento de los archivos en un hilo separado para no congelar la UI."""
        self.view.procesar_button.config(state="disabled")
        self.view.actualizar_estado("Iniciando proceso...", 0)
        
        thread = threading.Thread(target=self._ejecutar_proceso)
        thread.start()

    def _ejecutar_proceso(self):
        """Lógica de procesamiento que se ejecuta en segundo plano."""
        try:
            # --- INICIO: LEER FECHAS DEL FILTRO ---
            modo_actualizacion = self.view.update_mode_var.get()
            start_date = self.view.start_date_entry.get() or None
            end_date = self.view.end_date_entry.get() or None
            # --- FIN: LEER FECHAS DEL FILTRO ---
            

            lista_final_rutas = []
            for lista_rutas in self.rutas_archivos.values():
                lista_final_rutas.extend(lista_rutas)

            if not lista_final_rutas:
                messagebox.showwarning("Sin Archivos", "No se ha seleccionado ningún archivo para procesar.")
                return

            
            reporte_negativos = pd.DataFrame() # Inicializar por si acaso
            reporte_correcciones = pd.DataFrame() # Inicializar por si acaso
            service_principal = ReportService(config=configuracion) # Se instancia siempre

            if modo_actualizacion:
                # --- MODO ACTUALIZACIÓN RÁPIDA ---
                self.view.actualizar_estado("Iniciando sincronización rápida...", 10)
                if not self.cache_path.exists():
                    messagebox.showerror("Error", "No se encontró el archivo de caché. Ejecute primero una construcción completa.")
                    return

                # Cargar la base anterior desde el caché
                self.view.actualizar_estado("Cargando base anterior desde caché...", 20)
                df_base_anterior = pd.read_feather(self.cache_path)
                
                # Cargar solo los dataframes nuevos que el usuario seleccionó
                self.view.actualizar_estado("Cargando archivos de actualización...", 40)
                
                # <<< --- CORRECCIÓN AQUÍ --- >>>
                # Se debe llamar desde el 'service_principal' que ya tiene el data_loader
                dataframes_nuevos = service_principal.data_loader.load_dataframes(lista_final_rutas)
                
                # Instanciar y usar el nuevo servicio de actualización
                update_service = UpdateBaseService(report_service=service_principal)
                
                self.view.actualizar_estado("Sincronizando cambios...", 60)
                reporte_final, reporte_correcciones = update_service.sincronizar_reporte(df_base_anterior, dataframes_nuevos)
                
                # NOTA: La generación de 'reporte_negativos' se podría integrar en la sincronización
                # para mayor eficiencia, pero por ahora lo mantenemos simple. Puedes generarlo
                # a partir del 'reporte_final' si es necesario.

            else:
                # --- MODO CONSTRUCCIÓN COMPLETA (como antes) ---
                self.view.actualizar_estado("Iniciando construcción completa...", 10)
                reporte_final, reporte_negativos, reporte_correcciones = service_principal.generate_consolidated_report(
                    file_paths=lista_final_rutas,
                    orden_columnas=ORDEN_COLUMNAS_FINAL,
                    start_date=start_date,
                    end_date=end_date
                )

            if reporte_final is None or reporte_final.empty:
                raise Exception("El reporte final está vacío o no se generó. Verifique los archivos.")

            self.view.actualizar_estado("Esperando para guardar el archivo...", 90)

            nombre_archivo_salida = filedialog.asksaveasfilename(
                title="Guardar reporte como...",
                defaultextension=".xlsx",
                filetypes=[("Archivos de Excel", "*.xlsx"), ("Todos los archivos", "*.*")],
                initialfile="Reporte_Consolidado_Final.xlsx"
            )

            if not nombre_archivo_salida:
                self.view.actualizar_estado("Guardado cancelado por el usuario.", 0)
                messagebox.showinfo("Cancelado", "La operación de guardado fue cancelada.")
                return

            # --- Lógica de guardado para múltiples hojas ---
            print(f"💾 Guardando reporte en {nombre_archivo_salida}...")
            with pd.ExcelWriter(nombre_archivo_salida, engine='openpyxl') as writer:
                # Hoja 1: El reporte principal
                reporte_final.to_excel(writer, sheet_name='Reporte Consolidado', index=False)
                
                # Hoja 2: El reporte de negativos (solo si tiene datos)
                if reporte_negativos is not None and not reporte_negativos.empty:
                    reporte_negativos.to_excel(writer, sheet_name='Creditos_Negativos', index=False)
                    print("   - Hoja 'Creditos_Negativos' añadida.")

                # --- Guardar la hoja de correcciones CON COLORES ---
                if reporte_correcciones is not None and not reporte_correcciones.empty:
                    print("   - 🎨 Aplicando estilos a la hoja de correcciones...")
                    
                    def aplicar_estilos(val):
                        """Función que define el color de fondo de cada celda."""
                        if val == 'CORREGIR':
                            color = '#FFCDD2' # Rojo claro
                        elif val == 'BIEN':
                            color = '#C8E6C9' # Verde claro
                        else:
                            color = '#FFFFFF' # Blanco
                        return f'background-color: {color}'
                    
                    styled_df = reporte_correcciones.style.applymap(aplicar_estilos)
                    styled_df.to_excel(writer, sheet_name='Registros_Para_Corregir', index=False)
                    print("   - ✅ Hoja 'Registros_Para_Corregir' añadida con colores.")

            try:
                self.cache_path.parent.mkdir(exist_ok=True)
                df_cache = reporte_final.copy()
                for col in df_cache.columns:
                    if df_cache[col].dtype == 'object':
                        df_cache[col] = df_cache[col].astype(str)
                
                print(f"⚡ Guardando caché actualizado en: {self.cache_path}")
                df_cache.reset_index(drop=True).to_feather(self.cache_path)
                
            except Exception as e:
                print(f"⚠️ No se pudo guardar el caché de datos: {e}")
                messagebox.showwarning("Advertencia de Caché", "No se pudo guardar el archivo de caché.")

            self.view.actualizar_estado("¡Éxito! Reporte guardado.", 100)
            messagebox.showinfo("Proceso Completado", "El reporte ha sido guardado exitosamente.") 

        except Exception as e:
            messagebox.showerror("Error en el Proceso", f"Ocurrió un error: {str(e)}")
            self.view.actualizar_estado(f"Error: {str(e)}", 0)
        finally:
            self.view.procesar_button.config(state="normal")