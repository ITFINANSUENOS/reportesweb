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
        self.rutas_archivos = {} 
        self.ruta_reporte_base = None

    def set_view(self, view):
        self.view = view
        
    def abrir_vista(self, parent):
        """Crea y muestra la ventana para cargar la base mensual."""
        if self.view is None or not self.view.winfo_exists():
            main_controller = getattr(self.view, 'main_window_controller', None)
            self.view = BaseMensualView(parent, self, main_controller)
        self.view.deiconify() 

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
            
            print(f"Archivos para {tipo_archivo}: {self.rutas_archivos[tipo_archivo]}")

    def seleccionar_reporte_base(self):
        """Abre un diálogo para seleccionar el archivo Excel del reporte anterior."""
        filetypes = [("Excel files", "*.xlsx *.xls")]
        ruta = filedialog.askopenfilename(title="Seleccione el Reporte de Excel Anterior", filetypes=filetypes)
        
        if ruta:
            self.ruta_reporte_base = ruta
            if self.view:
                # Usamos el estilo 'Success.TLabel' para consistencia visual
                self.view.base_report_path_label.config(text=Path(ruta).name, style='Success.TLabel')
            print(f"Reporte base para actualización seleccionado: {self.ruta_reporte_base}")
            
    def procesar_archivos(self):
        """Inicia el procesamiento de los archivos en un hilo separado para no congelar la UI."""
        self.view.procesar_button.config(state="disabled")
        self.view.actualizar_estado("Iniciando proceso...", 0)
        
        thread = threading.Thread(target=self._ejecutar_proceso)
        thread.start()

    def _ejecutar_proceso(self):
        """Lógica de procesamiento que se ejecuta en segundo plano."""
        try:
            modo_actualizacion = self.view.update_mode_var.get()
            start_date = self.view.start_date_entry.get() or None
            end_date = self.view.end_date_entry.get() or None
            
            lista_final_rutas = []
            for lista_rutas in self.rutas_archivos.values():
                lista_final_rutas.extend(lista_rutas)

            if not lista_final_rutas:
                messagebox.showwarning("Sin Archivos", "No se ha seleccionado ningún archivo para procesar.")
                return

            reporte_negativos = pd.DataFrame()
            reporte_correcciones = pd.DataFrame()
            
            service_principal = ReportService(config=configuracion)
            
            if modo_actualizacion:
                # --- MODO ACTUALIZACIÓN RÁPIDA (DESDE EXCEL) ---
                self.view.actualizar_estado("Iniciando sincronización rápida...", 10)
                
                if not self.ruta_reporte_base:
                    messagebox.showerror("Error", "Para el modo actualización, primero debe seleccionar el reporte de Excel anterior.")
                    return
                
                print('Cargando base anterior desde excel...')
                self.view.actualizar_estado(f"Cargando base anterior desde Excel...", 20)
                try:
                    df_base_anterior = pd.read_excel(self.ruta_reporte_base, dtype=str)
                except Exception as e:
                    messagebox.showerror("Error al leer Excel", f"No se pudo leer el archivo Excel base: {e}")
                    return
            
                # 1. USAMOS DATALOADER para cargar y estandarizar los nuevos archivos.
                self.view.actualizar_estado("Estandarizando archivos nuevos...", 30)
                dataframes_nuevos_estandarizados = service_principal.data_loader.load_dataframes(lista_final_rutas)
                
                # 2. Creamos la instancia del UpdateService.
                update_service = UpdateBaseService(report_service=service_principal,)
                
                self.view.actualizar_estado("Sincronizando cambios...", 60)
                reporte_final, reporte_negativos, reporte_correcciones = update_service.sincronizar_reporte(
                    df_base_anterior, 
                    dataframes_nuevos_estandarizados 
                )

            else:
                self.view.actualizar_estado("Iniciando construcción completa...", 10)
                reporte_final, reporte_negativos, reporte_correcciones = service_principal.generate_consolidated_report(
                    file_paths=lista_final_rutas,
                    orden_columnas=ORDEN_COLUMNAS_FINAL,
                    start_date=start_date,
                    end_date=end_date
                )

            if reporte_final is None or reporte_final.empty:
                raise Exception("El reporte final está vacío o no se generó. Verifique los archivos de entrada.")

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

            print(f"💾 Guardando reporte en {nombre_archivo_salida}...")
            with pd.ExcelWriter(nombre_archivo_salida, engine='openpyxl') as writer:
                reporte_final.to_excel(writer, sheet_name='Reporte Consolidado', index=False)
                
                if reporte_negativos is not None and not reporte_negativos.empty:
                    reporte_negativos.to_excel(writer, sheet_name='Creditos_Negativos', index=False)
                    print("   - Hoja 'Creditos_Negativos' añadida.")

                if reporte_correcciones is not None and not reporte_correcciones.empty:
                    print("   - 🎨 Aplicando estilos a la hoja de correcciones...")
                    def aplicar_estilos(val):
                        if val == 'CORREGIR': return 'background-color: #FFCDD2'
                        if val == 'BIEN': return 'background-color: #C8E6C9'
                        return 'background-color: #FFFFFF'
                    
                    styled_df = reporte_correcciones.style.applymap(aplicar_estilos)
                    styled_df.to_excel(writer, sheet_name='Registros_Para_Corregir', index=False)
                    print("   - ✅ Hoja 'Registros_Para_Corregir' añadida con colores.")
            
            self.view.actualizar_estado("¡Éxito! Reporte guardado.", 100)
            messagebox.showinfo("Proceso Completado", f"El reporte ha sido guardado exitosamente en:\n{nombre_archivo_salida}") 

        except Exception as e:
            messagebox.showerror("Error en el Proceso", f"Ocurrió un error: {str(e)}")
            self.view.actualizar_estado(f"Error: {str(e)}", 0)
        finally:
            self.view.procesar_button.config(state="normal")