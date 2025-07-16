from tkinter import filedialog, messagebox
import threading
from pathlib import Path

# Importaciones de tu proyecto
from src.views.base_view import BaseMensualView
from src.services.base.base_service import ReportService
from src.models.base_model import configuracion, ORDEN_COLUMNAS_FINAL

class BaseMensualController:
    def __init__(self):
        self.view = None
        self.rutas_archivos = {} # Diccionario para almacenar las rutas

    def abrir_vista(self, parent):
        """Crea y muestra la ventana para cargar la base mensual."""
        if self.view is None or not self.view.winfo_exists():
            self.view = BaseMensualView(parent, self)
        self.view.deiconify() # Muestra la ventana si estaba oculta

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
            start_date = self.view.start_date_entry.get() or None
            end_date = self.view.end_date_entry.get() or None
            # --- FIN: LEER FECHAS DEL FILTRO ---

            lista_final_rutas = []
            for lista_rutas in self.rutas_archivos.values():
                lista_final_rutas.extend(lista_rutas)

            if not lista_final_rutas:
                messagebox.showwarning("Sin Archivos", "No se ha seleccionado ningún archivo para procesar.")
                return

            self.view.actualizar_estado("Instanciando servicio...", 10)
            service = ReportService(config=configuracion)

            self.view.actualizar_estado("Generando reporte consolidado...", 30)
            
            # --- MODIFICADO: Pasar las fechas al servicio ---
            reporte_final = service.generate_consolidated_report(
                file_paths=lista_final_rutas,
                orden_columnas=ORDEN_COLUMNAS_FINAL,
                start_date=start_date,
                end_date=end_date
            )

            if reporte_final is None or reporte_final.empty:
                raise Exception("El reporte final está vacío o no se generó. Verifique los archivos de entrada y el rango de fechas.")

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

            reporte_final.to_excel(nombre_archivo_salida, index=False, sheet_name='Reporte Consolidado')
            self.view.actualizar_estado("¡Éxito! Reporte guardado.", 100)
            messagebox.showinfo("Proceso Completado", f"El reporte ha sido guardado exitosamente en:\n{nombre_archivo_salida}")

        except Exception as e:
            messagebox.showerror("Error en el Proceso", f"Ocurrió un error: {str(e)}")
            self.view.actualizar_estado(f"Error: {str(e)}", 0)
        finally:
            self.view.procesar_button.config(state="normal")