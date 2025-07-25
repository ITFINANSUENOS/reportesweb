import threading
from tkinter import filedialog, messagebox
from src.views.datacredito_view import DataCreditoView
from src.models.datacredito_model import DataCreditoModel

class DataCreditoController:
    def __init__(self):
        self.datacredito_view = None
        self.model = DataCreditoModel()

    def abrir_vista_datacredito(self, parent):
        """Crea y muestra la ventana de Datacredito."""
        if self.datacredito_view is None or not self.datacredito_view.top.winfo_exists():
            self.datacredito_view = DataCreditoView(parent, self)
            self.datacredito_view.top.grab_set()
        else:
            self.datacredito_view.top.lift()

    def run_processing_datacredito(self, view, plano_path, correcciones_path):
            """Pide el archivo de salida e inicia el procesamiento en un hilo."""
            output_path = filedialog.asksaveasfilename(
                title="Guardar archivo procesado como...",
                defaultextension=".xlsx",
                filetypes=[("Archivos de Excel", "*.xlsx")]
            )
            if not output_path:
                view.update_status("Proceso cancelado por el usuario.")
                return

            thread = threading.Thread(
                target=self._run_processing_thread,
                # 3. Le pasamos la 'view' correcta al hilo
                args=(view, plano_path, correcciones_path, output_path)
            )
            thread.start()

    def _run_processing_thread(self, view, plano_path, correcciones_path, output_path):
        """La función que se ejecuta en el hilo. Contiene toda la lógica."""
        try:
            # Usa el parámetro 'view' que recibe la función, NO 'self.view'
            view.update_status("Iniciando proceso Datacredito...")
            
            # 1. Cargar datos (Modelo)
            self.model.load_plano_file(plano_path)
            view.update_status("Archivo plano cargado, transformando...")
            
            # 2. Procesar datos (Modelo llama al Servicio)
            self.model.process_data(correcciones_path)
            view.update_status("Datos procesados, guardando archivo...")
            
            # 3. Guardar datos (Modelo)
            self.model.save_processed_file(output_path)
            view.update_status(f"¡Éxito! Archivo guardado.")
            messagebox.showinfo("Proceso Completado", f"El archivo de Datacredito se ha guardado en:\n{output_path}")

        except Exception as e:
            error_message = f"Error en el proceso: {e}"
            print(error_message)
            # Usa el parámetro 'view' para mostrar el error
            view.update_status(error_message)
            messagebox.showerror("Error", error_message)
        finally:
            # Usa 'view.top.after' para limpiar el estado
            view.top.after(5000, lambda: view.update_status("Listo para comenzar."))