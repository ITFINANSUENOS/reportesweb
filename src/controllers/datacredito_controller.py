import threading
from tkinter import filedialog, messagebox
from src.views.datacredito_view import DataCreditoView
from src.models.datacredito_model import DataCreditoModel

class DataCreditoController:
    def __init__(self, view):
        self.view = view
        self.model = DataCreditoModel()

    def abrir_vista_datacredito(self, parent):
        """Crea y muestra la ventana para cargar archivos."""
        # Se pasa a sí mismo (self) a la vista para que la vista pueda llamarlo
        DataCreditoView(parent, self)

    def procesar_archivos(self, plano_path, correcciones_path):
        """Inicia el procesamiento en un hilo separado para no bloquear la GUI."""
        
        # Pide al usuario dónde guardar el archivo final ANTES de empezar
        output_path = filedialog.asksaveasfilename(
            title="Guardar archivo procesado como...",
            defaultextension=".xlsx",
            filetypes=[("Archivos de Excel", "*.xlsx")]
        )
        if not output_path:
            self.view.update_status("Proceso cancelado por el usuario.")
            return

        # Inicia el proceso en un hilo secundario
        thread = threading.Thread(
            target=self._run_processing_thread,
            args=(plano_path, correcciones_path, output_path)
        )
        thread.start()

    def _run_processing_thread(self, plano_path, correcciones_path, output_path):
        """La función que se ejecuta en el hilo. Contiene toda la lógica."""
        try:
            self.view.update_display("Iniciando proceso Datacredito...", 10)
            
            # 1. Cargar datos (Modelo)
            self.model.load_plano_file(plano_path)
            self.view.update_display("Archivo plano cargado, iniciando transformaciones...", 30)
            
            # 2. Procesar datos (Modelo llama al Servicio)
            self.model.process_data(correcciones_path)
            self.view.update_display("Datos procesados, guardando archivo...", 80)
            
            # 3. Guardar datos (Modelo)
            self.model.save_processed_file(output_path)
            self.view.update_display(f"¡Éxito! Archivo guardado en {output_path}", 100)
            messagebox.showinfo("Proceso Completado", "El archivo de Datacredito ha sido procesado y guardado exitosamente.")

        except Exception as e:
            error_message = f"Error en el proceso: {e}"
            print(error_message)
            self.view.update_status(error_message)
            messagebox.showerror("Error", error_message)
        finally:
            # Limpia la barra de progreso después de un tiempo
            self.view.root.after(5000, lambda: self.view.update_progress(0))