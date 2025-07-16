from tkinter import filedialog, messagebox
from src.services.anticipos.anticipos_service import AnticiposService

class AnticiposController:
    def __init__(self,view):
        self.view = view
        self.service = AnticiposService()
    
    def start_report_generation(self):
        """Inicia el flujo completo a petición de la vista."""
        file_path = filedialog.askopenfilename(
            title='Selecciona el archivo de Anticipos',
            filetypes=[('Archivos Excel', '*.xlsx')]
        )
        if not file_path:
            self.view.update_display("Operación cancelada.", 0)
            return

        try:
            # Define una función para que el servicio actualice la UI
            def status_update_callback(message, progress):
                self.view.update_display(message, progress)

            # 1. Llama al servicio para que haga el trabajo pesado
            final_sheets = self.service.generate_report_data(file_path, status_update_callback)

            output_path = filedialog.asksaveasfilename(
                title="Guardar reporte de Anticipos como...",
                initialfile=self.service.config.output_filename,
                defaultextension=".xlsx"
            )
            if not output_path:
                self.view.update_display("Guardado cancelado.", 0)
                return
            
            # 2. Llama a un método del servicio para guardar el archivo
            self.view.update_display("Guardando archivo formateado...", 95)
            self.service.save_report(output_path, final_sheets,status_update_callback)
            
            messagebox.showinfo("Éxito", f"Reporte generado exitosamente en:\n{output_path}")

        except Exception as e:
            messagebox.showerror("Error", f"Ocurrió un error: {str(e)}")
        finally:
            self.view.update_display("Proceso finalizado.", 100)