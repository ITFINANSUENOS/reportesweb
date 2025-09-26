import os
from tkinter import filedialog, messagebox
from src.services.convenios.convenios_service import ConveniosService  


class ConveniosController:
    def __init__(self, view):
        self.view = view
        self.service = ConveniosService()

    def set_view(self, view):
        """
        Guarda una referencia a la vista para que el controlador 
        pueda comunicarse con ella (ej. para actualizar un mensaje de estado).
        """
        self.view = view    
        
    def start_report_generation(self,input_path):
       
        try:
            # 1. Llamar al servicio para que genere los datos
            df_bancolombia, df_efecty = self.service.generate_report(
                input_path, 
                self.view.update_display
            )
            
            # 2. Pedir la ruta de guardado
            output_path = filedialog.asksaveasfilename(
                title="Guardar Reporte Financiero como...",
                initialfile=self.service.config.output_filename,
                defaultextension=".xlsx"
            )
            if not output_path:
                self.view.update_display("Guardado cancelado.", 0)
                return

            # 3. Llamar al servicio para que guarde los datos
            self.view.update_display("Guardando archivo formateado...", 95)
            self.service.save_report(output_path, df_bancolombia, df_efecty)
            
            messagebox.showinfo("Éxito", f"Reporte Financiero generado en:\n{output_path}")

        except Exception as e:
            messagebox.showerror("Error", f"Ocurrió un error en el reporte financiero: {str(e)}")
        finally:
            if hasattr(self.view, 'update_status'):
                self.view.update_status("Proceso finalizado.")
