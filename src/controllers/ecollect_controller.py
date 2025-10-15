from tkinter import filedialog
import pandas as pd
from src.services.ecollect.ecollect_service import EcollectService
from src.services.ecollect.plano_service import PlanoService
from src.models.ecollect_model import configuracion


class EcollectController:
    def __init__(self):
        self.view = None
        self.ecollect_service = EcollectService(configuracion)
        self.plano_service = PlanoService()

    def set_view(self, view):
        self.view = view

    def procesar_archivos_vencimientos(self):
        if not self.view:
            print("Error: La vista no ha sido asignada al controlador.")
            return

        file_paths = filedialog.askopenfilenames(
            title="Seleccione los archivos de vencimientos",
            filetypes=(("Archivos de Excel", "*.xlsx *.xls"), ("Todos los archivos", "*.*"))
        )

        if not file_paths:
            self.view.main_window.update_status("Operación cancelada por el usuario.")
            return

        file_names = [path.split('/')[-1] for path in file_paths]
        self.view.main_window.update_status(f"Procesando {len(file_names)} archivos...")

        try:
            resultado_df = self.ecollect_service.process_vencimientos(list(file_paths))
            
            if resultado_df is not None and not resultado_df.empty:
                self.view.main_window.update_status("¡Proceso completado! Elija dónde guardar el archivo plano.")
                
                # --- 3. LÓGICA DE GUARDADO ACTUALIZADA ---
                # Generar el nombre de archivo dinámico
                fecha_hoy = pd.Timestamp.now().strftime('%Y%m%d')
                nombre_archivo_sugerido = f"carga_cartera_{fecha_hoy}_10791.txt"

                save_path = filedialog.asksaveasfilename(
                    title="Guardar archivo plano como...",
                    initialfile=nombre_archivo_sugerido, # <-- Nombre sugerido
                    defaultextension=".txt",
                    filetypes=[("Archivos de Texto", "*.txt"), ("Todos los archivos", "*.*")]
                )
                
                if save_path:
                    # Llamar al PlanoService para generar y guardar el archivo
                    success = self.plano_service.generar_archivo_plano(resultado_df, save_path)
                    
                    if success:
                        self.view.main_window.update_status(f"¡Archivo plano guardado en {save_path.split('/')[-1]}!")
                    else:
                        self.view.main_window.update_status("Error al generar o guardar el archivo plano.")
                else:
                    self.view.main_window.update_status("Guardado cancelado por el usuario.")
                # --- FIN DE LA LÓGICA DE GUARDADO ---
                
            else:
                self.view.main_window.update_status("Los archivos no contenían datos para procesar.")

        except Exception as e:
            self.view.main_window.update_status(f"Error durante el procesamiento: {e}")
            print(f"Error detallado: {e}")