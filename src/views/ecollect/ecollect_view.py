import tkinter as tk
from tkinter import ttk

class EcollectView(ttk.Frame):
    def __init__(self, parent, controller, main_window):
        super().__init__(parent)
        
        self.controller = controller
        self.main_window = main_window
        
        # Asignamos la vista al controlador para que pueda comunicarse con ella
        self.controller.set_view(self)

        self._create_widgets()

    def _create_widgets(self):
        # Botón para cargar y procesar el archivo
        process_button = ttk.Button(
            self,
            text="Cargar y Procesar Archivo de Vencimientos",
            style="Modern.TButton",
            # Conectamos el botón al método del controlador
            command=self.controller.procesar_archivos_vencimientos
        )
        process_button.pack(pady=50, padx=50, ipady=10) # ipady para hacerlo más alto