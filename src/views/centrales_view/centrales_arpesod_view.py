import tkinter as tk
from tkinter import ttk

class CentralesArpesodView(ttk.Frame):
    """
    Vista para el módulo de Centrales de ARPESOD.
    (Actualmente es un marcador de posición).
    """
    def __init__(self, parent, main_window_controller):
        super().__init__(parent)
        self.main_window_controller = main_window_controller
        
        # --- Estilos ---
        style = ttk.Style()
        style.configure('Module.TFrame', background='#F0F0F0')
        self.configure(style='Module.TFrame')

        # --- Botón para volver al menú de Centrales ---
        ttk.Button(self, text="← Volver a Centrales", 
                   command=lambda: self.main_window_controller.mostrar_vista("centrales_menu")
        ).pack(anchor="nw", padx=10, pady=10)
        
        # --- Contenido de la vista ---
        ttk.Label(self, text="Módulo de Centrales ARPESOD\n(En Construcción)", 
                  font=("Helvetica", 18, "bold"),
                  justify=tk.CENTER,
                  style='ModuleTitle.TLabel'
        ).pack(expand=True)