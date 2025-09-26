import tkinter as tk
from tkinter import ttk


class BaseMensualMenuView(ttk.Frame):
    """
    Vista que sirve como sub-menú para el módulo de Base Mensual.
    """
    def __init__(self, parent, main_window_controller):
        super().__init__(parent)
        self.main_window_controller = main_window_controller
        
        # --- Estilos ---
        style = ttk.Style()
        style.configure('Module.TFrame', background='#F0F0F0')
        style.configure('ModuleTitle.TLabel', background='#F0F0F0', font=("Helvetica", 16, "bold"))
        self.configure(style='Module.TFrame')

        # --- Botón para volver al menú principal ---
        ttk.Button(self, text="← Volver al Menú Principal", 
                   command=lambda: self.main_window_controller.mostrar_vista("menu")
        ).pack(anchor="nw", padx=10, pady=10)
        
        # --- Contenido de la vista ---
        content_frame = ttk.Frame(self, style='Module.TFrame')
        content_frame.pack(fill=tk.BOTH, expand=True, pady=20)
        
        ttk.Label(content_frame, text="Módulo Base Mensual", 
                  style='ModuleTitle.TLabel'
        ).pack(pady=(20, 30))
        
        # --- Botones del sub-menú ---
        ttk.Button(content_frame, text="Reporte Base", 
                   command=lambda: self.main_window_controller.mostrar_vista("base_mensual_carga"),
                   style='Accent.TButton'
        ).pack(pady=10, ipadx=20, ipady=10)
        
        ttk.Button(content_frame, text="Reporte Novedades", 
                   command=lambda: self.main_window_controller.mostrar_vista("reporte_novedades"),
                   style='Accent.TButton'
        ).pack(pady=10, ipadx=20, ipady=10)