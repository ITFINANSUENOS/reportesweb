import tkinter as tk
from tkinter import ttk

class ConveniosAnticiposView(ttk.Frame):
    """
    Una vista que agrupa los botones de 'Cruce de convenios' y 'Anticipos Online'.
    """
    def __init__(self, parent, convenios_controller, anticipos_controller, main_window_controller):
        super().__init__(parent)
        self.convenios_controller = convenios_controller
        self.anticipos_controller = anticipos_controller
        
        # --- Estilo y configuración ---
        # (Los estilos se definen globalmente en MainWindow, así que puedes simplificar aquí)
        self.configure(style='TFrame', padding=20)

        # --- Contenido de la vista ---
        # Título del módulo
        title_label = ttk.Label(self, text="Módulo de Convenios y Anticipos", style='Title.TLabel')
        title_label.pack(pady=(20, 30))

        # Contenedor para los botones, para centrarlos fácilmente
        button_container = ttk.Frame(self)
        button_container.pack(expand=True)

        # Botones de acción
        action1_button = ttk.Button(
            button_container, text="Cruce de convenios",
            command=self.convenios_controller.start_report_generation, 
            style='Modern.TButton'
        )
        action1_button.pack(pady=10, ipady=5, ipadx=10)
        
        action2_button = ttk.Button(
            button_container, text="Anticipos Online",
            command=self.anticipos_controller.start_report_generation, 
            style='Modern.TButton'
        )
        action2_button.pack(pady=10, ipady=5, ipadx=10)