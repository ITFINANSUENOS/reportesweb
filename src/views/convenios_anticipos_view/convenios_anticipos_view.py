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
        self.main_window_controller = main_window_controller

        # --- Estilo y configuración ---
        style = ttk.Style()
        style.configure('Module.TFrame', background='#F0F0F0')
        style.configure('ModuleTitle.TLabel', background='#F0F0F0', font=("Helvetica", 16, "bold"))
        self.configure(style='Module.TFrame')

        # --- Botón para volver al menú principal ---
        top_bar_frame = ttk.Frame(self, style='Module.TFrame')
        top_bar_frame.pack(fill=tk.X, padx=10, pady=5)
        back_button = ttk.Button(top_bar_frame, text="← Volver al Menú Principal", command=self.volver_al_menu)
        back_button.pack(anchor="nw")

        # --- Contenido de la vista ---
        content_frame = ttk.Frame(self, style='Module.TFrame')
        content_frame.pack(fill=tk.BOTH, expand=True, pady=20)

        # Título del módulo
        title_label = ttk.Label(content_frame, text="Módulo de Convenios y Anticipos", style='ModuleTitle.TLabel')
        title_label.pack(pady=(20, 30))

        # Botones de acción
        action1_button = ttk.Button(
            content_frame, text="Cruce de convenios",
            command=self.convenios_controller.start_report_generation, style='Accent.TButton'
        )
        action1_button.pack(pady=10, ipadx=20, ipady=10)
        
        action2_button = ttk.Button(
            content_frame, text="Anticipos Online",
            command=self.anticipos_controller.start_report_generation, style='Accent.TButton'
        )
        action2_button.pack(pady=10, ipadx=20, ipady=10)

    def volver_al_menu(self):
        """Llama al método de la ventana principal para volver al menú."""
        self.main_window_controller.mostrar_vista("menu")