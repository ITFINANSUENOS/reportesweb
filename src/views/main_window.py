# src/views/main_window.py

import tkinter as tk
from tkinter import ttk
import pandas as pd
from src.views.config_view.config_view import AppConfig
from src.views.config_view.style_assets import create_rounded_button_images

# --- CAMBIO: Importamos solo las vistas que actuarán como PESTAÑAS PRINCIPALES ---
from src.views.convenios_anticipos_view.convenios_anticipos_view import ConveniosAnticiposView
from src.views.base_view.base_menu_view import BaseMensualMenuView
from src.views.base_view.base_mensual_tab_view import BaseMensualTabView
from src.views.centrales_view.centrales_menu_view import CentralesMenuView
from src.views.centrales_view.centrales_tab_view import CentralesTabView    

# Las vistas secundarias (como BaseMensualView, NovedadesView, etc.) ahora
# serán gestionadas por sus vistas de menú correspondientes, no por MainWindow.

class MainWindow:
    def __init__(self, root, controller_convenios, controller_anticipos, controller_base_mensual,
                 controller_datacredito, controller_cifin, controller_novedades_analisis):
        
        self.root = root
        self.config = AppConfig()
        
        self.button_images = create_rounded_button_images(self.config)

        # --- NUEVO: Guardamos los controllers en un diccionario para un acceso más limpio ---
        self.controllers = {
            "convenios": controller_convenios,
            "anticipos": controller_anticipos,
            "base_mensual": controller_base_mensual,
            "datacredito": controller_datacredito,
            "cifin": controller_cifin,
            "novedades_analisis": controller_novedades_analisis
        }
        
        # --- Configuración de la Ventana Principal ---
        self.root.title(self.config.title)
        self.root.geometry(self.config.geometry)
        self.root.resizable(False, False)
        self.root.configure(background=self.config.bg_color)
        
        # --- NUEVO: Métodos para organizar la creación de la UI ---
        self._setup_styles()
        self._create_main_layout()
        controller_anticipos.set_view(self)
        controller_convenios.set_view(self)

    def _setup_styles(self):
        """
        NUEVO: Configura todos los estilos de ttk en un solo lugar para mayor consistencia.
        """
        style = ttk.Style()
        style.theme_use('clam')
        bg_color = self.config.bg_color
        style.configure('TFrame', background=bg_color)
        style.configure('TNotebook', background=bg_color, borderwidth=0)
        
        
        # 1. Cargamos las imágenes (sin cambios)
        self.button_normal_img = self.button_images["normal"]
        self.button_hover_img = self.button_images["hover"]
        self.button_pressed_img = self.button_images["pressed"]

        # 2. Creamos el elemento de fondo con la imagen (sin cambios)
        style.element_create("Modern.Button.background", "image", self.button_normal_img,
            ('active', self.button_hover_img),
            ('pressed', self.button_pressed_img),
            border=10, sticky="nsew")

        style.layout("Modern.TButton", [
            ('Modern.Button.background', {'sticky': 'nsew'}),
            ('Button.padding', {'sticky': 'nsew', 'children': [
                ('Button.label', {'sticky': 'nsew'})
            ]})
        ])
        
        style.configure("Modern.TButton", 
                        font=("Helvetica", 12, "bold"),
                        foreground=self.config.button_text_color,
                        borderwidth=0, 
                        highlightthickness=0, 
                        anchor="center", # Asegura que el texto esté centrado
                        padding=10)


    def _create_main_layout(self):
        """
        NUEVO: Crea la estructura visual principal de la aplicación con título,
        pestañas y una barra de estado persistente.
        """
        
        # Contenedor principal con un padding general
        main_frame = ttk.Frame(self.root, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Título de la aplicación
        title_label = ttk.Label(main_frame, text=self.config.title, style='Title.TLabel')
        title_label.pack(pady=(0, 20), anchor="center")

        # --- Creación del Notebook que contendrá las pestañas ---
        notebook = ttk.Notebook(main_frame, style='TNotebook')
        notebook.pack(fill=tk.BOTH, expand=True)

        convenios_view = ConveniosAnticiposView(
            notebook, 
            self.controllers["convenios"], 
            self.controllers["anticipos"], 
            self
        )
        notebook.add(convenios_view, text="  Convenios y Anticipos  ") # Espacios para más padding

        base_mensual_tab = BaseMensualTabView(
            notebook,
            self.controllers["base_mensual"],
            self.controllers["novedades_analisis"],
            self
        )
        notebook.add(base_mensual_tab, text="  Base Mensual  ")

        # Pestaña 3: Centrales de Riesgo
        centrales_tab = CentralesTabView(
            notebook,
            self.controllers["datacredito"],
            self.controllers["cifin"],
            self
        )
        notebook.add(centrales_tab, text="  Centrales de Riesgo  ")


        # --- Barra de Estado y Pie de Página (siempre visible) ---
        status_frame = ttk.Frame(main_frame, padding=(0, 15, 0, 0))
        status_frame.pack(fill=tk.X, side=tk.BOTTOM)
        
        self.status_label = ttk.Label(status_frame, text="Estado: Listo", style='Status.TLabel')
        self.status_label.pack(side=tk.LEFT)
        
        footer_text = f"© {pd.Timestamp.now().year} Departamento Financiero"
        footer_label = ttk.Label(status_frame, text=footer_text)
        footer_label.pack(side=tk.RIGHT)
    
    def update_status(self, message: str):
        """Actualiza el texto de estado en la barra inferior."""
        self.status_label.config(text=f"Estado: {message}")
        self.root.update_idletasks()

    def update_progress(self, progress: int):
        """
        Actualiza una barra de progreso. Puedes implementarla en la barra de estado.
        Por ahora, solo imprime en consola para mantener la funcionalidad.
        """
        print(f"Progreso: {progress}%")
        self.root.update_idletasks()
    
    def update_display(self, message: str, progress: int):
        """Método unificado que actualiza estado y progreso."""
        self.update_status(message)
        self.update_progress(progress)