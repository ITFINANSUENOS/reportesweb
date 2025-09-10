import tkinter as tk
from tkinter import ttk
import pandas as pd 
from src.views.config_view.config_view import AppConfig
from src.views.base_view.base_view import BaseMensualView
from src.views.convenios_anticipos_view.convenios_anticipos_view import ConveniosAnticiposView
from src.views.centrales_view.centrales_menu_view import CentralesMenuView
from src.views.centrales_view.centrales_arpesod_view import CentralesArpesodView
from src.views.centrales_view.centrales_finansuenos_view import CentralesFinansuenosView
from src.views.base_view.base_menu_view import BaseMensualMenuView
from src.views.base_view.novedades_view import NovedadesView

class MainWindow:
    def __init__(self, root, controller_convenios, controller_anticipos, controller_base_mensual,
                 controller_datacredito, controller_cifin, controller_novedades_analisis):
        
        self.root = root
        # Guardamos los controllers para pasarlos a las vistas que los necesiten
        self.convenios_controller = controller_convenios
        self.anticipos_controller = controller_anticipos
        self.base_mensual_controller = controller_base_mensual
        self.datacredito_controller = controller_datacredito
        self.cifin_controller = controller_cifin
        self.novedades_analisis_controller = controller_novedades_analisis
        
        self.config = AppConfig()
        
        # --- Configuración básica de la ventana (se hace una sola vez) ---
        self.root.title(self.config.title)
        self.root.geometry(self.config.geometry)
        self.root.resizable(False, False)
        
        # --- Contenedor principal que alojará todas las vistas ---
        container = ttk.Frame(self.root)
        container.pack(fill=tk.BOTH, expand=True)
        container.grid_rowconfigure(0, weight=1)
        container.grid_columnconfigure(0, weight=1)


        menu_frame = self._crear_menu_principal(container) 
        base_carga_frame = BaseMensualView(container, self.base_mensual_controller, self) 
        self.base_mensual_controller.set_view(base_carga_frame)
        convenios_frame = ConveniosAnticiposView(container, self.convenios_controller, self.anticipos_controller, self)
        centrales_menu_frame = CentralesMenuView(container, self)
        centrales_arpesod_frame = CentralesArpesodView(container, self.datacredito_controller, self.cifin_controller, self)
        centrales_finansuenos_frame = CentralesFinansuenosView(container, self.datacredito_controller, self.cifin_controller, self)
        base_menu_frame = BaseMensualMenuView(container, self)
        novedades_frame = NovedadesView(container, self.novedades_analisis_controller, self)
        # Las guardamos en el diccionario con un nombre clave

        self.frames = {
            "menu": menu_frame,
            "convenios_anticipos": convenios_frame,
            "centrales_menu": centrales_menu_frame,
            "centrales_arpesod": centrales_arpesod_frame,
            "centrales_finansuenos": centrales_finansuenos_frame,
            "base_mensual_menu": base_menu_frame,
            "base_mensual_carga": base_carga_frame,
            "reporte_novedades": novedades_frame
        }
        
        for frame in self.frames.values():
            frame.grid(row=0, column=0, sticky="nsew")

        self.mostrar_vista("menu")
        
    def mostrar_vista(self, nombre_vista):
        frame = self.frames[nombre_vista]
        frame.tkraise()
        
    def _crear_menu_principal(self, parent):
        """
        Crea y devuelve el Frame que contiene todos los widgets del menú principal.
        """
        main_frame = ttk.Frame(parent, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # --- Configurar Estilo y Fuentes ---
        style = ttk.Style()
        style.theme_use('clam')
        
        # Estilo para los frames que actúan como fondo
        style.configure('Card.TFrame', background=self.config.bg_color)
        
        # Estilo para el Título
        style.configure('Title.TLabel', 
                        background=self.config.bg_color, 
                        foreground=self.config.secondary_color, 
                        font=("Helvetica", 16, "bold"))
                        
        # Estilo para el texto normal (descripción y pie de página)
        style.configure('Normal.TLabel', 
                        background=self.config.bg_color, 
                        foreground=self.config.text_color, 
                        font=("Arial", 10))

        # Estilo para el texto de estado
        style.configure('Status.TLabel', 
                        background=self.config.bg_color, 
                        foreground=self.config.secondary_color, 
                        font=("Arial", 10))
        
        # Estilo para los botones
        style.configure('Accent.TButton', 
                        font=("Arial", 12), 
                        foreground='white', 
                        background=self.config.accent_color)
        style.map('Accent.TButton', 
                background=[('active', self.config.secondary_color), ('pressed', self.config.secondary_color)])

        # --- Creación de Widgets usando los estilos ---

        # Título
        title_label = ttk.Label(
            main_frame, 
            text=self.config.title, 
            style='Title.TLabel'
        )
        title_label.pack(pady=(0, 20))
        
        # Descripción
        desc_label = ttk.Label(
            main_frame,
            text="Esta herramienta procesa archivos Excel con información financiera\ny genera un reporte consolidado.",
            style='Normal.TLabel',
            justify=tk.CENTER
        )
        desc_label.pack(pady=(0, 30))
        
        # Contenedor de botones
        buttons_container_frame = ttk.Frame(main_frame, style='Card.TFrame')
        buttons_container_frame.pack(pady=(0, 20))
        top_row_frame = ttk.Frame(buttons_container_frame, style='Card.TFrame')
        top_row_frame.pack(pady=(0, 10))
        bottom_row_frame = ttk.Frame(buttons_container_frame, style='Card.TFrame')
        bottom_row_frame.pack()
        
        convenios_anticipos_button = ttk.Button(
            top_row_frame,
            text="Convenios y Anticipos",
            command=lambda: self.mostrar_vista("convenios_anticipos"), # <-- Llama a la nueva vista
            style='Accent.TButton'
        )
        convenios_anticipos_button.pack(side=tk.LEFT, padx=10, ipadx=10, ipady=5)
        
        base_mensual_button = ttk.Button(
            top_row_frame, 
            text="Base Mensual",
            command=lambda: self.mostrar_vista("base_mensual_menu"), # <-- Llama al NUEVO sub-menú
            style='Accent.TButton'
        )
        base_mensual_button.pack(side=tk.LEFT, padx=10, ipadx=10, ipady=5)

        # --- Botones de la Fila Inferior ---
        centrales_button = ttk.Button(
            bottom_row_frame,
            text="Centrales de Riesgo",
            command=lambda: self.mostrar_vista("centrales_menu"), # <-- Llama al nuevo menú de centrales
            style='Accent.TButton'
        )
        centrales_button.pack(padx=10, ipadx=10, ipady=5)
        
        # Estado del proceso
        self.status_label = ttk.Label(
            main_frame, 
            text="Estado: Inactivo", 
            style='Status.TLabel'
        )
        self.status_label.pack(pady=(10, 0))
        
        # Pie de página
        footer_label = ttk.Label(
            main_frame, 
            text=f"© {pd.Timestamp.now().year} Departamento Financiero", 
            style='Normal.TLabel'
        )
        footer_label.pack(side=tk.BOTTOM, pady=(20, 0))
        return main_frame

    def update_status(self, message: str):
        """Actualiza solo el texto de estado."""
        self.status_label.config(text=message)
        self.root.update_idletasks()

    def update_progress(self, progress: int):
        """Actualiza solo la barra de progreso."""
        if not self.progress_bar.winfo_viewable():
            self.progress_bar.pack(pady=(10, 0))
        self.progress_bar['value'] = progress
        self.root.update_idletasks()
    
    def update_display(self, message: str, progress: int):
        """Método unificado que actualiza tanto el texto de estado como la barra de progreso."""
        self.update_status(message)
        self.update_progress(progress)   