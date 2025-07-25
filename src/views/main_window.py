import tkinter as tk
from tkinter import ttk
from tkinter.font import Font
from src.views.config_view.config_view import AppConfig

class MainWindow:
    def __init__(self, root, controller_convenios, controller_anticipos, controller_base_mensual,
                 controller_datacredito,controller_cifin):
        
        self.root = root
        self.convenios_controller = controller_convenios
        self.anticipos_controller = controller_anticipos
        self.base_mensual_controller = controller_base_mensual
        self.datacredito_controller = controller_datacredito
        self.cifin_controller = controller_cifin
        self.config = AppConfig()
        self.setup_ui()
        
    def setup_ui(self):
        """Configura la interfaz de usuario."""
        self.root.title(self.config.title)
        self.root.geometry(self.config.geometry)
        self.root.resizable(False, False)
        self.root.configure(bg=self.config.bg_color)
        
        # Configurar estilo
        self.style = ttk.Style()
        self.style.theme_use('clam')
        
        # Fuentes
        self.title_font = Font(family="Helvetica", size=16, weight="bold")
        self.button_font = Font(family="Arial", size=12)
        self.label_font = Font(family="Arial", size=10)
        
        # Marco principal
        self.main_frame = ttk.Frame(self.root, padding="20")
        self.main_frame.pack(fill=tk.BOTH, expand=True)
        self.main_frame.configure(style='Card.TFrame')
        
        # Estilo para el marco
        # Estilo para el marco
        self.style.configure('Card.TFrame', background=self.config.bg_color)
        
        # Título
        self.title_label = ttk.Label(
            self.main_frame, 
            text=self.config.title, 
            font=self.title_font,
            background=self.config.bg_color,
            foreground=self.config.secondary_color
        )
        self.title_label.pack(pady=(0, 20))
        
        # Descripción
        self.desc_label = ttk.Label(
            self.main_frame,
            text="Esta herramienta procesa archivos Excel con información financiera\ny genera un reporte consolidado.",
            font=self.label_font,
            background=self.config.bg_color,
            foreground=self.config.text_color,
            justify=tk.CENTER
        )
        self.desc_label.pack(pady=(0, 30))
        
        
        # Frame principal que contendrá las dos filas de botones
        self.buttons_container_frame = ttk.Frame(self.main_frame, style='Card.TFrame')
        self.buttons_container_frame.pack(pady=(0, 20))

        # Frame para la fila SUPERIOR de botones
        self.top_row_frame = ttk.Frame(self.buttons_container_frame, style='Card.TFrame')
        self.top_row_frame.pack(pady=(0, 10))

        # Frame para la fila INFERIOR de botones
        self.bottom_row_frame = ttk.Frame(self.buttons_container_frame, style='Card.TFrame')
        self.bottom_row_frame.pack()
        
        # --- Botones de la Fila Superior (3 botones) ---
        self.action1_button = ttk.Button(
            self.top_row_frame, # <-- Se añade al marco superior
            text="Cruce de convenios",
            command=self.convenios_controller.start_report_generation,
            style='Accent.TButton'
        )
        self.action1_button.pack(side=tk.LEFT, padx=10, ipadx=10, ipady=5)
        
        self.action2_button = ttk.Button(
            self.top_row_frame, # <-- Se añade al marco superior
            text="Anticipos Online",
            command=self.anticipos_controller.start_report_generation,
            style='Accent.TButton'
        )
        self.action2_button.pack(side=tk.LEFT, padx=10, ipadx=10, ipady=5)
        
        self.base_mensual_button = ttk.Button(
            self.top_row_frame, # <-- Se añade al marco superior
            text="Base Mensual",
            command=lambda: self.base_mensual_controller.abrir_vista(self.root),
            style='Accent.TButton'
        )
        self.base_mensual_button.pack(side=tk.LEFT, padx=10, ipadx=10, ipady=5)

        # --- Botones de la Fila Inferior (2 botones) ---
        self.datacredito_button = ttk.Button(
            self.bottom_row_frame, # <-- Se añade al marco inferior
            text="Centrales Datacredito",
            command=lambda: self.datacredito_controller.abrir_vista_datacredito(self.root),
            style='Accent.TButton'
        )
        self.datacredito_button.pack(side=tk.LEFT, padx=10, ipadx=10, ipady=5)

        self.cifin_button = ttk.Button(
            self.bottom_row_frame, # <-- Se añade al marco inferior
            text="Centrales CIFIN",
            command=lambda: self.cifin_controller.open_cifin_window(self.root),
            style='Accent.TButton'
        )
        self.cifin_button.pack(side=tk.LEFT, padx=10, ipadx=10, ipady=5)
        
        # Configurar estilo para el botón de acento
        self.style.configure('Accent.TButton', font=self.button_font, foreground='white', background=self.config.accent_color)
        self.style.map('Accent.TButton',
                     background=[('active', self.config.secondary_color), ('pressed', self.config.secondary_color)])
        
        
        self.progress_bar = ttk.Progressbar(
            self.main_frame,
            orient=tk.HORIZONTAL,
            length=300,
            mode='determinate'
        )
        self.progress_bar.pack(pady=(10, 0))
        self.progress_bar['value'] = 0
        self.progress_bar.pack_forget()  # Ocultarla inicialmente

        # Estado del proceso (status_label)
        self.status_label = ttk.Label(
            self.main_frame,
            text="Estado: Inactivo",
            font=self.label_font,
            background=self.config.bg_color,
            foreground=self.config.secondary_color
        )
        self.status_label.pack(pady=(10, 0))
        
        # Pie de página
        self.footer_label = ttk.Label(
            self.main_frame,
            text="© 2023 Departamento Financiero",
            font=self.label_font,
            background=self.config.bg_color,
            foreground=self.config.text_color
        )
        self.footer_label.pack(side=tk.BOTTOM, pady=(20, 0))
    
    def update_status(self, message: str):
        """Actualiza solo el texto de estado."""
        self.status_label.config(text=message)
        self.root.update_idletasks()

    
    def update_progress(self, progress: int):
        """Actualiza solo la barra de progreso."""
        self.progress_bar['value'] = progress
        if progress > 0 and not self.progress_bar.winfo_viewable():
            self.progress_bar.pack(pady=(10, 0))
        self.root.update_idletasks()
    
    def update_display(self, message: str, progress: int):
        """Método unificado que actualiza tanto el texto de estado como la barra de progreso."""
        self.update_status(message)
        self.update_progress(progress)    