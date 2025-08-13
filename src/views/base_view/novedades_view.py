import tkinter as tk
from tkinter import ttk, filedialog, messagebox

class NovedadesView(ttk.Frame):
    """
    Vista para el módulo de Reporte de Novedades y Análisis.
    """
    def __init__(self, parent, controller, main_window_controller):
        super().__init__(parent)
        self.controller = controller
        # Le decimos al controlador cuál es su vista
        self.controller.set_view(self)
        self.main_window_controller = main_window_controller
        
        self.rutas_novedades = []
        self.rutas_analisis = []
        self.rutas_r91 = []

        self.label_novedades_path = tk.StringVar(value="No seleccionado")
        self.label_analisis_path = tk.StringVar(value="No seleccionado")
        self.label_r91_path = tk.StringVar(value="No seleccionado")

        # --- Estilos ---
        style = ttk.Style()
        style.configure('Module.TFrame', background='#F0F0F0')
        self.configure(style='Module.TFrame')

        ttk.Button(self, text="← Volver a Base Mensual", 
                   command=lambda: self.main_window_controller.mostrar_vista("base_mensual_menu")
        ).pack(anchor="nw", padx=10, pady=10)
        
        # --- Contenido de la vista ---
        content_frame = ttk.LabelFrame(self, text=" Módulo de Novedades y Análisis Mensual ", padding="15")
        content_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=10)
        
        # --- Selección del Archivo de Novedades ---
        ttk.Label(content_frame, text="1. Cargar Archivo(s) de Novedades (.xlsx):").grid(row=0, column=0, columnspan=2, sticky="w", pady=(10, 5))
        ttk.Entry(content_frame, textvariable=self.label_novedades_path, state="readonly").grid(row=1, column=0, sticky="ew", padx=(0, 10))
        ttk.Button(content_frame, text="Seleccionar...", command=self.seleccionar_novedades).grid(row=1, column=1, sticky="ew")

        # --- Selección del Archivo de Análisis ---
        ttk.Label(content_frame, text="2. Cargar Archivo(s) de Análisis (.xlsx):").grid(row=2, column=0, columnspan=2, sticky="w", pady=(15, 5))
        ttk.Entry(content_frame, textvariable=self.label_analisis_path, state="readonly").grid(row=3, column=0, sticky="ew", padx=(0, 10))
        ttk.Button(content_frame, text="Seleccionar...", command=self.seleccionar_analisis).grid(row=3, column=1, sticky="ew")

       # --- Selección del Archivo de Recaudos R91 ---
        ttk.Label(content_frame, text="3. Cargar Archivo(s) de Recaudos (R91):").grid(row=4, column=0, columnspan=2, sticky="w", pady=(15, 5))
        ttk.Entry(content_frame, textvariable=self.label_r91_path, state="readonly").grid(row=5, column=0, sticky="ew", padx=(0, 10))
        ttk.Button(content_frame, text="Seleccionar...", command=self.seleccionar_r91).grid(row=5, column=1, sticky="ew")
        
        
        # --- Botón de Procesar ---
        ttk.Button(content_frame, text="▶ Procesar y Generar Reporte", command=self.procesar, style='Accent.TButton').grid(row=6, column=0, columnspan=2, pady=(25, 10), ipady=5)
        content_frame.grid_columnconfigure(0, weight=1)

    def seleccionar_r91(self):
        file_types = [("Archivos de Excel", "*.xlsx *.XLSX *.xls *.XLS"), ("Todos los archivos", "*.*")]
        filepaths = filedialog.askopenfilenames(title="Seleccionar Archivo(s) R91", filetypes=file_types)
        if filepaths:
            self.rutas_r91 = list(filepaths)
            self.label_r91_path.set(f"{len(self.rutas_r91)} archivo(s) seleccionado(s)")


    def seleccionar_novedades(self):
         # Permite seleccionar múltiples archivos
        file_types = [
            ("Archivos de Excel", "*.xlsx *.XLSX *.xls *.XLS"),
            ("Todos los archivos", "*.*")
        ]
        
        filepaths = filedialog.askopenfilenames(
            title="Seleccionar Archivo(s) de Novedades",
            filetypes=file_types 
        )
        if filepaths:
            self.rutas_novedades = list(filepaths)
            # Actualiza la etiqueta para mostrar cuántos archivos se seleccionaron
            self.label_novedades_path.set(f"{len(self.rutas_novedades)} archivo(s) seleccionado(s)")


    def seleccionar_analisis(self):
        # Permite seleccionar múltiples archivos
        file_types = [
            ("Archivos de Excel", "*.xlsx *.XLSX *.xls *.XLS"),
            ("Todos los archivos", "*.*")
        ]

        filepaths = filedialog.askopenfilenames(
            title="Seleccionar Archivo(s) de Análisis",
            filetypes=file_types # <-- Se usa la nueva lista
        )
        if filepaths:
            self.rutas_analisis = list(filepaths)
            self.label_analisis_path.set(f"{len(self.rutas_analisis)} archivo(s) seleccionado(s)")


    def procesar(self):
        # Llama al controlador con las LISTAS de rutas
        self.controller.procesar_archivos(
            rutas_novedades=self.rutas_novedades,
            rutas_analisis=self.rutas_analisis,
            rutas_r91=self.rutas_r91
        )