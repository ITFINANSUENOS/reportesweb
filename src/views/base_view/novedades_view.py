import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from pathlib import Path

class NovedadesView(ttk.Frame):
    """
    Vista para el módulo de Reporte de Novedades y Análisis.
    """
    def __init__(self, parent, controller, main_window_controller):
        super().__init__(parent)
        self.controller = controller
        self.controller.set_view(self)
        self.main_window_controller = main_window_controller
        
        self.rutas_novedades = []
        self.rutas_analisis = []
        self.rutas_r91 = []
        self.ruta_usuarios = ""
        self.ruta_reporte_base = ""
        self.ruta_nomina = ""

        self.label_novedades_path = tk.StringVar(value="No seleccionado")
        self.label_analisis_path = tk.StringVar(value="No seleccionado")
        self.label_r91_path = tk.StringVar(value="No seleccionado")
        self.label_usuarios_path = tk.StringVar(value="No seleccionado")
        self.label_base_path = tk.StringVar(value="No seleccionado")

        # --- Estilos ---
        style = ttk.Style()
        style.configure('Module.TFrame', background='#F0F0F0')
        self.configure(style='Module.TFrame')

        # --- Botón Volver ---
        # Lo ponemos en un frame separado para que no sea afectado por el scroll
        top_frame = ttk.Frame(self, style='Module.TFrame')
        top_frame.pack(fill=tk.X, anchor="n")
        
        ttk.Button(top_frame, text="← Volver a Base Mensual", 
                   command=lambda: self.main_window_controller.mostrar_vista("base_mensual_menu")
        ).pack(anchor="nw", padx=10, pady=10)
        
        # --- ESTRUCTURA PARA EL SCROLL ---
        # 1. Un frame contenedor que usará .grid para organizar el canvas y el scrollbar
        container = ttk.Frame(self, style='Module.TFrame')
        container.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))
        container.grid_rowconfigure(0, weight=1)
        container.grid_columnconfigure(0, weight=1)
        
        # 2. El Canvas principal que podrá ser "scrolleado"
        canvas = tk.Canvas(container, bg='#F0F0F0', highlightthickness=0)
        
        # 3. El Scrollbar vertical, vinculado al canvas
        scrollbar = ttk.Scrollbar(container, orient="vertical", command=canvas.yview)
        
        # 4. Un Frame "scrollable" DENTRO del canvas. ¡Aquí irá todo tu contenido!
        self.scrollable_frame = ttk.Frame(canvas, style='Module.TFrame')

        # 5. "Dibuja" el frame scrollable dentro del canvas
        self.scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(
                scrollregion=canvas.bbox("all")
            )
        )
        canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # 6. Posiciona el Canvas y el Scrollbar en el 'container'
        canvas.grid(row=0, column=0, sticky="nsew")
        scrollbar.grid(row=0, column=1, sticky="ns")

        # --- Contenido de la vista ---
        # IMPORTANTE: Ahora el padre de este widget es 'self.scrollable_frame', no 'self'
        content_frame = ttk.LabelFrame(self.scrollable_frame, text=" Módulo de Novedades y Análisis Mensual ", padding="15")
        content_frame.pack(fill=tk.X, expand=True, padx=10, pady=5)

        # --- SECCIÓN 1: REPORTE BASE ---
        ttk.Label(content_frame, text="1. Cargar Reporte Base Mensual (.xlsx):").grid(row=0, column=0, columnspan=2, sticky="w", pady=(10, 5))
        ttk.Entry(content_frame, textvariable=self.label_base_path, state="readonly").grid(row=1, column=0, sticky="ew", padx=(0, 10))
        ttk.Button(content_frame, text="Seleccionar...", command=self.seleccionar_reporte_base).grid(row=1, column=1, sticky="ew")

        # --- SECCIÓN 2: NOVEDADES ---
        ttk.Label(content_frame, text="2. Cargar Archivo(s) de Novedades (.xlsx):").grid(row=2, column=0, columnspan=2, sticky="w", pady=(15, 5))
        ttk.Entry(content_frame, textvariable=self.label_novedades_path, state="readonly").grid(row=3, column=0, sticky="ew", padx=(0, 10))
        ttk.Button(content_frame, text="Seleccionar...", command=self.seleccionar_novedades).grid(row=3, column=1, sticky="ew")

        # --- SECCIÓN 3: ANÁLISIS ---
        ttk.Label(content_frame, text="3. Cargar Archivo(s) de Análisis (.xlsx):").grid(row=4, column=0, columnspan=2, sticky="w", pady=(15, 5))
        ttk.Entry(content_frame, textvariable=self.label_analisis_path, state="readonly").grid(row=5, column=0, sticky="ew", padx=(0, 10))
        ttk.Button(content_frame, text="Seleccionar...", command=self.seleccionar_analisis).grid(row=5, column=1, sticky="ew")

        # --- SECCIÓN 4: RECAUDOS R91 ---
        ttk.Label(content_frame, text="4. Cargar Archivo(s) de Recaudos (R91):").grid(row=6, column=0, columnspan=2, sticky="w", pady=(15, 5))
        ttk.Entry(content_frame, textvariable=self.label_r91_path, state="readonly").grid(row=7, column=0, sticky="ew", padx=(0, 10))
        ttk.Button(content_frame, text="Seleccionar...", command=self.seleccionar_r91).grid(row=7, column=1, sticky="ew")
        
        # --- SECCIÓN 5: USUARIOS (NUEVA SECCIÓN) ---
        ttk.Label(content_frame, text="5. Cargar Archivo(s) de Usuarios:").grid(row=8, column=0, columnspan=2, sticky="w", pady=(15, 5))
        ttk.Entry(content_frame, textvariable=self.label_usuarios_path, state="readonly").grid(row=9, column=0, sticky="ew", padx=(0, 10))
        ttk.Button(content_frame, text="Seleccionar...", command=self.seleccionar_usuarios).grid(row=9, column=1, sticky="ew")

        
        # --- BOTÓN DE PROCESAR ---
        ttk.Button(content_frame, text="▶ Procesar y Generar Reporte", command=self.procesar, style='Accent.TButton').grid(row=8, column=0, columnspan=2, pady=(25, 10), ipady=5)
        
        content_frame.grid_columnconfigure(0, weight=1)

    def seleccionar_r91(self):
        file_types = [("Archivos de Excel", "*.xlsx *.XLSX *.xls *.XLS"), ("Todos los archivos", "*.*")]
        filepaths = filedialog.askopenfilenames(title="Seleccionar Archivo(s) R91", filetypes=file_types)
        if filepaths:
            self.rutas_r91 = list(filepaths)
            self.label_r91_path.set(f"{len(self.rutas_r91)} archivo(s) seleccionado(s)")


    def seleccionar_reporte_base(self):
        file_types = [("Archivos de Excel", "*.xlsx *.XLSX .xls .XLS"), ("Todos los archivos", ".")]
        filepath = filedialog.askopenfilename(title="Seleccionar Reporte Base Mensual", filetypes=file_types)
        if filepath:
            self.ruta_reporte_base = filepath
            self.label_base_path.set(Path(filepath).name)

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


    def seleccionar_usuarios(self):
        file_types = [("Archivos de Excel", "*.xlsx *.XLSX *.xls *.XLS"), ("Todos los archivos", "*.*")]
        filepaths = filedialog.askopenfilenames(title="Seleccionar Archivo(s) de Usuarios", filetypes=file_types)
        if filepaths:
            self.ruta_usuarios = list(filepaths)
            self.label_usuarios_path.set(f"{len(self.ruta_usuarios)} archivo(s) seleccionado(s)")

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
            ruta_base=self.ruta_reporte_base,
            rutas_novedades=self.rutas_novedades,
            rutas_analisis=self.rutas_analisis,
            rutas_r91=self.rutas_r91,
            ruta_usuarios=self.ruta_usuarios
        )