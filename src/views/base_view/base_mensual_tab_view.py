import tkinter as tk
from tkinter import ttk, filedialog
from pathlib import Path

class BaseMensualView(ttk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.controller.set_view(self) 
        self.rutas_labels = {} 

        self.configure(style='TFrame')
        canvas = tk.Canvas(self, bg="#F0F0F0", highlightthickness=0)
        scrollbar = ttk.Scrollbar(self, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)

        scrollable_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        
        self.scrollable_window = canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        def _on_canvas_configure(event):
            canvas.itemconfig(self.scrollable_window, width=event.width)

        canvas.bind("<Configure>", _on_canvas_configure)
        
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # --- Rejilla 5%-90%-5% en el scrollable_frame para centrar el contenido ---
        scrollable_frame.grid_columnconfigure(0, weight=5)
        scrollable_frame.grid_columnconfigure(1, weight=90)
        scrollable_frame.grid_columnconfigure(2, weight=5)

        # --- NUEVO CONTENEDOR para poner todo en la columna central ---
        content_container = ttk.Frame(scrollable_frame)
        content_container.grid(row=0, column=1, sticky="nsew")

        # --- AHORA, TODO EL CONTENIDO VA DENTRO DE 'content_container' ---
        ttk.Label(content_container, text="Cargar Archivos para Reporte Base", style='Title.TLabel').pack(pady=(20, 20))

        self.update_mode_var = tk.BooleanVar(value=False)
        action_frame = ttk.Frame(content_container, padding="10")
        action_frame.pack(fill=tk.X, pady=(0, 10))
        update_switch = ttk.Checkbutton(
            action_frame, text="⚡ Modo Actualización Rápida (usar base anterior)",
            variable=self.update_mode_var, command=self._toggle_base_report_visibility
        )
        update_switch.pack(pady=(0, 10), anchor='w')
        self.base_report_frame = ttk.LabelFrame(action_frame, text="Cargar Base Anterior", padding="10")
        self.base_report_path_label = ttk.Label(self.base_report_frame, text="Ningún reporte base seleccionado...", width=60)
        self.base_report_path_label.pack(side=tk.LEFT, padx=5, expand=True, fill=tk.X)
        ttk.Button(self.base_report_frame, text="Seleccionar...", command=self.controller.seleccionar_reporte_base).pack(side=tk.LEFT, padx=5)
        
        files_container = ttk.Frame(content_container)
        files_container.pack(fill='x', expand=True)

        self._create_file_group(files_container, "Reportes de Cartera", {
            "ANALISIS": "Análisis de Cartera (ARP y FNS)", "R91": "Reportes R91 (ARP y FS)",
            "VENCIMIENTOS": "Vencimientos (ARP y FNS)", "R03": "Reportes R03 (Codeudores)",
        })
        self._create_file_group(files_container, "Desembolsos y Ventas", {
            "SC04": "Desembolsos Arpesod (SC04)", "FNZ001": "Desembolsos Finansueños (FNZ001)",
            "CRTMPCONSULTA1": "Reporte de ventas CRTMPCONSULTA1",
        })
        self._create_file_group(files_container, "Datos Complementarios", {
            "FNZ003": "Saldos FNZ003", "MATRIZ_CARTERA": "Matriz de Cartera",
            "METAS_FRANJAS": "Metas por Franjas", "ASESORES": "Asesores Activos",
        })
        
        # --- Frame para las fechas ---
        dates_frame = ttk.LabelFrame(content_container, text=" Rango de Fechas del Reporte ", padding="10")
        dates_frame.pack(fill='x', expand=True, pady=10)
        
        # --- Widget para Fecha de Inicio ---
        ttk.Label(dates_frame, text="Fecha de Inicio:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        # La siguiente línea es la que soluciona el error
        self.start_date_entry = ttk.Entry(dates_frame)
        self.start_date_entry.grid(row=0, column=1, sticky="ew", padx=5)
        
        # --- Widget para Fecha de Fin (es muy probable que también lo necesites) ---
        ttk.Label(dates_frame, text="Fecha de Fin:").grid(row=0, column=2, sticky="w", padx=(15, 5), pady=5)
        self.end_date_entry = ttk.Entry(dates_frame)
        self.end_date_entry.grid(row=0, column=3, sticky="ew", padx=5)

        # Hacemos que los campos de entrada se expandan
        dates_frame.grid_columnconfigure(1, weight=1)
        dates_frame.grid_columnconfigure(3, weight=1)

        final_action_frame = ttk.Frame(content_container, padding="10")
        final_action_frame.pack(fill=tk.X, pady=20)
        self.procesar_button = ttk.Button(final_action_frame, text="▶ Procesar Base Mensual", command=self.controller.procesar_archivos, style='Modern.TButton')
        self.procesar_button.pack(pady=10, ipady=5)
        self.status_label = ttk.Label(final_action_frame, text="Esperando archivos...", anchor="center")
        self.status_label.pack(pady=10, fill='x')
        self.progress_bar = ttk.Progressbar(final_action_frame, orient='horizontal', mode='determinate')
        self.progress_bar.pack(pady=5, fill=tk.X)
        self._toggle_base_report_visibility()

    def _create_file_group(self, parent, title, files):
        group_frame = ttk.LabelFrame(parent, text=f" {title} ", padding=15)
        group_frame.pack(fill='x', expand=True, pady=10)
        for i, (key, desc) in enumerate(files.items()):
            ttk.Label(group_frame, text=f"{desc}:").grid(row=i, column=0, sticky="w", padx=5, pady=5)
            ruta_label = ttk.Label(group_frame, text="No seleccionado", relief="sunken", anchor="w", padding=5)
            ruta_label.grid(row=i, column=1, sticky="ew", padx=5)
            self.rutas_labels[key] = ruta_label
            ttk.Button(group_frame, text="Seleccionar...", command=lambda k=key: self.controller.seleccionar_archivo(k)).grid(row=i, column=2, sticky="e", padx=5)
        group_frame.grid_columnconfigure(1, weight=1)

    def _toggle_base_report_visibility(self):
        if self.update_mode_var.get(): self.base_report_frame.pack(fill=tk.X, padx=5, pady=(5, 10))
        else: self.base_report_frame.pack_forget()

    def actualizar_ruta_label(self, tipo_archivo, display_text):
        if tipo_archivo in self.rutas_labels:
            self.rutas_labels[tipo_archivo].config(text=display_text, style='Success.TLabel')
            self.update_idletasks()
    
    def actualizar_estado(self, mensaje, progreso=None):
        self.status_label.config(text=mensaje)
        if progreso is not None: self.progress_bar.config(value=progreso)
        self.update_idletasks()

class NovedadesView(ttk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.controller.set_view(self)
        
        self.rutas_novedades, self.rutas_analisis, self.rutas_r91, self.ruta_usuarios, self.ruta_reporte_base = [], [], [], [], ""
        self.rutas_call_center = []
        self.ruta_nomina = ""
        self.label_novedades_path = tk.StringVar(value="No seleccionado")
        self.label_analisis_path = tk.StringVar(value="No seleccionado")
        self.label_r91_path = tk.StringVar(value="No seleccionado")
        self.label_usuarios_path = tk.StringVar(value="No seleccionado")
        self.label_base_path = tk.StringVar(value="No seleccionado")
        self.label_call_center_path = tk.StringVar(value="No seleccionado")
        self.label_nomina_path = tk.StringVar(value="No seleccionado")

        self.configure(style='TFrame')
        
        canvas = tk.Canvas(self, bg="#F0F0F0", highlightthickness=0)
        scrollbar = ttk.Scrollbar(self, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)

        scrollable_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        self.scrollable_window = canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        def _on_canvas_configure(event):
            canvas.itemconfig(self.scrollable_window, width=event.width)

        canvas.bind("<Configure>", _on_canvas_configure)
        
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        scrollable_frame.grid_columnconfigure(0, weight=5)
        scrollable_frame.grid_columnconfigure(1, weight=90)
        scrollable_frame.grid_columnconfigure(2, weight=5)
        
        content_frame = ttk.LabelFrame(scrollable_frame, text=" Módulo de Novedades y Análisis Mensual ", padding="20")
        content_frame.grid(row=0, column=1, sticky="nsew", pady=20)

        file_inputs = {
            "1. Cargar Reporte Base Mensual (.xlsx):": (self.label_base_path, self.seleccionar_reporte_base),
            "2. Cargar Archivo(s) de Novedades (.xlsx):": (self.label_novedades_path, self.seleccionar_novedades),
            "3. Cargar Archivo(s) de Análisis (.xlsx):": (self.label_analisis_path, self.seleccionar_analisis),
            "4. Cargar Archivo(s) de Recaudos (R91):": (self.label_r91_path, self.seleccionar_r91),
            "5. Cargar Archivo de Usuarios:": (self.label_usuarios_path, self.seleccionar_usuarios),
            "6. Cargar Archivo(s) de Call Center:": (self.label_call_center_path, self.seleccionar_call_center)
        }

        self.current_row = 0
        for i, (text, (var, cmd)) in enumerate(file_inputs.items()):
            row = i * 2
            ttk.Label(content_frame, text=text).grid(row=row, column=0, columnspan=2, sticky="w", pady=(15, 5))
            ttk.Entry(content_frame, textvariable=var, state="readonly").grid(row=row + 1, column=0, sticky="ew", padx=(0, 10))
            ttk.Button(content_frame, text="Seleccionar...", command=cmd).grid(row=row + 1, column=1, sticky="ew")
            self.current_row = row + 1 # Actualizamos el contador de fila

        # Movemos el contador a la siguiente fila disponible
        self.current_row += 1
        
        self.calcular_nomina_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            content_frame,
            text="Calcular Nómina",
            variable=self.calcular_nomina_var,
            command=self._toggle_nomina_visibility
        ).grid(row=self.current_row, column=0, columnspan=2, sticky="w", pady=(20, 5))

        # La fila para el módulo de nómina será la siguiente a la del checkbox
        self.nomina_frame_row = self.current_row + 1
        
        self.nomina_frame = ttk.LabelFrame(content_frame, text=" Cargar Archivo de Nómina ", padding="10")
        
        ttk.Entry(self.nomina_frame, textvariable=self.label_nomina_path, state="readonly").pack(side=tk.LEFT, expand=True, fill=tk.X, padx=(0, 10))
        ttk.Button(self.nomina_frame, text="Seleccionar...", command=self.seleccionar_nomina).pack(side=tk.LEFT)
        
        # --- CORRECCIÓN 2: El botón de procesar ahora se coloca en la fila siguiente al módulo opcional de nómina ---
        self.process_button_row = self.nomina_frame_row + 1
        ttk.Button(
            content_frame,
            text="▶ Procesar y Generar Reporte",
            command=self.procesar,
            style='Modern.TButton'
        ).grid(row=self.process_button_row, column=0, columnspan=2, pady=(30, 10), ipady=5)

        content_frame.grid_columnconfigure(0, weight=1)
        
        self._toggle_nomina_visibility()

    # --- CORRECCIÓN 3: La función de visibilidad ahora usa la fila que calculamos ---
    def _toggle_nomina_visibility(self):
        """Muestra u oculta el frame para cargar el archivo de nómina."""
        if self.calcular_nomina_var.get():
            # Usamos la variable de instancia para colocarlo en la fila correcta
            self.nomina_frame.grid(row=self.nomina_frame_row, column=0, columnspan=2, sticky="ew", padx=5, pady=5)
        else:
            self.nomina_frame.grid_forget()

    def _get_excel_file_types(self):
        return [
            ("Archivos de Excel", "*.xlsx *.XLSX *.xlsm *.XLSM *.xls *.XLS"),
            ("Todos los archivos", "*.*")
        ]
    
    # --- NUEVA FUNCIÓN PARA SELECCIONAR EL ARCHIVO DE NÓMINA ---
    def seleccionar_nomina(self):
        filepath = filedialog.askopenfilename(title="Seleccionar Archivo de Nómina", filetypes=self._get_excel_file_types())
        if filepath:
            self.ruta_nomina = filepath
            self.label_nomina_path.set(Path(filepath).name)    
    
    def _get_excel_file_types(self):
        """Función auxiliar para tener un único formato de tipos de archivo."""
        return [
            # Añadimos las extensiones en mayúsculas para asegurar compatibilidad
            ("Archivos de Excel", "*.xlsx *.XLSX *.xlsm *.XLSM *.xls *.XLS"),
            ("Todos los archivos", "*.*")
        ]

    def seleccionar_r91(self):
        # --- CORREGIDO Y ESTANDARIZADO ---
        filepaths = filedialog.askopenfilenames(title="Seleccionar Archivo(s) R91", filetypes=self._get_excel_file_types())
        if filepaths: self.rutas_r91 = list(filepaths); self.label_r91_path.set(f"{len(self.rutas_r91)} archivo(s) seleccionado(s)")

    def seleccionar_reporte_base(self):
        # --- CORREGIDO Y ESTANDARIZADO ---
        filepath = filedialog.askopenfilename(title="Seleccionar Reporte Base Mensual", filetypes=self._get_excel_file_types())
        if filepath: self.ruta_reporte_base = filepath; self.label_base_path.set(Path(filepath).name)

    def seleccionar_novedades(self):
        # --- CORREGIDO Y ESTANDARIZADO ---
        filepaths = filedialog.askopenfilenames(title="Seleccionar Archivo(s) de Novedades", filetypes=self._get_excel_file_types())
        if filepaths: self.rutas_novedades = list(filepaths); self.label_novedades_path.set(f"{len(self.rutas_novedades)} archivo(s) seleccionado(s)")

    def seleccionar_usuarios(self):
        # --- CORREGIDO Y ESTANDARIZADO ---
        filepaths = filedialog.askopenfilenames(title="Seleccionar Archivo(s) de Usuarios", filetypes=self._get_excel_file_types())
        if filepaths: self.ruta_usuarios = list(filepaths); self.label_usuarios_path.set(f"{len(self.ruta_usuarios)} archivo(s) seleccionado(s)")

    def seleccionar_analisis(self):
        filepaths = filedialog.askopenfilenames(title="Seleccionar Archivo(s) de Análisis", filetypes=self._get_excel_file_types())
        if filepaths: self.rutas_analisis = list(filepaths); self.label_analisis_path.set(f"{len(self.rutas_analisis)} archivo(s) seleccionado(s)")

    def seleccionar_call_center(self):
        filepaths = filedialog.askopenfilenames(title="Seleccionar Archivo(s) de Call Center", filetypes=self._get_excel_file_types())
        if filepaths:
            self.rutas_call_center = list(filepaths)
            self.label_call_center_path.set(f"{len(self.rutas_call_center)} archivo(s) seleccionado(s)")

    def procesar(self):
        # Ahora pasamos el estado del checkbox y la ruta del archivo de nómina al controlador
        self.controller.procesar_archivos(
            ruta_base=self.ruta_reporte_base, rutas_novedades=self.rutas_novedades,
            rutas_analisis=self.rutas_analisis, rutas_r91=self.rutas_r91,
            ruta_usuarios=self.ruta_usuarios,
            rutas_call_center=self.rutas_call_center,
            calcular_nomina=self.calcular_nomina_var.get(),
            ruta_nomina=self.ruta_nomina
        )

class BaseMensualTabView(ttk.Frame):
    """
    Esta es la vista principal para la pestaña 'Base Mensual'.
    Ahora contiene un Notebook con las sub-pestañas 'Reporte Base' y 'Reporte Novedades'.
    """
    def __init__(self, parent, base_mensual_controller, novedades_analisis_controller, main_window_controller):
        super().__init__(parent)
        self.configure(style='TFrame')
        
        # --- Creamos el Notebook para las sub-pestañas ---
        notebook = ttk.Notebook(self)
        notebook.pack(fill="both", expand=True, pady=10, padx=5)
        
        # --- Creamos el contenido de la primera pestaña ---
        reporte_base_frame = BaseMensualView(notebook, base_mensual_controller)
        notebook.add(reporte_base_frame, text="  Reporte Base  ")
        
        # --- Creamos el contenido de la segunda pestaña ---
        reporte_novedades_frame = NovedadesView(notebook, novedades_analisis_controller)
        notebook.add(reporte_novedades_frame, text="  Reporte Novedades  ")