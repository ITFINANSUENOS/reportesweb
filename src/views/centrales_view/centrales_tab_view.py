import tkinter as tk
from tkinter import ttk, filedialog, messagebox

class BaseCentralesView(ttk.Frame):
    """
    CLASE BASE: Ahora incluye la estructura de scroll y centrado.
    Se ha eliminado el botón 'Volver', ya que no es necesario con las pestañas.
    """
    def __init__(self, parent, datacredito_controller, cifin_controller, empresa_name):
        super().__init__(parent)
        self.datacredito_controller = datacredito_controller
        self.cifin_controller = cifin_controller
        self.empresa_name = empresa_name
        
        self.dc_plano_path = tk.StringVar()
        self.dc_correcciones_path = tk.StringVar()
        self.cifin_plano_path = tk.StringVar()
        self.cifin_correcciones_path = tk.StringVar()

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

        # --- Rejilla 5%-90%-5% en el scrollable_frame para centrar ---
        scrollable_frame.grid_columnconfigure(0, weight=5)
        scrollable_frame.grid_columnconfigure(1, weight=90)
        scrollable_frame.grid_columnconfigure(2, weight=5)

        # --- Contenedor que irá en la columna central ---
        content_container = ttk.Frame(scrollable_frame)
        content_container.grid(row=0, column=1, sticky="nsew")

        # --- Título Principal ---
        ttk.Label(content_container, text=f"Centrales {empresa_name.upper()}", style='Title.TLabel').pack(pady=(20, 20))

        # --- SECCIÓN DATACREDITO ---
        dc_frame = ttk.LabelFrame(content_container, text=" Datacredito ", padding="15")
        dc_frame.pack(fill=tk.X, expand=True, pady=10)
        
        ttk.Label(dc_frame, text="1. Cargar Archivo Plano (.txt):").grid(row=0, column=0, sticky="w", pady=(0, 5))
        ttk.Entry(dc_frame, textvariable=self.dc_plano_path, width=60, state="readonly").grid(row=1, column=0, sticky="ew", padx=(0, 10))
        ttk.Button(dc_frame, text="Seleccionar...", command=self._select_dc_plano).grid(row=1, column=1, sticky="ew")
        
        ttk.Label(dc_frame, text="2. Cargar Correcciones (.xlsx):").grid(row=2, column=0, sticky="w", pady=(15, 5))
        ttk.Entry(dc_frame, textvariable=self.dc_correcciones_path, width=60, state="readonly").grid(row=3, column=0, sticky="ew", padx=(0, 10))
        ttk.Button(dc_frame, text="Seleccionar...", command=self._select_dc_corrections).grid(row=3, column=1, sticky="ew")
        
        ttk.Button(dc_frame, text="▶ Generar Reporte Datacredito", command=self._process_datacredito, style='Modern.TButton').grid(row=4, column=0, columnspan=2, pady=(20, 10), ipady=5)
        dc_frame.grid_columnconfigure(0, weight=1)
        
        
        self.status_label = ttk.Label(content_container, text="Listo", style="Status.TLabel", anchor="center")
        self.status_label.pack(fill=tk.X, pady=(20, 10))


        # --- SECCIÓN CIFIN ---
        cifin_frame = ttk.LabelFrame(content_container, text=" CIFIN ", padding="15")
        cifin_frame.pack(fill=tk.X, expand=True, pady=10)

        ttk.Label(cifin_frame, text="1. Cargar Archivo Plano CIFIN (.txt):").grid(row=0, column=0, sticky="w", pady=(0, 5))
        ttk.Entry(cifin_frame, textvariable=self.cifin_plano_path, width=60, state="readonly").grid(row=1, column=0, sticky="ew", padx=(0, 10))
        ttk.Button(cifin_frame, text="Seleccionar...", command=self._select_cifin_plano).grid(row=1, column=1, sticky="ew")

        ttk.Label(cifin_frame, text="2. Cargar Correcciones (.xlsx):").grid(row=2, column=0, sticky="w", pady=(15, 5))
        ttk.Entry(cifin_frame, textvariable=self.cifin_correcciones_path, width=60, state="readonly").grid(row=3, column=0, sticky="ew", padx=(0, 10))
        ttk.Button(cifin_frame, text="Seleccionar...", command=self._select_cifin_corrections).grid(row=3, column=1, sticky="ew")
        
        ttk.Button(cifin_frame, text="▶ Generar Reporte CIFIN", command=self._process_cifin, style='Modern.TButton').grid(row=4, column=0, columnspan=2, pady=(20, 10), ipady=5)
        cifin_frame.grid_columnconfigure(0, weight=1)
        
        
    # --- AÑADIDO: El método que el controlador necesita ---
    def update_status(self, message):
        """Actualiza la etiqueta de estado de esta vista."""
        self.status_label.config(text=message)
        self.update_idletasks() # Asegura que la UI se refresque inmediatamente

    def _select_dc_plano(self):
        filepath = filedialog.askopenfilename(title="Seleccionar plano Datacredito", filetypes=[("Archivos de Texto", "*.txt")])
        if filepath: self.dc_plano_path.set(filepath)
    def _select_dc_corrections(self):
        filepath = filedialog.askopenfilename(title="Seleccionar correcciones Datacredito", filetypes=[("Archivos de Excel", "*.xlsx")])
        if filepath: self.dc_correcciones_path.set(filepath)
    def _select_cifin_plano(self):
        filepath = filedialog.askopenfilename(title="Seleccionar plano CIFIN", filetypes=[("Archivos de texto", "*.txt")])
        if filepath: self.cifin_plano_path.set(filepath)
    def _select_cifin_corrections(self):
        filepath = filedialog.askopenfilename(title="Seleccionar correcciones CIFIN", filetypes=[("Archivos Excel", "*.xlsx")])
        if filepath: self.cifin_correcciones_path.set(filepath)
    def _process_datacredito(self):
        raise NotImplementedError("Este método debe ser implementado por la clase hija.")
    def _process_cifin(self):
        raise NotImplementedError("Este método debe ser implementado por la clase hija.")
    

class CentralesArpesodView(BaseCentralesView):
    # --- CAMBIO: Se elimina 'menu_controller' del init ---
    def __init__(self, parent, datacredito_controller, cifin_controller):
        super().__init__(parent, datacredito_controller, cifin_controller, "ARPESOD")

    def _process_datacredito(self):
        messagebox.showinfo("Proceso Datacredito", "Iniciando proceso para Datacredito ARPESOD...")
    def _process_cifin(self):
        if not self.cifin_plano_path.get() or not self.cifin_correcciones_path.get():
            messagebox.showerror("Error", "Debe seleccionar ambos archivos para CIFIN.")
            return
        self.cifin_controller.set_empresa_actual(self.empresa_name)
        self.cifin_controller.run_processing(self.cifin_plano_path.get(), self.cifin_correcciones_path.get())

class CentralesFinansuenosView(BaseCentralesView):
    # --- CAMBIO: Se elimina 'menu_controller' del init ---
    def __init__(self, parent, datacredito_controller, cifin_controller):
        super().__init__(parent, datacredito_controller, cifin_controller, "FINANSUEÑOS")

    def _process_datacredito(self):
        if not self.dc_plano_path.get() or not self.dc_correcciones_path.get():
            messagebox.showerror("Error", "Debe seleccionar ambos archivos para Datacredito.")
            return
        self.datacredito_controller.set_empresa_actual(self.empresa_name)
        self.datacredito_controller.run_processing_datacredito(self, self.dc_plano_path.get(), self.dc_correcciones_path.get())
    def _process_cifin(self):
        if not self.cifin_plano_path.get() or not self.cifin_correcciones_path.get():
            messagebox.showerror("Error", "Debe seleccionar ambos archivos para CIFIN.")
            return
        self.cifin_controller.set_empresa_actual(self.empresa_name)
        self.cifin_controller.run_processing(self.cifin_plano_path.get(), self.cifin_correcciones_path.get())

class CentralesTabView(ttk.Frame):
    """
    --- CAMBIO ESTRUCTURAL ---
    Esta vista ahora contiene un Notebook con las sub-pestañas 'ARPESOD' y 'FINANSUEÑOS',
    eliminando la necesidad de un menú de botones y de cambiar vistas manualmente.
    """
    def __init__(self, parent, datacredito_controller, cifin_controller, main_window_controller):
        super().__init__(parent)
        self.configure(style='TFrame')

        # --- Creamos el Notebook para las sub-pestañas ---
        notebook = ttk.Notebook(self)
        notebook.pack(fill="both", expand=True, pady=10, padx=5)

        # --- Creamos el contenido de la primera pestaña ---
        arpesod_frame = CentralesArpesodView(notebook, datacredito_controller, cifin_controller)
        notebook.add(arpesod_frame, text="  ARPESOD  ")

        # --- Creamos el contenido de la segunda pestaña ---
        finansuenos_frame = CentralesFinansuenosView(notebook, datacredito_controller, cifin_controller)
        notebook.add(finansuenos_frame, text="  FINANSUEÑOS  ")