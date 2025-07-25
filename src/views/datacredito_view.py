import tkinter as tk
from tkinter import ttk, filedialog, messagebox

class DataCreditoView:
    """Crea y gestiona la ventana para el proceso de archivos Datacredito."""
    def __init__(self, parent, controller):
        self.controller = controller
        self.top = tk.Toplevel(parent)
        self.top.title("Procesador de Archivos Datacredito")
        self.top.geometry("550x280")
        self.top.resizable(False, False)
        self.top.configure(bg="#ECECEC")

        self.plano_path = tk.StringVar()
        self.correcciones_path = tk.StringVar()

        # Llama al método que construye la interfaz
        self.create_widgets()

    def create_widgets(self):
        main_frame = ttk.Frame(self.top, padding="15")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # --- Selección de Archivo Plano ---
        ttk.Label(main_frame, text="1. Cargar Archivo Plano (.txt):").grid(row=0, column=0, sticky="w", pady=(0, 5))
        plano_entry = ttk.Entry(main_frame, textvariable=self.plano_path, width=60, state="readonly")
        plano_entry.grid(row=1, column=0, sticky="ew", padx=(0, 10))
        ttk.Button(main_frame, text="Seleccionar...", command=self.seleccionar_plano).grid(row=1, column=1, sticky="ew")

        # --- Selección de Archivo de Correcciones ---
        ttk.Label(main_frame, text="2. Cargar Archivo de Correcciones (.xlsx):").grid(row=2, column=0, sticky="w", pady=(15, 5))
        correcciones_entry = ttk.Entry(main_frame, textvariable=self.correcciones_path, width=60, state="readonly")
        correcciones_entry.grid(row=3, column=0, sticky="ew", padx=(0, 10))
        ttk.Button(main_frame, text="Seleccionar...", command=self.seleccionar_correcciones).grid(row=3, column=1, sticky="ew")
        
        # --- Botón para Iniciar el Proceso ---
        procesar_button = ttk.Button(main_frame, text="Generar Reporte", command=self.procesar)
        procesar_button.grid(row=4, column=0, columnspan=2, pady=(25, 10), ipady=5)

        # --- ¡CORRECCIÓN 1/2: SE AÑADE LA ETIQUETA DE ESTADO! ---
        # Esta línea crea el widget y lo guarda en self.status_label
        self.status_label = ttk.Label(main_frame, text="Listo para comenzar.", anchor="center")
        self.status_label.grid(row=5, column=0, columnspan=2, pady=(10, 0))

        main_frame.grid_columnconfigure(0, weight=1)

    def seleccionar_plano(self):
        filepath = filedialog.askopenfilename(title="Seleccionar archivo plano", filetypes=[("Archivos de Texto", "*.txt")])
        if filepath:
            self.plano_path.set(filepath)

    def seleccionar_correcciones(self):
        filepath = filedialog.askopenfilename(title="Seleccionar archivo de correcciones", filetypes=[("Archivos de Excel", "*.xlsx")])
        if filepath:
            self.correcciones_path.set(filepath)

    def procesar(self):
        plano = self.plano_path.get()
        correcciones = self.correcciones_path.get()
        
        if not plano or not correcciones:
            messagebox.showerror("Error", "Debes seleccionar ambos archivos para continuar.")
            return
        
        self.controller.run_processing_datacredito(self, plano, correcciones)

    # --- ¡CORRECCIÓN 2/2: SE AÑADE EL MÉTODO DE ACTUALIZACIÓN! ---
    # Este método permite que el controlador cambie el texto de la etiqueta
    def update_status(self, message):
        """Método para que el controlador actualice el texto de estado."""
        self.status_label.config(text=message)
        self.top.update_idletasks()