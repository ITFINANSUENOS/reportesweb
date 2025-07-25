import tkinter as tk
from tkinter import ttk, filedialog, messagebox

class CifinView:
    def __init__(self, parent, controller):
        self.controller = controller
        self.top = tk.Toplevel(parent)
        self.top.title("Procesador de Archivos CIFIN")
        self.top.geometry("550x280") # <-- Hice la ventana un poco más corta
        self.top.resizable(False, False)
        self.top.configure(bg="#ECECEC")

        self.input_txt_path = tk.StringVar()
        self.corrections_path = tk.StringVar()
        # --- Ya no necesitamos la variable para la ruta de salida aquí ---

        self.create_widgets()

    def create_widgets(self):
        main_frame = ttk.Frame(self.top, padding="15")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # --- Selección de Archivo de Entrada ---
        ttk.Label(main_frame, text="1. Cargar Archivo Plano CIFIN (.txt):").grid(row=0, column=0, sticky="w", pady=(0, 5))
        entry_input = ttk.Entry(main_frame, textvariable=self.input_txt_path, width=60, state="readonly")
        entry_input.grid(row=1, column=0, sticky="ew", padx=(0, 10))
        ttk.Button(main_frame, text="Seleccionar...", command=self.seleccionar_plano).grid(row=1, column=1, sticky="ew")

        # --- Selección de Archivo de Correcciones ---
        ttk.Label(main_frame, text="2. Cargar Archivo de Correcciones (.xlsx):").grid(row=2, column=0, sticky="w", pady=(15, 5))
        entry_corrections = ttk.Entry(main_frame, textvariable=self.corrections_path, width=60, state="readonly")
        entry_corrections.grid(row=3, column=0, sticky="ew", padx=(0, 10))
        ttk.Button(main_frame, text="Seleccionar...", command=self.seleccionar_correcciones).grid(row=3, column=1, sticky="ew")

        # --- Los widgets para el archivo de salida han sido eliminados ---
        
        # --- Botón para Iniciar el Proceso ---
        procesar_button = ttk.Button(main_frame, text="Generar Reporte", command=self.procesar)
        procesar_button.grid(row=4, column=0, columnspan=2, pady=(25, 10), ipady=5)
        
        # --- Etiqueta de Estado ---
        self.status_label = ttk.Label(main_frame, text="Listo para comenzar.", anchor="center")
        self.status_label.grid(row=5, column=0, columnspan=2, pady=(10, 0))

        main_frame.grid_columnconfigure(0, weight=1)

    def seleccionar_plano(self):
        filepath = filedialog.askopenfilename(title="Seleccionar archivo TXT", filetypes=[("Archivos de texto", "*.txt")])
        if filepath:
            self.input_txt_path.set(filepath)

    def seleccionar_correcciones(self):
        filepath = filedialog.askopenfilename(title="Seleccionar archivo Excel", filetypes=[("Archivos Excel", "*.xlsx")])
        if filepath:
            self.corrections_path.set(filepath)

    # --- Ya no necesitamos el método para seleccionar salida ---

    def procesar(self):
        plano = self.input_txt_path.get()
        correcciones = self.corrections_path.get()
        
        if not all([plano, correcciones]):
            messagebox.showerror("Error", "Debes seleccionar el archivo plano y el de correcciones.")
            return
        
        # Llama al controlador pasándole solo los archivos de entrada
        self.controller.run_processing(self, plano, correcciones)

    def update_status(self, message):
        self.status_label.config(text=message)
        self.top.update_idletasks()