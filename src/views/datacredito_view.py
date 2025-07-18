import tkinter as tk
from tkinter import ttk, filedialog
from tkinter.font import Font
from src.views.config_view.config_view import AppConfig

# --- ¡NUEVO! Vista para la ventana de Centrales Datacredito ---
class DataCreditoView:
    """Crea la ventana para cargar los archivos de Datacredito."""
    def __init__(self, parent):
        # Crear una ventana Toplevel que se superpone a la principal
        self.top = tk.Toplevel(parent)
        self.top.title("Centrales Datacredito - Carga de Archivos")
        self.top.geometry("500x250")
        self.top.resizable(False, False)
        self.top.configure(bg="#ECECEC")

        # Guardar las rutas de los archivos
        self.plano_path = tk.StringVar()
        self.correcciones_path = tk.StringVar()

        # Marco principal
        main_frame = ttk.Frame(self.top, padding="15")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # --- Campo para el Archivo Plano ---
        plano_label = ttk.Label(main_frame, text="1. Cargar Archivo Plano (.txt):")
        plano_label.grid(row=0, column=0, sticky="w", pady=(0, 5))

        self.plano_entry = ttk.Entry(main_frame, textvariable=self.plano_path, width=50, state="readonly")
        self.plano_entry.grid(row=1, column=0, sticky="ew", padx=(0, 10))

        plano_button = ttk.Button(main_frame, text="Seleccionar...", command=self.seleccionar_plano)
        plano_button.grid(row=1, column=1, sticky="ew")

        # --- Campo para el Archivo de Correcciones ---
        correcciones_label = ttk.Label(main_frame, text="2. Cargar Archivo de Correcciones (.xlsx):")
        correcciones_label.grid(row=2, column=0, sticky="w", pady=(20, 5))

        self.correcciones_entry = ttk.Entry(main_frame, textvariable=self.correcciones_path, width=50, state="readonly")
        self.correcciones_entry.grid(row=3, column=0, sticky="ew", padx=(0, 10))

        correcciones_button = ttk.Button(main_frame, text="Seleccionar...", command=self.seleccionar_correcciones)
        correcciones_button.grid(row=3, column=1, sticky="ew")
        
        # --- Botón para procesar ---
        procesar_button = ttk.Button(main_frame, text="Procesar Archivos", command=self.procesar)
        procesar_button.grid(row=4, column=0, columnspan=2, pady=(25, 0), ipady=5)

        # Centrar los widgets en el marco
        main_frame.grid_columnconfigure(0, weight=1)

    def seleccionar_plano(self):
        """Abre el diálogo para seleccionar el archivo plano."""
        filepath = filedialog.askopenfilename(
            title="Selecciona el archivo plano",
            filetypes=(("Archivos de Texto", "*.txt"), ("Todos los archivos", "*.*"))
        )
        if filepath:
            self.plano_path.set(filepath)

    def seleccionar_correcciones(self):
        """Abre el diálogo para seleccionar el archivo de correcciones."""
        filepath = filedialog.askopenfilename(
            title="Selecciona el archivo de correcciones",
            filetypes=(("Archivos de Excel", "*.xlsx"), ("Todos los archivos", "*.*"))
        )
        if filepath:
            self.correcciones_path.set(filepath)

    def procesar(self):
        """Función que se llamará al presionar el botón 'Procesar'."""
        plano = self.plano_path.get()
        correcciones = self.correcciones_path.get()
        
        if not plano or not correcciones:
            # Puedes usar tkinter.messagebox para mostrar un error
            print("Error: Debes seleccionar ambos archivos.")
            return
            
        print("--- Archivos a procesar ---")
        print(f"Archivo Plano: {plano}")
        print(f"Archivo Correcciones: {correcciones}")
        # Aquí llamarías a la función del controlador que inicia el procesamiento
        # Por ejemplo: self.controller.procesar_archivos(plano, correcciones)
        self.top.destroy() # Cierra la ventana después de procesar


