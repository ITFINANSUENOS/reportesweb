import tkinter as tk
from tkinter import ttk
from pathlib import Path

class EcollectView(ttk.Frame):
    def __init__(self, parent, controller, main_window):
        super().__init__(parent)
        
        self.controller = controller
        self.main_window = main_window
        self.controller.set_view(self)
        
        # Usamos StringVars para vincular a los widgets Entry, como en tus otras vistas
        self.file_paths = {
            "PROCESO_VENCIMIENTOS": tk.StringVar(value="No se han seleccionado archivos."),
            "PROCESO_CONSULTA": tk.StringVar(value="No se ha seleccionado un archivo.")
        }

        self._create_widgets()

    def _create_widgets(self):
        """Crea la estructura principal de la vista con un área de scroll y layout centrado."""
        self.configure(style='TFrame') # Usamos el estilo base

        # --- Frame principal con scroll (similar a tus otras vistas) ---
        canvas = tk.Canvas(self, bg="#F0F0F0", highlightthickness=0)
        scrollbar = ttk.Scrollbar(self, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)

        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        scrollable_window = canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        def _on_canvas_configure(event):
            canvas_width = event.width
            canvas.itemconfig(scrollable_window, width=canvas_width)
        canvas.bind("<Configure>", _on_canvas_configure)
        
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # --- Layout Centrado usando grid (¡CAMBIO CLAVE!) ---
        scrollable_frame.grid_columnconfigure(0, weight=10)
        scrollable_frame.grid_columnconfigure(1, weight=80) # Columna principal para el contenido
        scrollable_frame.grid_columnconfigure(2, weight=10)

        main_container = ttk.Frame(scrollable_frame)
        main_container.grid(row=0, column=1, sticky="nsew", pady=(20, 0))

        # --- Tarjeta Única: Proceso completo ---
        self._create_proceso_unificado_card(main_container)

    def _create_proceso_unificado_card(self, parent):
        """Crea la tarjeta principal para el proceso de E-Collect."""
        card_frame = ttk.LabelFrame(parent, text=" Proceso: Generación de Planos E-Collect ", padding=20)
        card_frame.pack(fill='x', expand=True, pady=(0, 20))

        form_frame = ttk.Frame(card_frame)
        form_frame.pack(fill='x', expand=True)
        form_frame.grid_columnconfigure(0, weight=1) # Columna para Entry se expande

        # --- Paso 1: Campo para VENCIMIENTOS ---
        self._crear_campo_archivo(
            parent=form_frame,
            row_start=0,
            key="PROCESO_VENCIMIENTOS",
            desc="1. Seleccionar Archivo(s) de Vencimientos (.xlsx):",
            multiple=True
        )
        
        # --- Paso 2: Campo para CONSULTA ---
        self._crear_campo_archivo(
            parent=form_frame,
            row_start=2, # Dejamos una fila de espacio
            key="PROCESO_CONSULTA",
            desc="2. Seleccionar Archivo de Ventas (.xlsx):",
            multiple=False
        )

        # --- Botón de Acción con Estilo Centralizado (¡CAMBIO CLAVE!) ---
        procesar_button = ttk.Button(
            form_frame,
            text="▶ Iniciar Proceso y Generar Planos",
            command=self.controller.iniciar_proceso_completo,
            style='Modern.TButton' # Aplicamos el estilo de tus otros botones
        )
        procesar_button.grid(row=4, column=0, columnspan=2, pady=(20, 10), ipady=5)
        
    def _crear_campo_archivo(self, parent, row_start: int, key: str, desc: str, multiple: bool):
        """Función auxiliar para crear una fila de selección de archivo usando grid."""
        desc_label = ttk.Label(parent, text=desc)
        desc_label.grid(row=row_start, column=0, columnspan=2, sticky="w", pady=(10, 5))
        
        # --- Usamos un Entry en vez de un Label (¡CAMBIO CLAVE!) ---
        ruta_entry = ttk.Entry(parent, textvariable=self.file_paths[key], state="readonly")
        ruta_entry.grid(row=row_start + 1, column=0, sticky="ew", padx=(0, 10))
        
        command_lambda = lambda k=key, m=multiple: self.controller.seleccionar_archivo(k, m)
        boton = ttk.Button(parent, text="Seleccionar...", command=command_lambda)
        boton.grid(row=row_start + 1, column=1, sticky="ew")

    def actualizar_ruta_label(self, key: str, display_text: str):
        """Actualiza el texto del Entry para un archivo seleccionado."""
        if key in self.file_paths:
            self.file_paths[key].set(display_text)
            self.update_idletasks()