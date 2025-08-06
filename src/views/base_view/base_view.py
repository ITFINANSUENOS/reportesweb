import tkinter as tk
from tkinter import ttk
from pathlib import Path
from tkinter.font import Font

class BaseMensualView(ttk.Frame):
    """
    Vista para seleccionar todos los archivos necesarios para generar la base mensual.
    Ahora es un Frame que se puede poner dentro de la ventana principal.
    """
    def __init__(self, parent, controller, main_window_controller):
        super().__init__(parent)
        self.controller = controller
        self.main_window_controller = main_window_controller
        self.rutas_labels = {} 

        # --- Estilo para esta vista ---
        style = ttk.Style()
        style.configure('Base.TFrame', background='#F0F0F0')
        style.configure('Card.TFrame', background='#FFFFFF')
        style.configure('Title.TLabel', background='#FFFFFF', font=("Helvetica", 14, "bold"))
        style.configure('Desc.TLabel', background='#FFFFFF', width=35)
        style.configure('Status.TLabel', background='#FFFFFF')
        
        # Estilo para la etiqueta cuando un archivo se selecciona con éxito
        style.configure('Success.TLabel',
                        background="#D4EDDA",
                        foreground="#155724",
                        relief="flat",
                        padding=5)

        self.configure(style='Base.TFrame')

        # --- Botón para volver al menú principal ---
        top_bar_frame = ttk.Frame(self, style='Base.TFrame')
        top_bar_frame.pack(fill=tk.X, padx=10, pady=5)
        back_button = ttk.Button(top_bar_frame, text="← Volver a Base Mensual", command=self.volver_al_menu)
        back_button.pack(anchor="nw")

        # --- Frame principal con scroll ---
        main_frame = ttk.Frame(self, padding="10", style='Base.TFrame')
        main_frame.pack(fill=tk.BOTH, expand=True)

        canvas = tk.Canvas(main_frame, bg="#F0F0F0", highlightthickness=0)
        scrollbar = ttk.Scrollbar(main_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas, style='Card.TFrame')

        scrollable_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # Título
        title_label = ttk.Label(scrollable_frame, text="Cargar Archivos para el Reporte", style='Title.TLabel')
        title_label.pack(pady=20, padx=20)

        archivos_requeridos = {
            "ANALISIS": "Análisis de Cartera (ARP y FNS)",
            "R91": "Reportes R91 (ARP y FS)",
            "VENCIMIENTOS": "Vencimientos (ARP y FNS)",
            "R03": "Reportes R03 (Codeudores ARP y FNS)",
            "SC04": "Desembolsos Arpesod (SC04)",
            "CRTMPCONSULTA1": "Reporte de ventas CRTMPCONSULTA1",
            "FNZ003": "Saldos FNZ003",
            "MATRIZ_CARTERA": "Matriz de Cartera",
            "METAS_FRANJAS": "Metas por Franjas",
            "ASESORES": "Asesores Activos",
            "FNZ001": "Desembolsos Finansueños (FNZ001)"
        }

        # --- Crear dinámicamente los campos de carga de archivos ---
        for key, desc in archivos_requeridos.items():
            frame_archivo = ttk.Frame(scrollable_frame, padding=5, style='Card.TFrame')
            frame_archivo.pack(fill=tk.X, expand=True, padx=20, pady=5)

            label = ttk.Label(frame_archivo, text=f"{desc}:", style='Desc.TLabel')
            label.pack(side=tk.LEFT, padx=5)

            ruta_label = ttk.Label(frame_archivo, text="No seleccionado", relief="sunken", width=40, anchor="w", padding=5)
            ruta_label.pack(side=tk.LEFT, padx=5, expand=True, fill=tk.X)
            self.rutas_labels[key] = ruta_label

            boton = ttk.Button(frame_archivo, text="Seleccionar...", command=lambda k=key: self.controller.seleccionar_archivo(k))
            boton.pack(side=tk.LEFT, padx=5)

        # --- Filtro de Fechas ---
        date_filter_frame = ttk.LabelFrame(scrollable_frame, text=" Filtro por Fecha (Opcional) ", padding="10")
        date_filter_frame.pack(fill=tk.X, padx=20, pady=(20, 10))

        start_date_label = ttk.Label(date_filter_frame, text="Fecha de Inicio (dd/mm/yyyy):")
        start_date_label.grid(row=0, column=0, padx=5, pady=5, sticky="w")
        self.start_date_entry = ttk.Entry(date_filter_frame, width=20)
        self.start_date_entry.grid(row=0, column=1, padx=5, pady=5, sticky="ew")

        end_date_label = ttk.Label(date_filter_frame, text="Fecha de Fin (dd/mm/yyyy):")
        end_date_label.grid(row=0, column=2, padx=15, pady=5, sticky="w")
        self.end_date_entry = ttk.Entry(date_filter_frame, width=20)
        self.end_date_entry.grid(row=0, column=3, padx=5, pady=5, sticky="ew")
        
        date_filter_frame.columnconfigure(1, weight=1)
        date_filter_frame.columnconfigure(3, weight=1)

        # --- Botón de Procesar y Estado ---
        action_frame = ttk.Frame(scrollable_frame, padding="10", style='Card.TFrame')
        action_frame.pack(fill=tk.X, pady=20, padx=20)
        
        self.procesar_button = ttk.Button(action_frame, text="▶ Procesar Base Mensual", command=self.controller.procesar_archivos, style='Accent.TButton')
        self.procesar_button.pack(pady=10)

        self.status_label = ttk.Label(action_frame, text="Esperando archivos...", style='Status.TLabel')
        self.status_label.pack(pady=10)

        self.progress_bar = ttk.Progressbar(action_frame, orient='horizontal', mode='determinate', length=400)
        self.progress_bar.pack(pady=5, fill=tk.X, expand=True)
        
    def volver_al_menu(self):
        """Llama al método del controlador principal para mostrar el menú."""
        self.main_window_controller.mostrar_vista("base_mensual_menu")

    def actualizar_ruta_label(self, tipo_archivo, display_text):
        """
        Actualiza la etiqueta que muestra el estado del archivo seleccionado
        aplicando el nuevo estilo 'Success.TLabel'.
        """
        if tipo_archivo in self.rutas_labels:
            label = self.rutas_labels[tipo_archivo]
            # En lugar de .config(background...), cambiamos el estilo del widget
            label.config(text=display_text, style='Success.TLabel')
            self.update_idletasks()
    
    def actualizar_estado(self, mensaje, progreso=None):
        """Actualiza el mensaje de estado y la barra de progreso."""
        self.status_label.config(text=mensaje)
        if progreso is not None:
            self.progress_bar.config(value=progreso)
        self.update_idletasks()

