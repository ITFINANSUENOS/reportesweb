import tkinter as tk
from tkinter import ttk
from pathlib import Path
from tkinter.font import Font

class BaseMensualView(tk.Toplevel):
    """
    Vista para seleccionar todos los archivos necesarios para generar la base mensual.
    """
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.parent = parent
        self.rutas_labels = {}  # Diccionario para guardar las etiquetas de las rutas

        # Configuración de la ventana
        self.title("Generar Base Mensual")
        self.geometry("800x650") # Aumentamos un poco la altura
        self.configure(bg="#F0F0F0")

        # --- Frame principal con scroll ---
        main_frame = ttk.Frame(self, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        canvas = tk.Canvas(main_frame, bg="#F0F0F0", highlightthickness=0)
        scrollbar = ttk.Scrollbar(main_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas, style='Card.TFrame')

        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        style = ttk.Style()
        style.configure('Card.TFrame', background="#FFFFFF")

        # Título
        title_font = Font(family="Helvetica", size=14, weight="bold")
        title_label = ttk.Label(scrollable_frame, text="Cargar Archivos para el Reporte", font=title_font, background="#FFFFFF")
        title_label.pack(pady=20, padx=20)

        # Definir los archivos a solicitar
        archivos_requeridos = {
            "ANALISIS": "Análisis de Cartera (ARP y FNS)",
            "R91": "Reportes R91 (ARP y FS)",
            "VENCIMIENTOS": "Vencimientos (ARP y FNS)",
            "R03": "Reportes R03 (Codeudores ARP y FNS)",
            "SC04": "Archivo SC04",
            "CRTMPCONSULTA1": "Consulta CRTMPCONSULTA1",
            "FNZ003": "Saldos FNZ003",
            "MATRIZ_CARTERA": "Matriz de Cartera",
            "METAS_FRANJAS": "Metas por Franjas",
            "ASESORES": "Asesores Activos",
            "DESEMBOLSOS_FINANSUEÑOS": "Desembolsos Finansueños"
        }

        # --- Crear dinámicamente los campos de carga de archivos ---
        for key, desc in archivos_requeridos.items():
            frame_archivo = ttk.Frame(scrollable_frame, padding=5, style='Card.TFrame')
            frame_archivo.pack(fill=tk.X, expand=True, padx=20, pady=5)

            label = ttk.Label(frame_archivo, text=f"{desc}:", width=35, background="#FFFFFF")
            label.pack(side=tk.LEFT, padx=5)

            ruta_label = ttk.Label(frame_archivo, text="No seleccionado", relief="sunken", width=40, anchor="w", padding=5)
            ruta_label.pack(side=tk.LEFT, padx=5, expand=True, fill=tk.X)
            self.rutas_labels[key] = ruta_label

            boton = ttk.Button(frame_archivo, text="Seleccionar...", command=lambda k=key: self.controller.seleccionar_archivo(k))
            boton.pack(side=tk.LEFT, padx=5)

        # --- INICIO: NUEVA SECCIÓN PARA FILTRO DE FECHAS ---
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
        # --- FIN DE LA NUEVA SECCIÓN ---

        # --- Botón de Procesar y Estado ---
        action_frame = ttk.Frame(scrollable_frame, padding="10", style='Card.TFrame')
        action_frame.pack(fill=tk.X, pady=20, padx=20)
        
        self.procesar_button = ttk.Button(action_frame, text="▶ Procesar Base Mensual", command=self.controller.procesar_archivos, style='Accent.TButton')
        self.procesar_button.pack(pady=10)

        self.status_label = ttk.Label(action_frame, text="Esperando archivos...", background="#FFFFFF")
        self.status_label.pack(pady=10)

        self.progress_bar = ttk.Progressbar(action_frame, orient='horizontal', mode='determinate', length=400)
        self.progress_bar.pack(pady=5, fill=tk.X, expand=True)

    def actualizar_ruta_label(self, tipo_archivo, display_text):
        """
        Actualiza la etiqueta que muestra el estado del archivo seleccionado
        y cambia su color para confirmar visualmente la carga.
        """
        if tipo_archivo in self.rutas_labels:
            label = self.rutas_labels[tipo_archivo]
            label.config(
                text=display_text, 
                background="#D4EDDA",  # Un fondo verde claro para indicar éxito
                foreground="#155724",  # Texto oscuro para buena legibilidad
                relief="flat"        # Borde plano
            )
            self.update_idletasks()
    
    def actualizar_estado(self, mensaje, progreso=None):
        """Actualiza el mensaje de estado y la barra de progreso."""
        self.status_label.config(text=mensaje)
        if progreso is not None:
            self.progress_bar.config(value=progreso)
        self.update_idletasks()
