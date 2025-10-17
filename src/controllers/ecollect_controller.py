from tkinter import filedialog
import pandas as pd
from src.services.ecollect.ecollect_service import EcollectService
from src.services.ecollect.plano_service import PlanoService
from src.services.ecollect.usuarios_service import UsuariosService
from src.models.ecollect_model import configuracion
class EcollectController:
    def __init__(self):
        self.view = None
        self.ecollect_service = EcollectService(configuracion)
        self.plano_service = PlanoService()
        self.usuarios_service = UsuariosService(configuracion)
        self.rutas_archivos = {}

    def set_view(self, view):
        self.view = view

    def seleccionar_archivo(self, key: str, multiple: bool):
        """
        Este método no necesita cambios. Funciona perfectamente con la nueva vista.
        Recibirá las nuevas 'keys' ("PROCESO_VENCIMIENTOS", "PROCESO_CONSULTA").
        """
        if multiple:
            paths = filedialog.askopenfilenames(title=f"Seleccione archivo(s) para {key}")
            if paths:
                self.rutas_archivos[key] = list(paths)
                display_text = f"{len(paths)} archivo(s) seleccionado(s)"
                self.view.actualizar_ruta_label(key, display_text)
        else:
            path = filedialog.askopenfilename(title=f"Seleccione un archivo para {key}")
            if path:
                self.rutas_archivos[key] = path
                display_text = path.split('/')[-1] # Muestra solo el nombre del archivo
                self.view.actualizar_ruta_label(key, display_text)

    # --- MÉTODO PRINCIPAL UNIFICADO ---
    def iniciar_proceso_completo(self):
        """Orquesta la ejecución secuencial de la generación de ambos planos."""
        self.view.main_window.update_status("Iniciando proceso...")
        vencimientos_paths = self.rutas_archivos.get("PROCESO_VENCIMIENTOS")
        consulta_path = self.rutas_archivos.get("PROCESO_CONSULTA")

        if not vencimientos_paths or not consulta_path:
            self.view.main_window.update_status("Error: Por favor, seleccione todos los archivos requeridos.")
            return

        try:
            # --- PASO 1: PROCESAR Y GUARDAR PLANO DE CARTERA ---
            self.view.main_window.update_status("Paso 1/2: Procesando plano de cartera...")
            df_cartera = self.ecollect_service.process_vencimientos(vencimientos_paths)
            if df_cartera is None or df_cartera.empty:
                self.view.main_window.update_status("Error: No se encontraron datos para el plano de cartera.")
                return

            fecha_hoy_cartera = pd.Timestamp.now().strftime('%Y%m%d')
            nombre_sugerido_cartera = f"carga_cartera_{fecha_hoy_cartera}_10791.txt"
            save_path_cartera = filedialog.asksaveasfilename(
                title="Guardar Plano de Cartera como...",
                initialfile=nombre_sugerido_cartera,
                defaultextension=".txt",
                filetypes=[("Archivos de Texto", "*.txt"), ("Todos los archivos", "*.*")]
            )
            if not save_path_cartera:
                self.view.main_window.update_status("Proceso cancelado por el usuario en el Paso 1.")
                return

            success_cartera = self.plano_service.generar_archivo_plano(df_cartera, save_path_cartera)
            if not success_cartera:
                self.view.main_window.update_status("Error al guardar el archivo de cartera.")
                return
            
            self.view.main_window.update_status(f"Paso 1/2 completado: Plano de cartera guardado.")

            # --- PASO 2: PROCESAR Y GUARDAR INFORME DE USUARIOS ---
            self.view.main_window.update_status("Paso 2/2: Cruzando datos para el informe de usuarios...")
            df_usuarios = self.usuarios_service.crear_dataframe_usuarios(
                list(vencimientos_paths), consulta_path
            )
            if df_usuarios is None or df_usuarios.empty:
                self.view.main_window.update_status("Error: No se pudo generar el informe de usuarios.")
                return
            
            # --- Lógica de guardado para el Informe de Usuarios (ACTUALIZADA) ---
            fecha_hoy_usuarios = pd.Timestamp.now().strftime('%Y%m%d')
            nombre_sugerido_usuarios = f"USU10791_{fecha_hoy_usuarios}.txt"
            save_path_usuarios = filedialog.asksaveasfilename(
                title="Guardar Plano de Usuarios como...",
                initialfile=nombre_sugerido_usuarios,
                defaultextension=".txt",
                filetypes=[("Archivos de Texto", "*.txt"), ("CSV", "*.csv"), ("Todos los archivos", "*.*")]
            )
            if not save_path_usuarios:
                self.view.main_window.update_status("Proceso cancelado por el usuario en el Paso 2.")
                return
            
            # --- LLAMADA AL NUEVO MÉTODO DEL SERVICIO ---
            success_usuarios = self.plano_service.generar_plano_usuarios(df_usuarios, save_path_usuarios)
            
            if success_usuarios:
                self.view.main_window.update_status("¡Proceso completado! Ambos archivos han sido generados.")
            else:
                self.view.main_window.update_status("Error al generar el plano de usuarios. Revise la consola.")

        except Exception as e:
            error_msg = f"Error durante el procesamiento: {e}"
            self.view.main_window.update_status(error_msg)
            print(f"Error detallado: {e}")