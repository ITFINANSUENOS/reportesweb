from tkinter import filedialog
import pandas as pd
from src.services.ecollect.ecollect_service import EcollectService
from src.services.ecollect.plano_service import PlanoService
from src.services.ecollect.usuarios_service import UsuariosService
from src.services.ecollect.colaboradores_service import ColaboradoresService 
from src.models.ecollect_model import configuracion

class EcollectController:
    def __init__(self):
        self.view = None
        self.ecollect_service = EcollectService(configuracion)
        self.plano_service = PlanoService()
        self.usuarios_service = UsuariosService(configuracion)
        self.colaboradores_service = ColaboradoresService(configuracion)
        self.rutas_archivos = {}

    def set_view(self, view):
        self.view = view

    def seleccionar_archivo(self, key: str, multiple: bool):
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

    def iniciar_proceso_completo(self):
        """Orquesta la ejecución para CLIENTES. (Sin cambios)"""
        self.view.main_window.update_status("Iniciando proceso Clientes...")
        vencimientos_paths = self.rutas_archivos.get("PROCESO_VENCIMIENTOS")
        consulta_path = self.rutas_archivos.get("PROCESO_CONSULTA")

        if not vencimientos_paths or not consulta_path:
            self.view.main_window.update_status("Error (Clientes): Por favor, seleccione todos los archivos requeridos.")
            return
        try:
            self.view.main_window.update_status("Paso 1/2 (Clientes): Procesando plano de cartera...")
            df_cartera = self.ecollect_service.process_vencimientos(vencimientos_paths)
            if df_cartera is None or df_cartera.empty:
                self.view.main_window.update_status("Error (Clientes): No se encontraron datos para el plano de cartera.")
                return

            fecha_hoy_cartera = pd.Timestamp.now().strftime('%Y%m%d')
            nombre_sugerido_cartera = f"carga_cartera_{fecha_hoy_cartera}_10791 CLIENTES .txt"
            save_path_cartera = filedialog.asksaveasfilename(
                title="Guardar Plano de Cartera (Clientes) como...",
                initialfile=nombre_sugerido_cartera,
                defaultextension=".txt",
                filetypes=[("Archivos de Texto", "*.txt"), ("Todos los archivos", "*.*")]
            )
            if not save_path_cartera:
                self.view.main_window.update_status("Proceso Clientes cancelado (Paso 1).")
                return
            success_cartera = self.plano_service.generar_archivo_plano(df_cartera, save_path_cartera)
            if not success_cartera:
                self.view.main_window.update_status("Error (Clientes) al guardar el archivo de cartera.")
                return
            self.view.main_window.update_status(f"Paso 1/2 (Clientes) completado: Plano de cartera guardado.")

            self.view.main_window.update_status("Paso 2/2 (Clientes): Cruzando datos para el informe de usuarios...")
            df_usuarios = self.usuarios_service.crear_dataframe_usuarios(
                list(vencimientos_paths), consulta_path
            )
            if df_usuarios is None or df_usuarios.empty:
                self.view.main_window.update_status("Error (Clientes): No se pudo generar el informe de usuarios.")
                return
            fecha_hoy_usuarios = pd.Timestamp.now().strftime('%Y%m%d')
            nombre_sugerido_usuarios = f"USU10791_{fecha_hoy_usuarios} CLIENTES.txt"
            save_path_usuarios = filedialog.asksaveasfilename(
                title="Guardar Plano de Usuarios (Clientes) como...",
                initialfile=nombre_sugerido_usuarios,
                defaultextension=".txt",
                filetypes=[("Archivos de Texto", "*.txt"), ("CSV", "*.csv"), ("Todos los archivos", "*.*")]
            )
            if not save_path_usuarios:
                self.view.main_window.update_status("Proceso Clientes cancelado (Paso 2).")
                return
            success_usuarios = self.plano_service.generar_plano_usuarios(df_usuarios, save_path_usuarios)
            if success_usuarios:
                self.view.main_window.update_status("¡Proceso Clientes completado! Archivos generados.")
            else:
                self.view.main_window.update_status("Error (Clientes) al generar el plano de usuarios.")
        except Exception as e:
            error_msg = f"Error (Clientes) durante el procesamiento: {e}"
            self.view.main_window.update_status(error_msg)
            print(f"Error detallado (Clientes): {e}")

    # --- ¡MÉTODO 2 MODIFICADO: Proceso de COLABORADORES! ---
    def iniciar_proceso_colaboradores(self):
        """
        Orquesta la ejecución para COLABORADORES llamando al nuevo servicio.
        """
        self.view.main_window.update_status("Iniciando proceso Colaboradores...")
        colaboradores_path = self.rutas_archivos.get("PROCESO_COLABORADORES")
        if not colaboradores_path:
            self.view.main_window.update_status("Error (Colaboradores): Por favor, seleccione el archivo de Colaboradores.")
            return
        try:
            # --- ¡CAMBIO 3: Llamar al servicio de Colaboradores! ---
            self.view.main_window.update_status("Paso 1/2 (Colaboradores): Procesando cartera...")
            df_cartera_colab = self.colaboradores_service.process_cartera(colaboradores_path)
            if df_cartera_colab is None or df_cartera_colab.empty:
                 self.view.main_window.update_status("Error (Colaboradores): No se encontraron datos en la hoja 'CARTERA' o hubo un error al leerla.")
                 return
            # --- FIN CAMBIO 3 ---
            # La lógica de guardado no cambia, ¡solo el DataFrame que le pasamos!
            fecha_hoy_cartera = pd.Timestamp.now().strftime('%Y%m%d')
            nombre_sugerido_cartera = f"carga_cartera_{fecha_hoy_cartera}_10791 COLAB.txt"
            save_path_cartera = filedialog.asksaveasfilename(
                title="Guardar Plano de Cartera (Colaboradores) como...",
                initialfile=nombre_sugerido_cartera,
                defaultextension=".txt",
                filetypes=[("Archivos de Texto", "*.txt"), ("Todos los archivos", "*.*")]
            )
            if not save_path_cartera:
                self.view.main_window.update_status("Proceso Colaboradores cancelado (Paso 1).")
                return

            # Reutilizamos el plano_service
            success_cartera = self.plano_service.generar_archivo_plano(df_cartera_colab, save_path_cartera)
            if not success_cartera:
                self.view.main_window.update_status("Error (Colaboradores) al guardar el archivo de cartera.")
                return
            self.view.main_window.update_status("Paso 1/2 (Colaboradores) completado: Plano de cartera guardado.")
            self.view.main_window.update_status("Paso 2/2 (Colaboradores): Procesando usuarios...")
            df_usuarios_colab = self.colaboradores_service.process_usuarios(colaboradores_path)

            if df_usuarios_colab is None or df_usuarios_colab.empty:
                 self.view.main_window.update_status("Error (Colaboradores): No se encontraron datos en la hoja 'USUARIOS' o hubo un error al leerla.")
                 return

            # La lógica de guardado no cambia
            fecha_hoy_usuarios = pd.Timestamp.now().strftime('%Y%m%d')
            nombre_sugerido_usuarios = f"USU10791_{fecha_hoy_usuarios} COLAB.txt"
            save_path_usuarios = filedialog.asksaveasfilename(
                title="Guardar Plano de Usuarios (Colaboradores) como...",
                initialfile=nombre_sugerido_usuarios,
                defaultextension=".txt",
                filetypes=[("Archivos de Texto", "*.txt"), ("CSV", "*.csv"), ("Todos los archivos", "*.*")]
            )
            if not save_path_usuarios:
                self.view.main_window.update_status("Proceso Colaboradores cancelado (Paso 2).")
                return
            
            # Reutilizamos el plano_service
            success_usuarios = self.plano_service.generar_plano_usuarios(df_usuarios_colab, save_path_usuarios)
            
            if success_usuarios:
                self.view.main_window.update_status("¡Proceso Colaboradores completado! Archivos generados.")
            else:
                self.view.main_window.update_status("Error (Colaboradores) al generar el plano de usuarios.")

        except Exception as e:
            error_msg = f"Error (Colaboradores) durante el procesamiento: {e}"
            self.view.main_window.update_status(error_msg)
            print(f"Error detallado (Colaboradores): {e}")