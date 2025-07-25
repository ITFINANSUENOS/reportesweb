# src/features/cifin/cifin_controller.py
from tkinter import messagebox, filedialog
from src.models.cifin_model import CifinModel
from src.views.cifin_view import CifinView
from src.services.cifin.cifin_service import DataProcessorService

class CifinController:
    def __init__(self):
        self.model = CifinModel()
        self.view = None
        self.column_map = {
            'id_number': 'NUMERO DE IDENTIFICACION',
            'id_type': 'tipo_identificacion',
            'full_name': 'nombre_tercero',
            'address': 'direccion_casa',
            'email': 'correo_electronico',
            'phone': 'numero_celular',
            'home_phone': 'telefono_casa',
            'company_phone': 'telefono_empresa',
            'account_number': 'numero_obligacion',
            'initial_value': 'valor_inicial',
            'payment_date': 'fecha_pago',
            'open_date': 'fecha_inicio',
            'due_date': 'fecha_terminacion',
            'city': 'ciudad_casa',
            'department': 'departamento_casa',
            'balance_due': 'valor_saldo',
            'available_value': 'cargo_fijo',
            'monthly_fee': 'valor_cuota',
            'arrears_value': 'valor_mora',
            'arrears_age': 'edad_mora', 
            'periodicity': 'periodicidad',
            'actual_value_paid':'valor_real_pagado'
        }

    def open_cifin_window(self, parent):
        # --- CORRECCIÓN AQUÍ ---
        # Comprueba la existencia de la ventana a través de .top
        if self.view is None or not self.view.top.winfo_exists():
            self.view = CifinView(parent, self)
            # Llama a grab_set() sobre .top
            self.view.top.grab_set()
        else:
            # Llama a lift() sobre .top
            self.view.top.lift()
            
    def run_processing(self, view, txt_path, corrections_path):
        try:
            # 1. Iniciar el proceso y actualizar la vista
            view.update_status("Paso 1/4: Cargando archivo plano...")
            df_cargado = self.model.load_plano_file(txt_path)
            if df_cargado is None:
                raise ValueError("No se pudo cargar el archivo plano.")

            view.update_status("Paso 2/4: Creando servicio de procesamiento...")
            procesador = DataProcessorService(df_cargado, corrections_path, self.column_map)
            
            view.update_status("Paso 3/4: Ejecutando transformaciones...")
            df_transformado = procesador.run_all_transformations()
            
            view.update_status("Paso 4/4: Transformación completa. Seleccione dónde guardar.")
            
            # --- LÓGICA DE GUARDADO AL FINAL ---
            # Pide al usuario la ruta de guardado DESPUÉS de procesar todo.
            output_path = filedialog.asksaveasfilename(
                title="Guardar reporte como",
                filetypes=[("Archivos Excel", "*.xlsx")],
                defaultextension=".xlsx"
            )

            # Si el usuario selecciona una ruta (no cancela)
            if output_path:
                self.model.df = df_transformado
                if self.model.guardar_en_excel(output_path):
                    view.update_status("¡Proceso completado exitosamente!")
                    messagebox.showinfo("Éxito", f"El reporte ha sido generado en:\n{output_path}")
                else:
                    raise ValueError("No se pudo guardar el archivo Excel.")
            else:
                view.update_status("Guardado cancelado por el usuario.")

        except Exception as e:
            view.update_status(f"Error: {e}")
            messagebox.showerror("Error en el Proceso", f"Ocurrió un error:\n{e}")
