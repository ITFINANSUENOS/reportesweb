import pandas as pd
from src.services.cifin.cifin_service import DataProcessorService # <-- Importa la clase

# Tu clase CifinModel se mantiene igual
class CifinModel:
    def __init__(self):
        self.df = None
        self.colspecs = [
            (1, 3), (3, 18), (18, 78), (80, 88), (88, 108), (108, 114),
            (114, 115), (119, 121), (121, 123), (123, 125), (125, 133),
            (133, 141), (141, 149), (149, 157), (157, 165), (165, 173),
            (173, 175), (175, 177), (177, 179), (182, 185), (185, 188),
            (188, 191), (191, 203), (203, 215), (215, 227), (227, 239),
            (239, 251), (251, 254), (257, 260), (260, 263), (263, 265),
            (265, 268), (289, 291), (291, 293), (293, 296), (298, 302),
            (304, 306), (306, 312), (329, 389), (389, 409), (409, 415),
            (415, 435), (435, 438), (438, 458), (458, 518), (518, 578),
            (578, 598), (598, 604), (604, 624), (624, 627), (627, 647),
            (797, 857), (857, 917), (917, 929)
        ]
        self.names = [
            "tipo_identificacion", "Nº_identificacion", "nombre_tercero", "fecha_limite_pago", "numero_obligacion",
            "codigo_sucursal", "calidad", "estado_obligacion", "edad_mora", "años_mora", "fecha_corte", "fecha_inicio",
            "fecha_terminacion", "fecha_exigibilidad", "fecha_prescripcion", "fecha_pago", "modo_extincion", "tipo_pago",
            "periodicidad", "cuotas_pagadas", "cuotas_pactadas", "cuotas_mora", "valor_inicial", "valor_mora",
            "valor_saldo", "valor_cuota", "cargo_fijo", "linea_credito", "tipo_contrato", "estado_contrato", "vigencia_contrato",
            "numero_meses_contrato", "obligacion_reestructurada", "naturaleza_reestructuracion", "numero_reestructuraciones",
            "Nº_cheques_devueltos", "plazo", "dias_cartera", "direccion_casa", "telefono_casa", "codigo_ciudad_casa",
            "ciudad_casa", "codigo_departamento", "departamento_casa", "nombre_empresa", "direccion_empresa", "telefono_empresa",
            "codigo_ciudad_empresa", "ciuda_empresa", "codigo_departamento_empresa", "departamento_empresa", "correo_electronico",
            "numero_celular", "valor_real_pagado"
        ]

    def load_plano_file(self, file_path):
        try:
            print("Modelo: Cargando archivo plano...")
            self.df = pd.read_fwf(
                file_path, colspecs=self.colspecs, names=self.names,dtype=str,
                encoding='cp1252', skiprows=1, skipfooter=1, engine='python'
            )
            
            self.df.replace(['nan', 'NaN'], '', inplace=True)
            # Normalizamos el nombre de la columna de identificación para que coincida con el mapa
            self.df.rename(columns={'Nº_identificacion': 'NUMERO DE IDENTIFICACION'}, inplace=True)
            
            print("Modelo: Archivo plano cargado exitosamente.")
            return self.df
        except Exception as e:
            print(f"❌ ERROR al cargar el archivo: {e}")
            return None

    def guardar_en_excel(self, output_path):
        if self.df is not None and not self.df.empty:
            try:
                print(f"Modelo: Guardando archivo en {output_path}...")
                self.df.to_excel(output_path, index=False)
                print(f"✅ ¡Éxito! Archivo guardado correctamente.")
            except Exception as e:
                print(f"❌ ERROR al guardar el archivo de Excel: {e}")
        else:
            print("⚠️ Advertencia: No hay datos para guardar.")

# --- CONFIGURACIÓN PRINCIPAL ---
if __name__ == "__main__":
    # 1. Definir rutas
    ruta_txt_entrada = '/home/balvin/dev/CIFIN MARZO FS.TXT'
    ruta_excel_correcciones = '/home/balvin/dev/Cédulas a revisar.xlsx' 
    ruta_excel_salida = '/home/balvin/dev/Resultado_Cifin_Transformado.xlsx'
    
    # ruta_txt_entrada = 'c:/Users/usuario\Desktop/Reporte LV/cifin/CIFIN MARZO FS.TXT'
    # ruta_excel_correcciones = 'c:/Users/usuario/Desktop/Reporte LV/datacredito/Cédulas a revisar.xlsx' 
    # ruta_excel_salida = 'c:/Users/usuario/Desktop/Reporte LV/cifin/Resultado_Cifin_Transformado.xlsx'

    # 2. Definir el MAPA DE COLUMNAS para CifinModel
    # Clave: Nombre genérico que usa el servicio. Valor: Nombre real de la columna en tu DataFrame.
    CIFIN_COLUMN_MAP = {
        'id_number': 'NUMERO DE IDENTIFICACION', # Renombramos 'Nº_identificacion' a este nombre al cargar
        'id_type': 'tipo_identificacion',
        'full_name': 'nombre_tercero',
        'address': 'direccion_casa',
        'email': 'correo_electronico',
        'phone': 'numero_celular',
        'home_phone':'telefono_casa',
        'company_phone':'telefono_empresa',
        'account_number': 'numero_obligacion',
        'initial_value': 'valor_inicial',
        'payment_date': 'fecha_pago',
        'open_date': 'fecha_inicio',
        'due_date': 'fecha_terminacion',
        'city': 'ciudad_casa',
        'department': 'departamento_casa',
        'balance_due': 'valor_saldo',
        'available_value': 'cargo_fijo', # Asumiendo que este es el campo correcto
        'monthly_fee': 'valor_cuota',
        'arrears_value': 'valor_mora',
        'arrears_age': 'edad_mora', 
        'periodicity': 'periodicidad',
        'actual_value_paid':'valor_real_pagado'
        # ... completa con las demás columnas que use el servicio si es necesario
    }

    # 3. Proceso de ejecución
    print("--- INICIANDO PROCESO DE TRANSFORMACIÓN DE DATOS ---")
    
    # Cargar los datos usando CifinModel
    modelo = CifinModel()
    df_cargado = modelo.load_plano_file(ruta_txt_entrada)

    if df_cargado is not None:
        # Crear una instancia del servicio, pasándole el DataFrame y el mapa de columnas
        procesador = DataProcessorService(df_cargado, ruta_excel_correcciones, CIFIN_COLUMN_MAP)
        
        # Ejecutar todas las transformaciones
        df_transformado = procesador.run_all_transformations()
        
        # Guardar el resultado
        modelo.df = df_transformado # Actualizamos el df del modelo con el transformado
        modelo.guardar_en_excel(ruta_excel_salida)
        
    print("--- PROCESO FINALIZADO ---")