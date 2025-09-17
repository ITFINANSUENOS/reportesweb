import pandas as pd

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
            print("Modelo: Cargando archivo plano como texto...")
            self.df = pd.read_fwf(
                file_path, colspecs=self.colspecs, names=self.names, dtype=str,
                encoding='cp1252', skiprows=1, skipfooter=1, engine='python'
            )
            self.df.replace(['nan', 'NaN'], '', inplace=True)
            self.df.rename(columns={'Nº_identificacion': 'NUMERO DE IDENTIFICACION'}, inplace=True)
            print(f"Modelo: Archivo plano cargado exitosamente.", self.df.head())
            
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
                return True
            except Exception as e:
                print(f"❌ ERROR al guardar el archivo de Excel: {e}")
                return False
        else:
            print("⚠️ Advertencia: No hay datos para guardar.")
            return False
