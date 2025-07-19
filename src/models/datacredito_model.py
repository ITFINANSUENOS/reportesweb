import pandas as pd
from src.services.datacredito.dataprocessor_service import DataProcessorService

class DataCreditoModel:
    """Gestiona los datos y la lógica de negocio para el reporte de Datacredito."""
    def __init__(self):
        self.df = None
        self.colspecs = [
            (0, 1), (1, 12), (30, 75), (12, 30), (76, 84), (84, 92),
            (92, 94), (107, 109), (109, 110), (188, 199), (199, 210),
            (210, 221), (221, 232), (232, 243), (243, 246), (246, 249),
            (249, 252), (263, 271), (271, 279), (577, 597), (625, 685),
            (685, 745), (445, 457), (75, 76), (185, 188), (105, 106),
            (110, 118), (118, 120), (120, 128), (137, 138), (138, 146),
            (252, 255), (255, 263)
        ]
        self.names = [
            "TIPO DE IDENTIFICACION", "NUMERO DE IDENTIFICACION", "NOMBRE COMPLETO",
            "NUMERO DE LA CUENTA U OBLIGACION", "FECHA APERTURA", "FECHA VENCIMIENTO",
            "RESPONSABLE", "NOVEDAD", "ESTADO ORIGEN DE LA CUENTA", "VALOR INICIAL",
            "VALOR SALDO DEUDA", "VALOR DISPONIBLE", "V CUOTA MENSUAL",
            "VALOR SALDO MORA", "TOTAL CUOTAS", "CUOTAS CANCELADAS", "CUOTAS EN MORA",
            "FECHA LIMITE DE PAGO", "FECHA DE PAGO", "CIUDAD CORRESPONDENCIA",
            "DIRECCION DE CORRESPONDENCIA", "CORREO ELECTRONICO", "CELULAR",
            "SITUACION DEL TITULAR", "EDAD DE MORA", "FORMA DE PAGO",
            "FECHA ESTADO ORIGEN", "ESTADO DE LA CUENTA", "FECHA ESTADO DE LA CUENTA",
            "ADJETIVO", "FECHA DE ADJETIVO", "CLAUSULA DE PERMANENCIA", "FECHA CLAUSULA DE PERMANENCIA"
        ]

    def load_plano_file(self, file_path):
        """Carga el archivo plano inicial en un DataFrame."""
        print("Modelo: Cargando archivo plano...")
        self.df = pd.read_fwf(
            file_path, colspecs=self.colspecs, names=self.names, encoding='cp1252',
            skiprows=1, skipfooter=1, engine='python'
        )
        self.df['NUMERO DE IDENTIFICACION'] = self.df['NUMERO DE IDENTIFICACION'].astype(str).str.strip()
        print("Modelo: Archivo plano cargado.")

    def process_data(self, correcciones_path):
        """Orquesta el procesamiento de datos utilizando el servicio."""
        if self.df is None:
            raise ValueError("El DataFrame no ha sido cargado. Llama a 'load_plano_file' primero.")
        
        processor = DataProcessorService(self.df.copy(), correcciones_path)
        self.df = processor.run_all_transformations()

    def save_processed_file(self, output_path):
        """Guarda el DataFrame procesado en un archivo Excel."""
        if self.df is None:
            raise ValueError("No hay datos procesados para guardar.")
        
        print(f"Modelo: Guardando archivo procesado en {output_path}")
        self.df.to_excel(output_path, index=False)
        print("Modelo: Archivo guardado con éxito.")