import pandas as pd
import os
import sys # Para usar flush=True en los print

# ¡¡¡IMPORTANTE!!!
# Asumimos que estos servicios (que no tengo) son el cuello de botella.
# Específicamente, el que carga el Excel de 84MB.
# Este código optimiza la lectura del TXT (13MB), pero el 'processor'
# sigue cargando los 84MB de golpe.
from src.services.datacredito.dataprocessor_service import FinansuenosDataProcessorService
from src.services.centrales.arpesod.datacredito_service import ArpesodDataProcessorService

class DataCreditoModel:
    """
    Gestiona los datos y la lógica de negocio para Datacredito.
    OPTIMIZADO: Procesa los archivos en trozos (chunks) para 
    funcionar en entornos con poca RAM (como 2GB).
    """
    
    def __init__(self):
        # Ya no guardamos self.df para ahorrar memoria
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

    def process_files_in_chunks(self, plano_path: str, correcciones_path: str, empresa_actual: str, output_path: str):
        """
        Esta es la NUEVA función principal optimizada.
        Procesa el archivo plano en trozos (chunks) para no agotar la RAM.
        """
        print(f"MODEL: Iniciando procesamiento optimizado para {empresa_actual}", flush=True)

        # 1. Cargar el archivo de correcciones (84MB)
        # ¡¡¡ESTE SIGUE SIENDO EL RIESGO!!!
        # Tu FinansuenosDataProcessorService probablemente carga el Excel de 84MB
        # en su constructor. Esto es lo que consume los 6GB de RAM.
        # ¡DEBES OPTIMIZAR ESE SERVICIO TAMBIÉN!
        print(f"MODEL: Cargando 'processor' (esto puede cargar 84MB de Excel)...", flush=True)
        if empresa_actual == "arpesod":
            processor = ArpesodDataProcessorService(None, correcciones_path)
        elif empresa_actual == "finansueños":
            processor = FinansuenosDataProcessorService(None, correcciones_path)
        else:
            raise ValueError(f"Tipo de empresa no válido: {empresa_actual}")
        
        print(f"MODEL: 'processor' cargado. {empresa_actual} (84MB) está en memoria.", flush=True)

        # 2. Procesar el archivo plano (13MB) en trozos (chunks)
        print(f"MODEL: Iniciando procesamiento en chunks para {plano_path}...", flush=True)
        
        chunk_size = 50000  # Procesa 50,000 filas a la vez (puedes ajustar esto)
        
        # Creamos un 'ExcelWriter' para guardar los chunks procesados uno por uno
        # Esto evita tener el resultado final en memoria
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            
            # Usamos un iterador de chunks para leer el TXT (plano)
            iterador_de_chunks = pd.read_fwf(
                plano_path, colspecs=self.colspecs, names=self.names, encoding='cp1252',
                skiprows=1, skipfooter=1, engine='python',
                chunksize=chunk_size  # ¡LA MAGIA ESTÁ AQUÍ!
            )
            
            header = True # Para guardar el header solo en el primer chunk
            
            for i, chunk_df in enumerate(iterador_de_chunks):
                print(f"MODEL: Procesando chunk #{i}...", flush=True)
                chunk_df['NUMERO DE IDENTIFICACION'] = chunk_df['NUMERO DE IDENTIFICACION'].astype(str).str.strip()
                
                # ¡DEBES MODIFICAR TU 'processor'!
                # Tu 'processor.run_all_transformations()' debe ser modificado
                # para que acepte un 'chunk_df' como argumento,
                # en lugar de usar 'self.df'.
                
                # ---- INICIO DE CÓDIGO ASUMIDO ----
                # Asumo que tu 'processor' tiene 'self.df'
                # Lo asignamos, procesamos y luego lo borramos.
                processor.df = chunk_df 
                chunk_procesado = processor.run_all_transformations() 
                # ---- FIN DE CÓDIGO ASUMIDO ----
                
                # Guarda el chunk procesado en la misma hoja de Excel
                chunk_procesado.to_excel(writer, sheet_name='Reporte', index=False, header=header)
                header = False # Ya no escribimos el header
                
                print(f"MODEL: Chunk #{i} guardado.", flush=True)

        print(f"MODEL: Archivo final guardado en {output_path}.", flush=True)