import pandas as pd
import os
import sys # Para usar flush=True en los print
import sqlite3

# --- ¡CAMBIO IMPORTANTE! ---
# Asumimos que la ruta a 'dataprocessor_service' es esta
# (El nombre del archivo que me diste en el chat)
from src.services.datacredito.dataprocessor_service import FinansuenosDataProcessorService
# (Asumo que también tienes uno de Arpesod, si no, puedes borrar esta línea)
# from src.services.centrales.arpesod.datacredito_service import ArpesodDataProcessorService

class DataCreditoModel:
    """
    Gestiona los datos y la lógica de negocio para Datacredito.
    OPTIMIZADO (Opción C - SQLite): Procesa los archivos en trozos (chunks) 
    y usa una BD SQLite para las correcciones.
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
        # ¡¡¡IMPORTANTE!!!
        # El Dockerfile copia la BD a /app/corrections.db
        # Esta ruta es dentro del contenedor Docker
        self.db_path = "/app/corrections.db" 

    def process_files_in_chunks(self, plano_path: str, correcciones_path: str, empresa_actual: str, output_path: str):
        """
        Esta es la NUEVA función principal optimizada.
        Procesa el archivo plano en trozos (chunks) y consulta SQLite.
        """
        print(f"MODEL: Iniciando procesamiento optimizado para {empresa_actual}", flush=True)

        # 1. Cargar el "processor" que consulta la BD SQLite
        # Ya no le pasamos 'correcciones_path', le pasamos la RUTA A LA BD
        print(f"MODEL: Cargando 'processor' (conectando a SQLite en {self.db_path})...", flush=True)
        
        if not os.path.exists(self.db_path):
            print(f"MODEL_ERROR: ¡No se encontró el archivo de base de datos 'corrections.db' en /app/!", flush=True)
            print("MODEL_ERROR: Asegúrate de haber ejecutado 'build_database.py' y que 'Dockerfile' lo esté copiando.", flush=True)
            raise FileNotFoundError(self.db_path)
            
        if empresa_actual == "arpesod":
            # (Asumimos que ArpesodDataProcessorService también usa SQLite ahora)
            # processor = ArpesodDataProcessorService(self.db_path)
            # Por ahora, lanzamos un error si no es finansueños
            raise ValueError(f"El procesador Arpesod (SQLite) no está implementado.")
        elif empresa_actual == "finansueños":
            processor = FinansuenosDataProcessorService(self.db_path)
        else:
            raise ValueError(f"Tipo de empresa no válido: {empresa_actual}")
        
        print(f"MODEL: 'processor' de SQLite cargado.", flush=True)

        # 2. Procesar el archivo plano (13MB) en trozos (chunks)
        print(f"MODEL: Iniciando procesamiento en chunks para {plano_path}...", flush=True)
        
        chunk_size = 50000  # Procesa 50,000 filas a la vez
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            
            try:
                # --- ¡¡¡AQUÍ ESTÁ LA CORRECCIÓN!!! ---
                # Hemos eliminado 'skipfooter=1'
                iterador_de_chunks = pd.read_fwf(
                    plano_path, colspecs=self.colspecs, names=self.names, encoding='cp1252',
                    skiprows=1,
                    # skipfooter=1, <-- ¡ESTE ES EL BUG! Lo quitamos.
                    engine='python',
                    chunksize=chunk_size
                )
            except Exception as e:
                print(f"MODEL_ERROR: No se pudo abrir el archivo plano {plano_path}. Error: {e}", flush=True)
                raise e
                
            header = True # Para guardar el header solo en el primer chunk
            found_chunks = False
            
            for i, chunk_df in enumerate(iterador_de_chunks):
                found_chunks = True
                print(f"MODEL: Procesando chunk #{i}...", flush=True)
                chunk_df['NUMERO DE IDENTIFICACION'] = chunk_df['NUMERO DE IDENTIFICACION'].astype(str).str.strip()
                
                # ¡Esta es la llamada clave!
                # El 'processor' ahora recibe el chunk y hace queries SQL
                try:
                    chunk_procesado = processor.run_all_transformations(chunk_df)
                except Exception as e:
                    print(f"MODEL_ERROR: Falló 'run_all_transformations' en el chunk #{i}. Error: {e}", flush=True)
                    # Opcional: ¿continuar con el siguiente chunk?
                    # Por ahora, relanzamos el error.
                    raise e
                
                # Guarda el chunk procesado en la misma hoja de Excel
                chunk_procesado.to_excel(writer, sheet_name='Reporte', index=False, header=header)
                header = False # Ya no escribimos el header
                
                print(f"MODEL: Chunk #{i} guardado.", flush=True)
            
            if not found_chunks:
                # Arregla el bug de 'At least one sheet must be visible'
                print("MODEL_WARN: No se encontraron chunks. Creando archivo Excel vacío.", flush=True)
                pd.DataFrame().to_excel(writer, sheet_name='Reporte', index=False)


        print(f"MODEL: Archivo final guardado en {output_path}.", flush=True)