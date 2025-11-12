import pandas as pd
import sqlite3
import sys
import os
from collections import defaultdict

class FinansuenosDataProcessorService:
    """
    Clase responsable de todas las transformaciones de datos.
    OPTIMIZADA (Opción C - SQLite): Lee las correcciones desde una 
    base de datos SQLite ('corrections.db') para uso de RAM mínimo y alta velocidad.
    """
    def __init__(self, db_path):
        """
        En lugar de cargar 6GB de 'mapas' en RAM, solo guarda la ruta a la BD.
        """
        print(f"SERVICE (FINANSUEÑOS): Conectando a la base de datos: {db_path}", flush=True)
        self.db_path = db_path
        
        # Verificamos la conexión (la mantenemos simple, abrimos en cada método)
        if not os.path.exists(self.db_path):
             print(f"SERVICE_ERROR: ¡No se encontró el archivo de base de datos en {self.db_path}!", flush=True)
             raise FileNotFoundError(f"No se encontró el archivo de base de datos: {self.db_path}")
        print(f"SERVICE (FINANSUEÑOS): Conexión a SQLite lista.", flush=True)
            
        # Este mapa es el único que guardamos, ya que es pequeño
        self.mapa_vinc_cols_df = {'NOMBRE COMPLETO':'NOMBRE', 'DIRECCION DE CORRESPONDENCIA':'DIRECCI', 'CORREO ELECTRONICO':'VINEMAIL', 'CELULAR':'TELEFONO'}

    
    def run_all_transformations(self, chunk_df):
        """
        Ejecuta todos los pasos de limpieza en un 'chunk' (trozo) del DataFrame.
        Hace consultas SQL a la BD por cada chunk.
        """
        # print("Servicio: Ejecutando transformaciones en chunk...", flush=True)
        self.df = chunk_df # Asigna el chunk actual
        
        # Abrimos una conexión a la BD por cada chunk
        # SQLite es muy rápido para esto.
        conn = sqlite3.connect(self.db_path)
        
        try:
            # Llama a los métodos de transformación
            self._correct_data_from_db(conn) 
            self._update_data_from_db(conn)
        except Exception as e:
            print(f"SERVICE_ERROR: Falló la transformación del chunk. Error: {e}", flush=True)
            raise e
        finally:
            # Cerramos la conexión a la BD
            conn.close()
        
        # Estos métodos no necesitan la BD
        self._clean_and_validate_data()
        self._apply_final_formatting()
        
        # print("Servicio: Transformaciones de chunk completadas.", flush=True)
        return self.df

    def _correct_data_from_db(self, conn):
        """PASO 3: Realiza correcciones (LEYENDO DESDE SQLITE)"""
        # A. Corregir Cédulas
        # Creamos un mapa (diccionario) solo para las cédulas de ESTE CHUNK
        cedulas_en_chunk = tuple(self.df['NUMERO DE IDENTIFICACION'].unique())
        # ¡IMPORTANTE! El '?' es para seguridad (evitar SQL Injection)
        query_cedulas = f"SELECT CEDULA_MAL, CEDULA_CORRECTA FROM cedulas_map WHERE CEDULA_MAL IN ({','.join(['?']*len(cedulas_en_chunk))})"
        mapa_cedulas_chunk = pd.read_sql_query(query_cedulas, conn, params=cedulas_en_chunk).set_index('CEDULA_MAL')['CEDULA_CORRECTA'].to_dict()
        if mapa_cedulas_chunk:
            self.df['NUMERO DE IDENTIFICACION'] = self.df['NUMERO DE IDENTIFICACION'].replace(mapa_cedulas_chunk)

        # B. Actualizar campos 'CORREGIR'
        mascara = self.df['NOMBRE COMPLETO'].astype(str).str.strip().str.contains('CORREGIR', case=False, na=False)
        if mascara.any():
            ids_a_buscar = tuple(self.df.loc[mascara, 'NUMERO DE IDENTIFICACION'].unique())
            query_vinc = f"SELECT * FROM vinculado_map WHERE CODIGO IN ({','.join(['?']*len(ids_a_buscar))})"
            df_vinculado_chunk = pd.read_sql_query(query_vinc, conn, params=ids_a_buscar).set_index('CODIGO')
            
            for col_df, col_vinc in self.mapa_vinc_cols_df.items():
                if col_vinc in df_vinculado_chunk.columns:
                    # Usamos .get() para evitar errores si un código no se encuentra
                    valores_nuevos = self.df.loc[mascara, 'NUMERO DE IDENTIFICACION'].map(df_vinculado_chunk.get(col_vinc, {}))
                    self.df.loc[mascara, col_df] = valores_nuevos.combine_first(self.df.loc[mascara, col_df]) # No sobrescribir con NaN

        # C. Actualizar Tipos de Identificación
        ids_para_tipos = tuple(self.df['NUMERO DE IDENTIFICACION'].unique())
        query_tipos = f"SELECT CEDULA_CORRECTA, CODIGO_DATA FROM tipos_map WHERE CEDULA_CORRECTA IN ({','.join(['?']*len(ids_para_tipos))})"
        mapa_tipos_chunk = pd.read_sql_query(query_tipos, conn, params=ids_para_tipos).set_index('CEDULA_CORRECTA')['CODIGO_DATA'].to_dict()
        
        self.df['TIPO DE IDENTIFICACION'] = 1
        if mapa_tipos_chunk:
            self.df['TIPO DE IDENTIFICACION'] = self.df['NUMERO DE IDENTIFICACION'].map(mapa_tipos_chunk).combine_first(self.df['TIPO DE IDENTIFICACION'])

    def _update_data_from_db(self, conn):
        """PASO 4: Actualiza desde FNZ001 y R05 (LEYENDO DESDE SQLITE)"""
        self.df['NUMERO DE LA CUENTA U OBLIGACION'] = self.df['NUMERO DE LA CUENTA U OBLIGACION'].astype(str).str.replace(' ', '').str.zfill(18)
        
        facturas_en_chunk = tuple(self.df['NUMERO DE LA CUENTA U OBLIGACION'].unique())
        
        # A. Procesando FNZ001
        query_fnz = f"SELECT FACTURA, VALOR FROM fnz_map WHERE FACTURA IN ({','.join(['?']*len(facturas_en_chunk))})"
        mapa_fnz_chunk = pd.read_sql_query(query_fnz, conn, params=facturas_en_chunk).set_index('FACTURA')['VALOR'].to_dict()
        if mapa_fnz_chunk:
            self.df['VALOR INICIAL'] = self.df['NUMERO DE LA CUENTA U OBLIGACION'].map(mapa_fnz_chunk).combine_first(self.df['VALOR INICIAL'])

        # B. Procesando R05
        query_r05 = f"SELECT LLAVE, VALOR_ABONO FROM r05_map WHERE LLAVE IN ({','.join(['?']*len(facturas_en_chunk))})"
        mapa_r05_chunk = pd.read_sql_query(query_r05, conn, params=facturas_en_chunk).set_index('LLAVE')['VALOR_ABONO'].to_dict()
        if mapa_r05_chunk:
            self.df['VALOR SALDO MORA'] = self.df['NUMERO DE LA CUENTA U OBLIGACION'].map(mapa_r05_chunk).combine_first(self.df['VALOR SALDO MORA'])

    def _clean_and_validate_data(self):
        """PASO 5: Realiza limpieza y validaciones generales."""
        # (Este código no consume mucha RAM, se queda igual)
        # print("  - Limpiando y validando datos...", flush=True)
        # A. Limpieza de caracteres de texto
        letter_replacements = {'Ñ':'N','Á':'A','É':'E','Í':'I','Ó':'O','Ú':'U','Ü':'U','Ÿ':'Y','Â':'A','Ã':'A','š':'S','©':'C','ñ':'N','á':'A','é':'E','í':'I','ó':'O','ú':'U','ü':'U','ÿ':'Y','â':'A','ã':'A'}
        chars_to_remove = ['@','°','|','¬','¡','“','#','$','%','&','/','(',')','=','‘','\\','¿','+','~','´´','´','[','{','^','-','_','.',':',',',';','<','>','Æ','±']
        string_cols = self.df.select_dtypes(include='object').columns.drop('CORREO ELECTRONICO', errors='ignore')
        for col in string_cols:
            self.df[col] = self.df[col].astype(str)
            for old, new in letter_replacements.items(): self.df[col] = self.df[col].str.replace(old, new, regex=False)
            for char in chars_to_remove: self.df[col] = self.df[col].str.replace(char, '', regex=False)
        
        # B. Limpieza y validación de fechas
        for col in ["FECHA APERTURA", "FECHA VENCIMIENTO", "FECHA DE PAGO"]:
            self.df[col] = pd.to_numeric(self.df[col], errors='coerce').fillna(0).astype('Int64').astype(str)
        try:
            condicion_fecha_invalida = self.df['FECHA VENCIMIENTO'] < self.df['FECHA APERTURA']
            self.df.loc[condicion_fecha_invalida, 'FECHA VENCIMIENTO'] = self.df['FECHA APERTURA']
        except TypeError:
             print("SERVICE_WARN: Error de tipo al comparar fechas, saltando validación.", flush=True)
             
        mascara_fecha_pago = (self.df['FECHA DE PAGO'].str.upper().str.contains('NA')) | (self.df['FECHA DE PAGO'] == '0')
        self.df.loc[mascara_fecha_pago, 'FECHA DE PAGO'] = '00000000'

        # C. Limpieza y validación de valores numéricos
        columnas_numericas = ["VALOR INICIAL", "VALOR SALDO DEUDA", "VALOR DISPONIBLE", "V CUOTA MENSUAL", "VALOR SALDO MORA"]
        for col in columnas_numericas:
            self.df[col] = pd.to_numeric(self.df[col], errors='coerce').fillna(0)
            if col != 'VALOR DISPONIBLE':
                self.df.loc[self.df[col] < 10000, col] = 0
        self.df['VALOR DISPONIBLE'] = 0
        self.df[columnas_numericas] = self.df[columnas_numericas].astype(int)


    def _apply_final_formatting(self):
        """PASO 6: Aplica el formato final de texto y longitud."""
        # (Este código no consume mucha RAM, se queda igual)
        # print("  - Aplicando formatos finales...", flush=True)
        self.df['NOMBRE COMPLETO'] = self.df['NOMBRE COMPLETO'].astype(str).str.replace(r'\s+', ' ', regex=True).str.strip().str.upper()
        replacements_map = {'1118291452':'FANDINO LAYNE ASTRID', '1025529458':'MARTINEZ MUNOZ JOSE MANUEL', '25559122':'RAMIREZ DE CASTRO MARIA ESTELLA'}
        for id_number, new_name in replacements_map.items():
            self.df.loc[self.df['NUMERO DE IDENTIFICACION'] == id_number, 'NOMBRE COMPLETO'] = new_name
        
        col_ciudad = 'CIUDAD CORRESPONDENCIA'
        self.df[col_ciudad] = self.df[col_ciudad].astype(str).fillna('')
        mascara_reemplazo_ciudad = (self.df[col_ciudad].str.strip() == '') | (self.df[col_ciudad].str.strip().str.upper() == 'N/A') | (self.df[col_ciudad].str.strip() == '0')
        self.df.loc[mascara_reemplazo_ciudad, col_ciudad] = 'POPAYAN'

        col_celular = 'CELULAR'
        self.df[col_celular] = self.df[col_celular].astype(str).str.replace(r'\D', '', regex=True)
        es_fijo_valido = (self.df[col_celular].str.len() == 7)
        es_celular_valido = (self.df[col_celular].str.len() == 10) & (self.df[col_celular].str.startswith('3'))
        self.df.loc[~(es_fijo_valido | es_celular_valido), col_celular] = ''
        
        col_email = 'CORREO ELECTRONICO'
        self.df[col_email] = self.df[col_email].astype(str).str.strip()
        placeholders_a_eliminar = ['CORREGIR', 'PENDIENTE', 'NOTIENE', 'SINC', 'NN@', 'AAA@']
        for placeholder in placeholders_a_eliminar:
            self.df.loc[self.df[col_email].str.contains(placeholder, case=False, na=False), col_email] = ''
        email_regex_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        self.df.loc[~self.df[col_email].str.match(email_regex_pattern, na=False), col_email] = ''

        # Formatos de longitud y valores fijos
        self.df['ESTADO ORIGEN DE LA CUENTA'] = '0'
        self.df['RESPONSABLE'] = self.df['RESPONSABLE'].astype(str).str.zfill(2)
        self.df['NOVEDAD'] = self.df['NOVEDAD'].astype(str).str.zfill(2)
        self.df['TOTAL CUOTAS'] = self.df['TOTAL CUOTAS'].astype(str).str.zfill(3)
        self.df['CUOTAS CANCELADAS'] = self.df['CUOTAS CANCELADAS'].astype(str).str.zfill(3)
        self.df['CUOTAS EN MORA'] = self.df['CUOTAS EN MORA'].astype(str).str.zfill(3)
        self.df['FECHA LIMITE DE PAGO'] = self.df['FECHA LIMITE DE PAGO'].astype(str)
        self.df['SITUACION DEL TITULAR'] = '0'
        self.df['EDAD DE MORA'] = self.df['EDAD DE MORA'].astype(str).str.zfill(3)
        self.df['FORMA DE PAGO'] = self.df['FORMA DE PAGO'].astype(str)
        self.df['FECHA ESTADO ORIGEN'] = self.df['FECHA ESTADO ORIGEN'].astype(str)
        self.df['ESTADO DE LA CUENTA'] = self.df['ESTADO DE LA CUENTA'].astype(str).str.zfill(2)
        self.df['FECHA ESTADO DE LA CUENTA'] = self.df['FECHA ESTADO DE LA CUENTA'].astype(str)
        self.df['ADJETIVO'] = '0'
        self.df['FECHA DE ADJETIVO'] = self.df['FECHA DE ADJETIVO'].astype(str).str.zfill(8)
        self.df['CLAUSULA DE PERMANENCIA'] = self.df['CLAUSULA DE PERMANENCIA'].astype(str).str.zfill(3)
        self.df['FECHA CLAUSULA DE PERMANENCIA'] = self.df['FECHA CLAUSULA DE PERMANENCIA'].astype(str).str.zfill(8)
        
        self.df['NOMBRE COMPLETO'] = self.df['NOMBRE COMPLETO'].str.ljust(45)
        self.df['DIRECCION DE CORRESPONDENCIA'] = self.df['DIRECCION DE CORRESPONDENCIA'].astype(str).str.ljust(60)
        self.df['CIUDAD CORRESPONDENCIA'] = self.df[col_ciudad].str.ljust(20)
        self.df['CORREO ELECTRONICO'] = self.df[col_email].str.ljust(60)
        self.df['CELULAR'] = self.df[col_celular].str.zfill(12)
        self.df['NUMERO DE IDENTIFICACION'] = self.df['NUMERO DE IDENTIFICACION'].str.zfill(11)
        self.df['TIPO DE IDENTIFICACION'] = self.df['TIPO DE IDENTIFICACION'].astype(int).astype(str)