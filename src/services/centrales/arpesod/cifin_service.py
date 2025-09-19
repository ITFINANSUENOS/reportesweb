import pandas as pd
import re
# Asegúrate de haber instalado rapidfuzz con: pip install rapidfuzz
from rapidfuzz.distance import Levenshtein

class ArpesodDataProcessorService:
    """
    Servicio para procesar y transformar datos de Arpesod, aplicando una serie de 
    limpiezas, validaciones y formatos definidos por las reglas de negocio.
    """
    def __init__(self, df, ruta_correcciones, column_mapping):
        self.df = df.copy() 
        self.ruta_correcciones = ruta_correcciones
        self.map = column_mapping

    def _tiene_diversidad(self, texto: str, umbral: int = 3) -> bool:
        """
        Verifica si el texto tiene un número mínimo de caracteres alfabéticos únicos.
        """
        if not isinstance(texto, str):
            return False
        letras_unicas = set(c for c in texto.lower() if c.isalpha())
        return len(letras_unicas) >= umbral

    def _es_correo_valido_estricto(self, correo: str) -> bool:
        """
        Valida un correo electrónico usando una lógica estricta y avanzada.
        """
        if not isinstance(correo, str) or not correo:
            return False
        correo = correo.strip()
        pattern = re.compile(
            r"^(?![.-])(?!(?:.*[.]{2}))[A-Z0-9._%+-ñÑ]{3,}@[A-Z0-9.-]{3,}\.[A-Z]{2,}$",
            re.IGNORECASE
        )
        if not pattern.match(correo):
            return False
        try:
            usuario, _ = correo.split('@', 1)
            usuario = usuario.lower()
        except ValueError:
            return False # No tiene un solo @

        if usuario.isdigit():
            return False
            
        if not self._tiene_diversidad(usuario):
            return False

        blacklist = [
            "notiene", "sincorreo", "pendiente", "corregir", "noregistra",
            "nulo", "ninguno", "vacio", "nodisponible"
        ]
        for item_prohibido in blacklist:
            if Levenshtein.distance(usuario, item_prohibido) <= 2:
                return False

        return True

    # --- MÉTODOS PRINCIPALES DEL PROCESO ---
    def run_all_transformations(self):
        """
        Ejecuta la secuencia completa de transformaciones sobre el DataFrame.
        """
        print("Servicio: Ejecutando todas las transformaciones...")
        self._correct_data_from_excel()
        self._update_data_from_sheets()
        self._clean_and_validate_data()
        self._apply_final_formatting()
        self._final_cleanup()
        self._apply_padding_formats()
        print("Servicio: Transformaciones completadas.")
        return self.df
    
    def _correct_data_from_excel(self):
        """
        Realiza la limpieza y filtrado de datos basado en reglas de negocio 
        definidas en un archivo Excel de correcciones.
        """
        print("  - Corrigiendo y filtrando datos desde Excel...")
        try:
            df_R91 = pd.read_excel(self.ruta_correcciones, sheet_name='R91', usecols=['MCDZONA', 'MCDVINCULA', 'VINNOMBRE'], dtype=str)
            df_cedulas_original = pd.read_excel(self.ruta_correcciones, sheet_name='CEDULAS_NO_REPORTAR', usecols=['NIT', 'NOMBRE'], dtype=str)
            df_facturas_eliminar = pd.read_excel(self.ruta_correcciones, sheet_name='FACTURAS_ELIMINAR', dtype=str)
        except Exception as e:
            print(f"❌ ERROR: No se pudo leer el archivo de correcciones: {e}")
            return

        # Procesar cédulas a no reportar
        cedulas_1CE = df_R91[df_R91['MCDZONA'] == '1CE'][['MCDVINCULA', 'VINNOMBRE']].rename(columns={'MCDVINCULA': 'NIT', 'VINNOMBRE': 'NOMBRE'})
        df_cedulas_completo = pd.concat([df_cedulas_original, cedulas_1CE]).drop_duplicates(subset=['NIT'], keep='first')
        
        # Eliminar NITs
        nits_a_eliminar = set(df_cedulas_completo['NIT'].astype(str).str.strip())
        col_id = self.map['id_number']
        self.df[col_id] = self.df[col_id].astype(str).str.strip()
        registros_antes = len(self.df)
        self.df = self.df[~self.df[col_id].str.lstrip('0').isin(nits_a_eliminar)]
        print(f"    -> Se eliminaron {registros_antes - len(self.df)} registros por coincidencia de NIT.")

        # Eliminar facturas
        facturas_a_eliminar = set(df_facturas_eliminar['NUMERO DE LA CUENTA U OBLIGACION'].astype(str).str.strip())
        col_obligacion = self.map['account_number']
        self.df[col_obligacion] = self.df[col_obligacion].astype(str).str.strip()
        registros_antes = len(self.df)
        self.df = self.df[~self.df[col_obligacion].isin(facturas_a_eliminar)]
        print(f"    -> Se eliminaron {registros_antes - len(self.df)} registros por coincidencia de factura.")

    def _update_data_from_sheets(self):
        """
        Actualiza valores del DataFrame principal cruzando información con
        las hojas FNZ001 y R05 del archivo de correcciones.
        """
        print("  - Actualizando desde SALDOS_INICIALES...")
         # Se mantiene la limpieza de la columna clave en el DataFrame principal
        self.df[self.map['account_number']] = self.df[self.map['account_number']].astype(str).str.replace(' ', '').str.strip().str[:20]

        print(" - Verificando saldos negativos desde 'SALDOS_INICIALES'...")
        try:
            # 1. Leer la hoja de Excel con los saldos a comparar
            df_saldos = pd.read_excel(
                self.ruta_correcciones,
                sheet_name='SALDOS_INICIALES',
                usecols=['NUMERO_CUENTA', 'VALOR_INICIAL']
            )

            # 2. Limpiar la clave de la misma forma que en el DataFrame principal para asegurar el cruce
            df_saldos['NUMERO_CUENTA_LIMPIA'] = df_saldos['NUMERO_CUENTA'].astype(str).str.replace(' ', '').str.strip().str[:20]

            # 3. Cruzar el DF principal con los saldos usando 'merge' en un DF temporal
            df_comparacion = pd.merge(
                left=self.df[[self.map['account_number'], self.map['initial_value']]],
                right=df_saldos[['NUMERO_CUENTA_LIMPIA', 'VALOR_INICIAL']],
                left_on=self.map['account_number'],
                right_on='NUMERO_CUENTA_LIMPIA',
                how='inner'
            )

            # 4. Realizar el cálculo de la diferencia
            df_comparacion['VALOR_INICIAL'] = pd.to_numeric(df_comparacion['VALOR_INICIAL'], errors='coerce').fillna(0)
            df_comparacion[self.map['initial_value']] = pd.to_numeric(df_comparacion[self.map['initial_value']], errors='coerce').fillna(0)
            df_comparacion['Diferencia'] = df_comparacion['VALOR_INICIAL'] - df_comparacion[self.map['initial_value']]

            # 5. Filtrar para obtener solo los resultados negativos
            df_negativos = df_comparacion[df_comparacion['Diferencia'] < 0].copy()

            # 6. Si se encontraron saldos negativos, exportarlos a una nueva hoja
            if not df_negativos.empty:
                print(f"  -> Se encontraron {len(df_negativos)} saldos negativos. Exportando a la hoja 'SALDOS_NEGATIVOS'...")
                
                # --- CAMBIO APLICADO AQUÍ ---
                # Preparar el DataFrame para el reporte con las 4 columnas solicitadas para máxima claridad
                reporte_df = df_negativos[[
                    self.map['account_number'],
                    self.map['initial_value'],
                    'VALOR_INICIAL',
                    'Diferencia'
                ]].rename(columns={
                    # Renombramos para que los encabezados en Excel sean claros
                    self.map['account_number']: 'account_number',
                    self.map['initial_value']: 'initial_value_dataframe', # El valor original en tu DF
                    'VALOR_INICIAL': 'VALOR_INICIAL_excel',      # El valor con el que se comparó
                    'Diferencia': 'Diferencia'                  # El resultado negativo
                })

                # Usar ExcelWriter para AÑADIR la nueva hoja al archivo existente
                with pd.ExcelWriter(self.ruta_correcciones, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
                    reporte_df.to_excel(writer, sheet_name='SALDOS_NEGATIVOS', index=False)
                
                print("  -> Hoja 'SALDOS_NEGATIVOS' generada exitosamente con el detalle completo.")
            else:
                print("  -> No se encontraron saldos negativos.")

        except Exception as e:
            print(f"  ADVERTENCIA: No se pudo procesar la hoja 'SALDOS_INICIALES' para buscar negativos. Error: {e}")

        # # Lógica de actualización con R05
        # df_r05 = pd.read_excel(self.ruta_correcciones, sheet_name='R05', usecols=['MCNTIPCRU2', 'MCNNUMCRU2', 'ABONO'])
        # df_r05['ABONO'] = pd.to_numeric(df_r05['ABONO'], errors='coerce').fillna(0)
        # tipo_cru = df_r05['MCNTIPCRU2'].astype(str).str.strip().str.upper() # <-- CAMBIO: Limpia espacios y convierte a mayúsculas
        # num_cru = df_r05['MCNNUMCRU2'].astype(str).str.strip().str.upper()   # <-- CAMBIO: Limpia espacios y convierte a mayúsculas

        # # Se combinan las claves y LUEGO se aplica el formato final
        # df_r05['llave_base'] = (tipo_cru + num_cru).str.replace(' ', '').str[:20] # <-- CAMBIO: Se quitan espacios internos y se trunca a 20
        # # --- FIN DE CAMBIOS ---

        # abonos_sumados = df_r05.groupby('llave_base')['ABONO'].sum().reset_index()
        # abonos_sumados['ABONO'] = (abonos_sumados['ABONO'] / 1000).astype(int)

        # # Esta parte ya estaba bien, pero se beneficia de la clave limpia
        # tabla_r05 = pd.concat([
        #     pd.DataFrame({'LLAVE': abonos_sumados['llave_base'], 'VALOR_ABONO': abonos_sumados['ABONO']}),
        #     pd.DataFrame({'LLAVE': abonos_sumados['llave_base'] + 'C1', 'VALOR_ABONO': abonos_sumados['ABONO']}),
        #     pd.DataFrame({'LLAVE': abonos_sumados['llave_base'] + 'C2', 'VALOR_ABONO': abonos_sumados['ABONO']})
        # ])

        # # Importante: Asegurarse de que las claves con C1/C2 también se trunquen
        # tabla_r05['LLAVE'] = tabla_r05['LLAVE'].str[:20] # <-- CAMBIO ADICIONAL: Seguridad para claves con sufijo
        # mapa_r05 = pd.Series(tabla_r05.VALOR_ABONO.values, index=tabla_r05.LLAVE).to_dict()
        
        # # Obtén las claves de tu DataFrame principal que deberían coincidir
        # claves_df = set(self.df[self.map['account_number']].unique())

        # # Obtén las claves de tu diccionario de R05
        # claves_mapa = set(mapa_r05.keys())

        # # Muestra algunas claves de ejemplo de cada lado
        # print(f"Ejemplo de claves en el DataFrame principal: {list(claves_df)[:5]}")
        # print(f"Ejemplo de claves en el mapa de R05: {list(claves_mapa)[:5]}")

        # # Encuentra las que están en tu DF pero no en el mapa
        # diferencia = claves_df - claves_mapa
        # if diferencia:
        #     print(f"Se encontraron {len(diferencia)} claves en el DF que NO están en el mapa R05.")
        #     print(f"Ejemplo de claves no encontradas: {list(diferencia)[:5]}")
        # else:
        #     print("Todas las claves del DF parecen estar en el mapa. ¡Revisa los valores!")

        # # La línea final de mapeo ahora debería funcionar correctamente
        # self.df[self.map['actual_value_paid']] = self.df[self.map['account_number']].map(mapa_r05).fillna(0).astype(int)

    def _clean_and_validate_data(self):
        """
        Aplica limpiezas generales a columnas de texto, numéricas y fechas.
        Aquí se incluye la validación estricta de correos electrónicos.
        """
        print("  - Limpiando y validando datos...")
        letter_replacements = {'Ñ':'N','Á':'A','É':'E','Í':'I','Ó':'O','Ú':'U'}
        chars_to_remove = ['@','°','|','¬','¡','“','#','$','%','&','/','(',')','=','‘','\\','¿','+','~','´´','´','[','{','^','-','_','.',':',',',';','<','>']

        # Limpieza de columnas de texto (excluyendo email temporalmente)
        string_cols = self.df.select_dtypes(include='object').columns.drop(self.map.get('email', ''), errors='ignore')
        for col in string_cols:
            self.df[col] = self.df[col].astype(str).str.upper()
            for old, new in letter_replacements.items(): self.df[col] = self.df[col].str.replace(old, new, regex=False)
            for char in chars_to_remove: self.df[col] = self.df[col].str.replace(char, '', regex=False)
        
        # Limpieza y validación de fechas
        for col_name in [self.map['open_date'], self.map['due_date']]:
            self.df[col_name] = pd.to_numeric(self.df[col_name], errors='coerce').fillna(0).astype('Int64').astype(str)
        self.df.loc[self.df[self.map['due_date']] < self.df[self.map['open_date']], self.map['due_date']] = self.df[self.map['open_date']]
        
        # Limpieza de columnas numéricas
        numeric_cols_keys = ['initial_value', 'balance_due', 'available_value', 'monthly_fee', 'arrears_value']
        columnas_numericas = [self.map[k] for k in numeric_cols_keys if k in self.map]
        for col in columnas_numericas:
            self.df[col] = pd.to_numeric(self.df[col], errors='coerce').fillna(0)
            self.df.loc[self.df[col] <= 10, col] = 0
        self.df[columnas_numericas] = self.df[columnas_numericas].astype(int)

        # --- VALIDACIÓN ESTRICTA DE CORREOS ELECTRÓNICOS (NUEVA LÓGICA) ---
        print("    -> Validando correos electrónicos con lógica estricta...")
        col_email = self.map['email']
        self.df[col_email] = self.df[col_email].astype(str).fillna('')
        correos_invalidos = ~self.df[col_email].apply(self._es_correo_valido_estricto)
        self.df.loc[correos_invalidos, col_email] = ''
        print(f"      -> Se invalidaron {correos_invalidos.sum()} correos por formato incorrecto o patrones no válidos.")

    def _apply_final_formatting(self):
        """
        Aplica formatos específicos y reglas de negocio a columnas individuales
        como ciudad, departamento, nombres y teléfonos.
        """
        print("  - Aplicando formatos finales...")
        
        # Formato de Ciudad y Departamento
        for col, default in [(self.map['city'], 'POPAYAN'), (self.map['department'], 'CAUCA')]:
            self.df[col] = self.df[col].astype(str).str.strip().str.upper()
            cond_invalida = self.df[col].isin(['', '0', 'NAN', 'NONE']) | self.df[col].str.isdigit() | self.df[col].isnull()
            self.df.loc[cond_invalida, col] = default
        
        # Formato de Nombre Completo y correcciones específicas
        self.df[self.map['full_name']] = self.df[self.map['full_name']].astype(str).str.replace(r'\s+', ' ', regex=True).str.strip().str.upper()
        self.df[self.map['id_number']] = self.df[self.map['id_number']].astype(str)
        correcciones_nombre = {'1118291452': 'FANDINO LAYNE ASTRID', '1025529458': 'MARTINEZ MUNOZ JOSE MANUEL', '25559122': 'RAMIREZ DE CASTRO MARIA ESTELLA'}
        for cedula, nombre in correcciones_nombre.items():
            self.df.loc[self.df[self.map['id_number']] == cedula, self.map['full_name']] = nombre
        
        # Formato y validación de Teléfonos
        for key in ['home_phone', 'company_phone', 'phone']:
            if key in self.map:
                col = self.map[key]
                self.df[col] = self.df[col].astype(str).str.replace(r'\D', '', regex=True).replace('^0+$', '', regex=True).str.strip()
                es_fijo = self.df[col].str.len() == 7
                es_celular = (self.df[col].str.len() == 10) & self.df[col].str.startswith('3')
                self.df.loc[~(es_fijo | es_celular), col] = ''

        # Asignación de periodicidad (código original)
        self.df[self.map['periodicity']] = '05'

    def _final_cleanup(self):
        """
        Limpia cualquier valor nulo o texto 'nan' restante en todas las
        columnas no numéricas antes de aplicar el formato final de padding.
        """
        print("  - Realizando limpieza final de valores nulos...")
        numeric_keys = ['initial_value', 'balance_due', 'available_value', 'monthly_fee', 'arrears_value', 'actual_value_paid']
        columnas_numericas = [self.map[k] for k in numeric_keys if k in self.map]
        columnas_texto = self.df.columns.drop(columnas_numericas)
        
        for col in columnas_texto:
            self.df[col] = self.df[col].astype(str).str.strip().replace(r'(?i)^nan$', '', regex=True).fillna('')

    def _apply_padding_formats(self):
        """
        Aplica los formatos de longitud fija (.ljust y .zfill) como último paso.
        """
        print("  - Aplicando formatos de longitud y relleno finales...")
        
        # Mapeo de columnas a su formato de padding
        padding_map = {
            'arrears_age': ('zfill', 2), 'full_name': ('ljust', 60),
            'account_number': ('ljust', 20), 'address': ('ljust', 60),
            'city': ('ljust', 20), 'department': ('ljust', 20),
            'email': ('ljust', 60), 'phone': ('ljust', 60),
            'home_phone': ('ljust', 20), 'company_phone': ('ljust', 20),
            'id_number': ('zfill', 15)
        }
        
        for key, (method, length) in padding_map.items():
            if key in self.map:
                col_name = self.map[key]
                self.df[col_name] = self.df[col_name].astype(str)
                if method == 'zfill':
                    self.df[col_name] = self.df[col_name].str.zfill(length)
                else: # ljust
                    self.df[col_name] = self.df[col_name].str.ljust(length)