import pandas as pd
import numpy as np

# --- PASO 1: DEFINICIONES INICIALES ---
print("--- PASO 1: Definiendo parámetros iniciales ---")
input_file_path = 'c:/Users/usuario/Desktop/Reporte LV/DATA MARZO FS.TXT'
ruta_archivo_correcciones = 'c:/Users/usuario/Desktop/Reporte LV/Cédulas a revisar.xlsx'

# Definición limpia de las posiciones y nombres para evitar caracteres ocultos
colspecs = [
    (0, 1), (1, 12), (30, 75), (12, 30), (76, 84), (84, 92),
    (92, 94), (107, 109), (109, 110), (188, 199), (199, 210),
    (210, 221), (221, 232), (232, 243), (243, 246), (246, 249),
    (249, 252), (263, 271), (271, 279), (577, 597), (625, 685),
    (685, 745), (445, 457), (75, 76), (185, 188), (105, 106),
    (110, 118), (118, 120), (120, 128), (137, 138), (138, 146),
    (252, 255), (255, 263)
]
names = [
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

try:
    # --- PASO 2: LECTURA Y PREPARACIÓN INICIAL ---
    print("\n--- PASO 2: Leyendo y preparando el archivo principal ---")
    df = pd.read_fwf(
        input_file_path, colspecs=colspecs, names=names, encoding='cp1252',
        skiprows=1, skipfooter=1, engine='python'
    )
    df['NUMERO DE IDENTIFICACION'] = df['NUMERO DE IDENTIFICACION'].astype(str).str.strip()

    # --- PASO 3: CORRECCIONES DE DATOS ---
    print("\n--- PASO 3: Iniciando correcciones desde archivo Excel ---")
    try:
        # A. Corregir Cédulas
        print("   A. Corrigiendo 'NUMERO DE IDENTIFICACION'...")
        df_cedulas = pd.read_excel(ruta_archivo_correcciones, sheet_name='Cedulas a corregir')
        mapa_cedulas = pd.Series(df_cedulas['CEDULA CORRECTA'].astype(str).str.strip().values, index=df_cedulas['CEDULA MAL'].astype(str).str.strip()).to_dict()
        df['NUMERO DE IDENTIFICACION'] = df['NUMERO DE IDENTIFICACION'].replace(mapa_cedulas)

        # B. Actualizar campos 'CORREGIR'
        print("   B. Actualizando campos con 'CORREGIR' desde 'Vinculado'...")
        df_vinculado = pd.read_excel(ruta_archivo_correcciones, sheet_name='Vinculado').set_index(pd.read_excel(ruta_archivo_correcciones, sheet_name='Vinculado')['CODIGO'].astype(str).str.strip())
        mapa_vinc = {'NOMBRE COMPLETO':'NOMBRE', 'DIRECCION DE CORRESPONDENCIA':'DIRECCI', 'CORREO ELECTRONICO':'VINEMAIL', 'CELULAR':'TELEFONO'}
        for col_df, col_vinc in mapa_vinc.items():
            mascara = df[col_df].astype(str).str.strip().str.contains('CORREGIR', case=False, na=False)
            if mascara.any():
                ids_a_buscar = df.loc[mascara, 'NUMERO DE IDENTIFICACION']
                valores_nuevos = ids_a_buscar.map(df_vinculado[col_vinc])
                df[col_df] = valores_nuevos.combine_first(df[col_df])

        # C. Actualizar Tipos de Identificación
        print("   C. Actualizando 'TIPO DE IDENTIFICACION'...")
        df['TIPO DE IDENTIFICACION'] = 1
        df_tipos = pd.read_excel(ruta_archivo_correcciones, sheet_name='Tipos de identificacion')
        mapa_tipos = pd.Series(df_tipos['CODIGO DATA'].values, index=df_tipos['CEDULA CORRECTA'].astype(str).str.strip()).to_dict()
        df['TIPO DE IDENTIFICACION'] = df['NUMERO DE IDENTIFICACION'].map(mapa_tipos).combine_first(df['TIPO DE IDENTIFICACION'])
        
        print("Correcciones desde Excel completadas.")
    except Exception as e:
        print(f"   ¡ERROR DURANTE CORRECCIONES EXCEL! Detalle: {e}")

    # --- PASO 4: ACTUALIZACIONES DESDE FNZ001 y R05 ---
    print("\n--- PASO 4: Actualizando datos desde hojas FNZ001 y R05 ---")
    try:
        df['NUMERO DE LA CUENTA U OBLIGACION'] = df['NUMERO DE LA CUENTA U OBLIGACION'].astype(str).str.replace(' ', '').str.zfill(18)
        
        print("   A. Procesando FNZ001 para 'VALOR INICIAL'...")
        df_fnz = pd.read_excel(ruta_archivo_correcciones, sheet_name='FNZ001', usecols=['DSM_TP', 'DSM_NUM', 'VLR_FNZ'])
        df_fnz['llave_base'] = df_fnz['DSM_TP'].astype(str).str.strip() + df_fnz['DSM_NUM'].astype(str).str.strip()
        tabla_fnz = pd.concat([pd.DataFrame({'FACTURA': df_fnz['llave_base'], 'VALOR': df_fnz['VLR_FNZ']}), pd.DataFrame({'FACTURA': df_fnz['llave_base'] + 'C1', 'VALOR': df_fnz['VLR_FNZ']}), pd.DataFrame({'FACTURA': df_fnz['llave_base'] + 'C2', 'VALOR': df_fnz['VLR_FNZ']})])
        tabla_fnz['FACTURA'] = tabla_fnz['FACTURA'].astype(str).str.zfill(18)
        mapa_fnz = pd.Series(tabla_fnz.VALOR.values, index=tabla_fnz.FACTURA).to_dict()
        df['VALOR INICIAL'] = df['NUMERO DE LA CUENTA U OBLIGACION'].map(mapa_fnz).combine_first(df['VALOR INICIAL'])
        
           # --- BLOQUE MODIFICADO ---
        print("   B. Procesando R05 para 'FECHA DE PAGO' (descartando fechas originales)...")
        df_r05 = pd.read_excel(ruta_archivo_correcciones, sheet_name='R05', usecols=['MCNTIPCRU2', 'MCNNUMCRU2', 'MCNFECHA'])

# Convertir fecha a formato YYYYMMDD para facilitar la comparación
        df_r05['FECHA_NUEVA'] = pd.to_datetime(df_r05['MCNFECHA'], format='%d/%m/%Y', errors='coerce').dt.strftime('%Y%m%d')
        df_r05.dropna(subset=['FECHA_NUEVA'], inplace=True) # Eliminar filas donde la fecha no fue válida

# Crear llave base
        df_r05['llave_base'] = df_r05['MCNTIPCRU2'].astype(str).str.strip() + df_r05['MCNNUMCRU2'].astype(str).str.strip()

# Crear tabla con las variaciones de llave (normal, C1, C2)
        tabla_r05 = pd.concat([
             pd.DataFrame({'LLAVE': df_r05['llave_base'], 'FECHA': df_r05['FECHA_NUEVA']}),
             pd.DataFrame({'LLAVE': df_r05['llave_base'] + 'C1', 'FECHA': df_r05['FECHA_NUEVA']}),
             pd.DataFrame({'LLAVE': df_r05['llave_base'] + 'C2', 'FECHA': df_r05['FECHA_NUEVA']})
             ])
        tabla_r05['LLAVE'] = tabla_r05['LLAVE'].astype(str).str.zfill(18)

# Encontrar la fecha más reciente por cada llave única
        mapa_r05 = tabla_r05.groupby('LLAVE')['FECHA'].max().to_dict()

# Mapear las fechas desde R05. Si no hay coincidencia, el resultado es NaN.
        nuevas_fechas = df['NUMERO DE LA CUENTA U OBLIGACION'].map(mapa_r05)

# REEMPLAZO TOTAL:
# Se asignan las nuevas fechas. Si el mapeo resultó en NaN (no se encontró la cuenta en R05),
# se rellena con '00000000', borrando cualquier valor que existiera antes.
        df['FECHA DE PAGO'] = nuevas_fechas.fillna('00000000')
        # --- FIN DEL BLOQUE MODIFICADO ---
        
        print("Actualizaciones completadas.")
    except Exception as e:
        print(f"   ¡ERROR! Ocurrió un error al procesar las hojas FNZ001 o R05: {e}")

    # --- PASO 5: LIMPIEZA GENERAL Y VALIDACIONES DE DATOS ---
    print("\n--- PASO 5: Realizando limpieza y formateo de datos ---")
    
    # A. Limpieza de caracteres de texto
    letter_replacements = {'Ñ':'N','Á':'A','É':'E','Í':'I','Ó':'O','Ú':'U','Ü':'U','Ÿ':'Y','Â':'A','Ã':'A','š':'S','©':'C','ñ':'N','á':'A','é':'E','í':'I','ó':'O','ú':'U','ü':'U','ÿ':'Y','â':'A','ã':'A'}
    chars_to_remove = ['@','°','|','¬','¡','“','#','$','%','&','/','(',')','=','‘','\\','¿','+','~','´´','´','[','{','^','-','_','.',':',',',';','<','>','Æ','±']
    string_cols = df.select_dtypes(include='object').columns.drop('CORREO ELECTRONICO', errors='ignore')
    for col in string_cols:
        df[col] = df[col].astype(str)
        for old, new in letter_replacements.items(): df[col] = df[col].str.replace(old, new, regex=False)
        for char in chars_to_remove: df[col] = df[col].str.replace(char, '', regex=False)
    
    # B. Limpieza y validación de fechas (sin decimales)
    for col in ["FECHA APERTURA", "FECHA VENCIMIENTO", "FECHA DE PAGO"]:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('Int64').astype(str)
    
    condicion_fecha_invalida = df['FECHA VENCIMIENTO'] < df['FECHA APERTURA']
    df.loc[condicion_fecha_invalida, 'FECHA VENCIMIENTO'] = df['FECHA APERTURA']
    
    mascara_fecha_pago = (df['FECHA DE PAGO'].str.upper().str.contains('NA')) | (df['FECHA DE PAGO'] == '0')
    df.loc[mascara_fecha_pago, 'FECHA DE PAGO'] = '00000000'

    # C. Limpieza y validación de valores numéricos
    columnas_numericas = ["VALOR INICIAL", "VALOR SALDO DEUDA", "VALOR DISPONIBLE", "V CUOTA MENSUAL", "VALOR SALDO MORA"]
    for col in columnas_numericas:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        if col != 'VALOR DISPONIBLE':
            df.loc[df[col] < 10000, col] = 0
    df['VALOR DISPONIBLE'] = 0
    df[columnas_numericas] = df[columnas_numericas].astype(int)
    print("Limpieza y validaciones completadas.")

    # --- PASO 6: FORMATEO FINAL DE TEXTO Y LONGITUD ---
    print("\n--- PASO 6: Aplicando formato final ---")
    
    # A. Formato de textos generales
    df['NOMBRE COMPLETO'] = df['NOMBRE COMPLETO'].astype(str).str.replace(r'\s+', ' ', regex=True).str.strip().str.upper()
    replacements_map = {'1118291452':'FANDINO LAYNE ASTRID', '1025529458':'MARTINEZ MUNOZ JOSE MANUEL', '25559122':'RAMIREZ DE CASTRO MARIA ESTELLA'}
    for id_number, new_name in replacements_map.items():
        df.loc[df['NUMERO DE IDENTIFICACION'] == id_number, 'NOMBRE COMPLETO'] = new_name
    
    # B. Formato de CIUDAD DE CORRESPONDENCIA
    col_ciudad = 'CIUDAD CORRESPONDENCIA'
    df[col_ciudad] = df[col_ciudad].astype(str).fillna('')
    mascara_reemplazo_ciudad = (df[col_ciudad].str.strip() == '') | (df[col_ciudad].str.strip().str.upper() == 'N/A') | (df[col_ciudad].str.strip() == '0')
    df.loc[mascara_reemplazo_ciudad, col_ciudad] = 'POPAYAN'
    
    # C. Formato de CELULAR
    col_celular = 'CELULAR'
    df[col_celular] = df[col_celular].astype(str).str.replace(r'\D', '', regex=True)
    es_fijo_valido = (df[col_celular].str.len() == 7)
    es_celular_valido = (df[col_celular].str.len() == 10) & (df[col_celular].str.startswith('3'))
    df.loc[~(es_fijo_valido | es_celular_valido), col_celular] = '0'
    
    # D. Formato de CORREO ELECTRONICO
    col_email = 'CORREO ELECTRONICO'
    df[col_email] = df[col_email].astype(str).str.strip()
    placeholders_a_eliminar = ['CORREGIR', 'PENDIENTE', 'NOTIENE', 'SINC', 'NN@', 'AAA@']
    for placeholder in placeholders_a_eliminar:
        df.loc[df[col_email].str.contains(placeholder, case=False, na=False), col_email] = ''
    email_regex_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    df.loc[~df[col_email].str.match(email_regex_pattern, na=False), col_email] = ''
    
    # E. Formatos de longitud y tipo (NUEVOS AJUSTES)
    print("   Aplicando formatos de longitud, tipo y valores fijos...")
    
    # --- ¡NUEVO CAMBIO AQUI! ---
    df['ESTADO ORIGEN DE LA CUENTA'] = '0'

    df['RESPONSABLE'] = df['RESPONSABLE'].astype(str).str.zfill(2)
    df['NOVEDAD'] = df['NOVEDAD'].astype(str).str.zfill(2)
    
    df['TOTAL CUOTAS'] = df['TOTAL CUOTAS'].astype(str).str.zfill(3)
    df['CUOTAS CANCELADAS'] = df['CUOTAS CANCELADAS'].astype(str).str.zfill(3)
    df['CUOTAS EN MORA'] = df['CUOTAS EN MORA'].astype(str).str.zfill(3)

    df['FECHA LIMITE DE PAGO'] = df['FECHA LIMITE DE PAGO'].astype(str)
    
    df['SITUACION DEL TITULAR'] = '0'
    
    df['EDAD DE MORA'] = df['EDAD DE MORA'].astype(str).str.zfill(3)
    
    df['FORMA DE PAGO'] = df['FORMA DE PAGO'].astype(str)
    df['FECHA ESTADO ORIGEN'] = df['FECHA ESTADO ORIGEN'].astype(str)
    
    df['ESTADO DE LA CUENTA'] = df['ESTADO DE LA CUENTA'].astype(str).str.zfill(2)
    
    df['FECHA ESTADO DE LA CUENTA'] = df['FECHA ESTADO DE LA CUENTA'].astype(str)
    
    df['ADJETIVO'] = '0'
    df['FECHA DE ADJETIVO'] = df['FECHA DE ADJETIVO'].astype(str).str.zfill(8)
    
    df['CLAUSULA DE PERMANENCIA'] = df['CLAUSULA DE PERMANENCIA'].astype(str).str.zfill(3)
    df['FECHA CLAUSULA DE PERMANENCIA'] = df['FECHA CLAUSULA DE PERMANENCIA'].astype(str).str.zfill(8)

    # F. Formatos finales de longitud y tipo
    print("   Aplicando formatos de longitud final...")
    df['NOMBRE COMPLETO'] = df['NOMBRE COMPLETO'].str.ljust(45)
    df['DIRECCION DE CORRESPONDENCIA'] = df['DIRECCION DE CORRESPONDENCIA'].astype(str).str.ljust(60)
    df['CIUDAD CORRESPONDENCIA'] = df[col_ciudad].str.ljust(20)
    df['CORREO ELECTRONICO'] = df[col_email].str.ljust(60)
    df['CELULAR'] = df[col_celular].str.zfill(12)
    df['NUMERO DE IDENTIFICACION'] = df['NUMERO DE IDENTIFICACION'].str.zfill(11)
    
    # Asegurar que TIPO DE IDENTIFICACION sea string al final
    df['TIPO DE IDENTIFICACION'] = df['TIPO DE IDENTIFICACION'].astype(int).astype(str)
    
    print("Formato final aplicado.")

    # --- PASO 7: GUARDAR ARCHIVO FINAL ---
    output_filename = "Data_Marzo_Finalisimo.xlsx"
    df.to_excel(output_filename, index=False)
    print(f"\n¡PROCESO COMPLETADO! Se ha generado el archivo final '{output_filename}'.")

except Exception as e:
    print(f"\nHa ocurrido un error inesperado en el proceso principal: {e}")