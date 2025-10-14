import pandas as pd
import numpy as np

# --- CONFIGURACIÓN CENTRALIZADA DE ARCHIVOS ---
config = {
    "CRTMPCONSULTA1": {
        "filepath": 'c:/Users/sb118/Downloads/CRTMPCONSULTA1 (GENERAL).XLSX',
        "usecols": ["CORREO", "FECHA_FACT", "TIPO_DOCUM", "NUMERO_DOC", "IDENTIFICA", "NOMBRE_PRO", "TOTVENTA", "CANTIDAD", "CLIENTE", "DNONOMBRE", "NOMBRE_LIN", "CODIGO_ASE", "ASESOR"],
        "rename_map": {
            "NOMBRE_LIN": "Linea", "FECHA_FACT": "Fecha_Facturada", "TIPO_DOCUM": "Tipo_Credito",
            "NUMERO_DOC": "Numero_Credito", "IDENTIFICA": "Cedula_Cliente", "CLIENTE": "Nombre_Cliente",
            "NOMBRE_PRO": "Nombre_Producto", "TOTVENTA": "Total_Venta", "DNONOMBRE": "Forma_Pago",
            "CODIGO_ASE": "Codigo_Vendedor", "ASESOR": "Nombre_Asesor", "CANTIDAD": "Cantidad_Item"
        }
    },
    "ASESORES": {
        "filepath": 'c:/Users/sb118/Downloads/ASESORES ACTIVOS.xlsx',
        "sheet_name": "ASESORES",
        "usecols": ["CODIGO_VENDEDOR", "JEFE VENTAS", "MOVIL ASESOR", "LIDER ZONA", "MOVIL LIDER"],
        "rename_map": {
            "CODIGO_VENDEDOR": "Codigo_Vendedor", "MOVIL ASESOR": "Movil_Vendedor",
            "LIDER ZONA": "Lider_Zona", "MOVIL LIDER": "Movil_Lider", "JEFE VENTAS": "Jefe_ventas"
        }
    }
}

def procesar_datos_clientes(df_ventas, df_asesores):
    """
    Procesa los datos para obtener un resumen por cliente y forma de pago.
    """
    print("--- Procesando datos de ventas y asesores ---")

    # 1. Preparación inicial de datos
    df_ventas['Credito'] = df_ventas['Tipo_Credito'].astype(str) + '-' + df_ventas['Numero_Credito'].astype(str)
    formas_pago_validas = [
        'VENTAS BRILLA', 'FINANCIADA', 'CREDICONTADO', 'ESTRICTO CONTADO',
        'VENTAS SISTECREDITO', 'FINANSUEÐOS', 'VENTAS ADDI'
    ]
    df_ventas = df_ventas[df_ventas['Forma_Pago'].isin(formas_pago_validas)].copy()
    df_ventas['Es_Obsequio'] = (df_ventas['Total_Venta'] >= 1000) & (df_ventas['Total_Venta'] <= 6000)

    # 2. Calcular el TOTAL de créditos por cliente ANTES de agrupar
    creditos_por_cliente = df_ventas.groupby('Cedula_Cliente', observed=True)['Credito'].nunique().reset_index(name='Numero_Total_Creditos')

    # 3. Agrupar por CLIENTE y FORMA DE PAGO
    print("--- Agrupando créditos y productos por cliente y forma de pago ---")
    
    def agregar_items_agrupados(df_group):
        productos = [f"{row['Nombre_Producto']} ({int(row['Cantidad_Item'])})" for _, row in df_group[~df_group['Es_Obsequio']].iterrows()]
        obsequios = [f"{row['Nombre_Producto']} ({int(row['Cantidad_Item'])})" for _, row in df_group[df_group['Es_Obsequio']].iterrows()]
        
        creditos_unicos = df_group['Credito'].unique()
        lineas_unicas = df_group['Linea'].unique()
        
        # **NUEVA LÍNEA**: Contar los créditos únicos solo para este grupo (esta forma de pago)
        creditos_en_grupo = len(creditos_unicos)
        
        return pd.Series({
            'Creditos_Agrupados': ' | '.join(creditos_unicos),
            # **NUEVA COLUMNA**: Añadimos el conteo al resultado
            'Creditos_Por_Forma_Pago': creditos_en_grupo,
            'Lineas_Agrupadas': ' | '.join(lineas_unicas),
            'Productos': ', '.join(productos) if productos else 'N/A',
            'Obsequios': ', '.join(obsequios) if obsequios else 'N/A'
        })

    df_agrupado = df_ventas.groupby(
        ['Cedula_Cliente', 'Nombre_Cliente', 'Forma_Pago'], 
        observed=True
    ).apply(agregar_items_agrupados).reset_index()

    # 4. Unir el conteo TOTAL de créditos
    df_resultado = pd.merge(df_agrupado, creditos_por_cliente, on='Cedula_Cliente')

    # 5. Determinar el último asesor activo por cliente
    print("--- Asignando último asesor activo por cliente ---")
    df_ventas['Fecha_Facturada'] = pd.to_datetime(df_ventas['Fecha_Facturada'], errors='coerce')
    
    df_asesores['Codigo_Vendedor'] = pd.to_numeric(df_asesores['Codigo_Vendedor'], errors='coerce').dropna().astype(int).astype(str)
    df_ventas['Codigo_Vendedor'] = pd.to_numeric(df_ventas['Codigo_Vendedor'], errors='coerce').dropna().astype(int).astype(str)
    
    ultimo_asesor_df = df_ventas.loc[df_ventas.groupby('Cedula_Cliente', observed=True)['Fecha_Facturada'].idxmax()].copy()
    ultimo_asesor_df['Asesor_Activo'] = ultimo_asesor_df['Codigo_Vendedor'].isin(df_asesores['Codigo_Vendedor'])

    ultimo_asesor_df['Codigo_Vendedor_Final'] = np.where(ultimo_asesor_df['Asesor_Activo'], ultimo_asesor_df['Codigo_Vendedor'], 'N/A')
    ultimo_asesor_df['Nombre_Asesor_Final'] = np.where(ultimo_asesor_df['Asesor_Activo'], ultimo_asesor_df['Nombre_Asesor'], 'SIN ASESOR ACTIVO')

    info_final_asesor = ultimo_asesor_df[['Cedula_Cliente', 'Codigo_Vendedor_Final', 'Nombre_Asesor_Final']]
    df_resultado = pd.merge(df_resultado, info_final_asesor, on='Cedula_Cliente', how='left')
    

    print("--- Finalizando el proceso ---")
    return df_resultado

# --- CARGA DE DATOS ---
print("--- Cargando datos de archivos Excel ---")
cfg_ventas = config['CRTMPCONSULTA1']
df_ventas = pd.read_excel(cfg_ventas['filepath'], usecols=cfg_ventas['usecols'])
df_ventas.rename(columns=cfg_ventas['rename_map'], inplace=True)

cfg_asesores = config['ASESORES']
df_asesores = pd.read_excel(cfg_asesores['filepath'], sheet_name=cfg_asesores['sheet_name'], usecols=cfg_asesores['usecols'])
df_asesores.rename(columns=cfg_asesores['rename_map'], inplace=True)

# --- EJECUCIÓN DEL PROCESO ---
df_final = procesar_datos_clientes(df_ventas, df_asesores)

print("--- Proceso finalizado. Mostrando las primeras 5 filas del resultado ---")
print(df_final.head())

# --- GUARDAR EN EXCEL ---
ruta_salida = 'c:/Users/sb118/Downloads/Clientes_lau.xlsx'
df_final.to_excel(ruta_salida, index=False)

print(f"\n✅ ¡Archivo guardado exitosamente en: {ruta_salida}!")