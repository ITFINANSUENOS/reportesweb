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

def procesar_datos_clientes_final(df_ventas, df_asesores):
    """
    Procesa los datos para obtener un resumen único por cliente, consolidando
    productos, líneas, venta total y asignando el último asesor activo con su código.
    """
    print("--- Procesando datos de ventas y asesores (versión final con código asesor) ---")

    # 1. Preparación inicial de datos
    formas_pago_validas = [
        'VENTAS BRILLA', 'FINANCIADA', 'CREDICONTADO', 'ESTRICTO CONTADO',
        'VENTAS SISTECREDITO', 'FINANSUEÐOS', 'VENTAS ADDI'
    ]
    df_ventas = df_ventas[df_ventas['Forma_Pago'].isin(formas_pago_validas)].copy()
    
    df_ventas['Producto_Con_Cantidad'] = df_ventas['Nombre_Producto'] + ' (' + df_ventas['Cantidad_Item'].astype(int).astype(str) + ')'

    # 2. Agrupar por CLIENTE y consolidar todos sus datos
    print("--- Agrupando productos, líneas y sumando ventas por cliente ---")
    df_agrupado = df_ventas.groupby(
        ['Cedula_Cliente', 'Nombre_Cliente'],
        observed=True
    ).agg(
        Productos=('Producto_Con_Cantidad', lambda x: ', '.join(x)),
        Lineas=('Linea', lambda x: ' | '.join(x.unique())),
        Total_Ventas=('Total_Venta', 'sum')
    ).reset_index()

    # 3. Determinar el último asesor activo por cliente
    print("--- Asignando último asesor activo y su código por cliente ---")
    df_ventas['Fecha_Facturada'] = pd.to_datetime(df_ventas['Fecha_Facturada'], errors='coerce')
    
    df_asesores['Codigo_Vendedor'] = pd.to_numeric(df_asesores['Codigo_Vendedor'], errors='coerce').dropna().astype(int).astype(str)
    df_ventas['Codigo_Vendedor'] = pd.to_numeric(df_ventas['Codigo_Vendedor'], errors='coerce').dropna().astype(int).astype(str)
    
    ultimo_asesor_df = df_ventas.loc[df_ventas.groupby('Cedula_Cliente', observed=True)['Fecha_Facturada'].idxmax()].copy()
    
    ultimo_asesor_df['Asesor_Activo'] = ultimo_asesor_df['Codigo_Vendedor'].isin(df_asesores['Codigo_Vendedor'])

    # --- NUEVO: Asignar el código del asesor final ---
    # Si el asesor está activo, usa su código; si no, pone 'N/A'
    ultimo_asesor_df['Codigo_Asesor_Final'] = np.where(
        ultimo_asesor_df['Asesor_Activo'],
        ultimo_asesor_df['Codigo_Vendedor'],
        'N/A'
    )
    
    # Asignar el nombre del asesor final (sin cambios)
    ultimo_asesor_df['Nombre_Asesor_Final'] = np.where(
        ultimo_asesor_df['Asesor_Activo'], 
        ultimo_asesor_df['Nombre_Asesor'], 
        'SIN ASESOR ACTIVO'
    )
    
    # --- AJUSTE: Incluir el código del asesor en la selección de columnas ---
    info_final_asesor = ultimo_asesor_df[['Cedula_Cliente', 'Codigo_Asesor_Final', 'Nombre_Asesor_Final']]

    # 4. Unir la información del asesor al dataframe agrupado
    df_resultado = pd.merge(df_agrupado, info_final_asesor, on='Cedula_Cliente', how='left')
    
    # 5. Renombrar y organizar columnas finales
    df_resultado.rename(columns={
        'Nombre_Asesor_Final': 'Asesor',
        'Codigo_Asesor_Final': 'Codigo_Asesor'
    }, inplace=True)
    
    # --- AJUSTE: Añadir la nueva columna al orden final ---
    columnas_finales = ['Cedula_Cliente', 'Nombre_Cliente', 'Productos', 'Lineas', 'Total_Ventas', 'Codigo_Asesor', 'Asesor']
    df_resultado = df_resultado[columnas_finales]


    print("--- Finalizando el proceso final ---")
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
df_final = procesar_datos_clientes_final(df_ventas, df_asesores)

print("--- Proceso finalizado. Mostrando las primeras 5 filas del resultado ---")
print(df_final.head())

# --- GUARDAR EN EXCEL ---
ruta_salida = 'c:/Users/sb118/Downloads/Clientes_base.xlsx'
df_final.to_excel(ruta_salida, index=False)

print(f"\n✅ ¡Archivo guardado exitosamente en: {ruta_salida}!")
