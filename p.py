import pandas as pd

# --- CONFIGURA ESTAS DOS VARIABLES ---
ruta_base = "/home/sb118/dev/Archivos de pruebas/Reportes generados/Reporte_Base (SEPTIEMBRE).xlsx"
nombre_columna = 'Fecha_Cuota_Vigente' # O 'Fecha_Cuota_Atraso'
# ------------------------------------

print(f"--- Analizando la columna '{nombre_columna}' ---")

try:
    # Leemos el archivo SIN NINGÚN MAPA DE TIPOS para ver cómo lo interpreta pandas por defecto
    df = pd.read_excel(ruta_base)

    # Verificamos que la columna exista
    if nombre_columna not in df.columns:
        print(f"ERROR: La columna '{nombre_columna}' no se encontró en el archivo.")
    else:
        # Imprimimos los tipos de datos que pandas detectó
        print("\n1. Tipos de datos detectados en la columna (sin forzar):")
        print(df[nombre_columna].apply(type).value_counts())

        # Imprimimos los primeros 20 valores únicos que no son nulos para ver ejemplos
        print(f"\n2. Muestra de los primeros 20 valores únicos en la columna:")
        # Usamos .dropna() para ignorar celdas vacías y .unique() para no repetir valores
        valores_unicos = df[nombre_columna].dropna().unique()
        print(valores_unicos[:20])

except FileNotFoundError:
    print(f"ERROR: No se pudo encontrar el archivo en la ruta: {ruta_base}")
except Exception as e:
    print(f"Ocurrió un error inesperado: {e}")