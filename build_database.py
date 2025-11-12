import pandas as pd
import sqlite3
import sys
import os
from collections import defaultdict

# --- CONFIGURACIÓN ---
# ¡¡¡IMPORTANTE!!!
# Coloca tu archivo de correcciones (84MB) en la misma carpeta que este script
# y cambia este nombre de archivo por el nombre del tuyo:
CORRECCIONES_EXCEL_PATH = "c:/Users/sb118/Downloads/Cédulas a revisar.xlsx" 
DATABASE_PATH = "corrections.db" # El archivo de BD que vamos a crear
# ---------------------

def crear_mapa_fnz(conn, wb):
    """Crea la tabla FNZ001 con la lógica de C1/C2"""
    print("Creando tabla 'fnz_map'...", flush=True)
    try:
        # Usamos Pandas aquí porque esto corre en tu PC (con mucha RAM)
        df_fnz = pd.read_excel(wb, sheet_name='FNZ001', usecols=['DSM_TP', 'DSM_NUM', 'VLR_FNZ'])
        df_fnz['VLR_FNZ'] = pd.to_numeric(df_fnz['VLR_FNZ'], errors='coerce').fillna(0).astype(int)
        df_fnz = df_fnz[df_fnz['VLR_FNZ'] > 0] # Filtramos
        
        df_fnz['llave_base'] = df_fnz['DSM_TP'].astype(str).str.strip() + df_fnz['DSM_NUM'].astype(str).str.strip()
        
        # Replicamos la lógica del 'concat'
        tabla_fnz = pd.concat([
            pd.DataFrame({'FACTURA': df_fnz['llave_base'], 'VALOR': df_fnz['VLR_FNZ']}), 
            pd.DataFrame({'FACTURA': df_fnz['llave_base'] + 'C1', 'VALOR': df_fnz['VLR_FNZ']}), 
            pd.DataFrame({'FACTURA': df_fnz['llave_base'] + 'C2', 'VALOR': df_fnz['VLR_FNZ']})
        ])
        tabla_fnz['FACTURA'] = tabla_fnz['FACTURA'].astype(str).str.zfill(18)
        
        # Guardamos en la BD
        tabla_fnz.to_sql('fnz_map', conn, index=False, if_exists='replace')
        # ¡ÍNDICE! La clave de la velocidad
        conn.execute("CREATE INDEX idx_fnz_factura ON fnz_map (FACTURA)")
        print("Tabla 'fnz_map' creada e indexada.", flush=True)
        
    except Exception as e:
        print(f"ERROR al crear 'fnz_map': {e}", flush=True)
        
def crear_mapa_r05(conn, wb):
    """Crea la tabla R05 con la lógica de C1/C2"""
    print("Creando tabla 'r05_map'...", flush=True)
    try:
        df_r05 = pd.read_excel(wb, sheet_name='R05', usecols=['MCNTIPCRU2', 'MCNNUMCRU2', 'ABONO'])
        df_r05['ABONO'] = pd.to_numeric(df_r05['ABONO'], errors='coerce').fillna(0)
        df_r05 = df_r05[df_r05['ABONO'] > 0] # Filtramos
        
        df_r05['llave_base'] = df_r05['MCNTIPCRU2'].astype(str).str.strip() + df_r05['MCNNUMCRU2'].astype(str).str.strip()
        abonos_sumados = df_r05.groupby('llave_base')['ABONO'].sum().reset_index()

        tabla_r05 = pd.concat([
            pd.DataFrame({'LLAVE': abonos_sumados['llave_base'], 'VALOR_ABONO': abonos_sumados['ABONO']}),
            pd.DataFrame({'LLAVE': abonos_sumados['llave_base'] + 'C1', 'VALOR_ABONO': abonos_sumados['ABONO']}),
            pd.DataFrame({'LLAVE': abonos_sumados['llave_base'] + 'C2', 'VALOR_ABONO': abonos_sumados['ABONO']})
        ])
        tabla_r05['LLAVE'] = tabla_r05['LLAVE'].astype(str).str.ljust(20)
        
        # Guardamos en la BD
        tabla_r05.to_sql('r05_map', conn, index=False, if_exists='replace')
        # ¡ÍNDICE!
        conn.execute("CREATE INDEX idx_r05_llave ON r05_map (LLAVE)")
        print("Tabla 'r05_map' creada e indexada.", flush=True)
        
    except Exception as e:
        print(f"ERROR al crear 'r05_map': {e}", flush=True)

def crear_otros_mapas(conn, wb):
    """Crea las tablas simples (Cédulas, Vinculado, Tipos)"""
    print("Creando tablas 'cedulas_map', 'vinculado_map', 'tipos_map'...", flush=True)
    try:
        # --- Cédulas (CORREGIDO) ---
        # Usamos los nombres de columna exactos de tu lógica de Finansuenos
        df_cedulas = pd.read_excel(wb, sheet_name='Cedulas a corregir', usecols=['CEDULA MAL', 'CEDULA CORRECTA'])
        df_cedulas.rename(columns={
            'CEDULA MAL': 'CEDULA_MAL',
            'CEDULA CORRECTA': 'CEDULA_CORRECTA'
        }, inplace=True)
        df_cedulas['CEDULA_MAL'] = df_cedulas['CEDULA_MAL'].astype(str).str.strip()
        df_cedulas.to_sql('cedulas_map', conn, index=False, if_exists='replace')
        conn.execute("CREATE INDEX idx_cedula_mal ON cedulas_map (CEDULA_MAL)")

        # --- Vinculado (CORREGIDO) ---
        # Guardamos la tabla entera (tu lógica original estaba bien)
        df_vinculado = pd.read_excel(wb, sheet_name='Vinculado')
        df_vinculado['CODIGO'] = df_vinculado['CODIGO'].astype(str).str.strip()
        df_vinculado.to_sql('vinculado_map', conn, index=False, if_exists='replace')
        conn.execute("CREATE INDEX idx_vinculado_codigo ON vinculado_map (CODIGO)")

        # --- Tipos (CORREGIDO) ---
        # Usamos los nombres de columna exactos de tu lógica de Finansuenos
        df_tipos = pd.read_excel(wb, sheet_name='Tipos de identificacion', usecols=['CEDULA CORRECTA', 'CODIGO DATA'])
        df_tipos.rename(columns={
            'CEDULA CORRECTA': 'CEDULA_CORRECTA',
            'CODIGO DATA': 'CODIGO_DATA'
        }, inplace=True)
        df_tipos['CEDULA_CORRECTA'] = df_tipos['CEDULA_CORRECTA'].astype(str).str.strip()
        df_tipos.to_sql('tipos_map', conn, index=False, if_exists='replace')
        conn.execute("CREATE INDEX idx_tipos_cedula ON tipos_map (CEDULA_CORRECTA)")
        
        print("Tablas simples creadas e indexadas.", flush=True)
        
    except Exception as e:
        print(f"ERROR al crear tablas simples: {e}", flush=True)
        # Importante: relanzamos el error para detener el script
        raise e

def main():
    if not os.path.exists(CORRECCIONES_EXCEL_PATH):
        print(f"ERROR: No se encuentra el archivo de Excel '{CORRECCIONES_EXCEL_PATH}'", flush=True)
        sys.exit(1)
        
    # Abrimos el Excel UNA VEZ
    print(f"Abriendo {CORRECCIONES_EXCEL_PATH} (84MB)...", flush=True)
    try:
        wb = pd.ExcelFile(CORRECCIONES_EXCEL_PATH)
    except Exception as e:
        print(f"ERROR: No se pudo abrir el archivo Excel. Asegúrate de tener 'openpyxl' instalado. Error: {e}", flush=True)
        sys.exit(1)
    print("Archivo Excel abierto.", flush=True)
    
    # Creamos la conexión a la base de datos (esto crea el archivo)
    if os.path.exists(DATABASE_PATH):
        os.remove(DATABASE_PATH)
        
    conn = sqlite3.connect(DATABASE_PATH)
    
    try:
        # Creamos las 5 tablas (mapas)
        crear_mapa_fnz(conn, wb)
        crear_mapa_r05(conn, wb)
        crear_otros_mapas(conn, wb)
        
        # Cerramos la conexión
        conn.commit()
        print(f"\n¡ÉXITO! Base de datos '{DATABASE_PATH}' creada.", flush=True)
        print("Ahora, pon este archivo 'corrections.db' en la carpeta raíz de tu proyecto.", flush=True)
        
    except Exception as e:
        print(f"\n¡FALLÓ! Ocurrió un error durante la creación de la BD: {e}", flush=True)
        print("La base de datos 'corrections.db' está incompleta. Arregla el error y vuelve a intentarlo.", flush=True)
    
    finally:
        conn.close()


if __name__ == "__main__":
    main()