# src/utils/polars_utils.py
import polars as pl
import os
import json
from datetime import datetime, date

def guardar_json(data: dict, output_path: str) -> bool:
    """Guarda un diccionario en JSON manejando fechas."""
    def json_serial(obj):
        if isinstance(obj, (datetime, date)): return obj.isoformat()
        raise TypeError (f"Type {type(obj)} not serializable")
    try:
        # --- CORRECCIÓN AQUÍ ---
        # Solo intentamos crear carpetas si la ruta tiene un directorio padre
        parent_dir = os.path.dirname(output_path)
        if parent_dir: 
            os.makedirs(parent_dir, exist_ok=True)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4, default=json_serial)
        return True
    except Exception as e:
        print(f"❌ Error guardando JSON {output_path}: {e}")
        return False

def guardar_parquet(df: pl.DataFrame, output_path: str, cols_especificas: list = None) -> bool:
    """Guarda un DataFrame en Parquet comprimido."""
    try:
        parent_dir = os.path.dirname(output_path)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)

        if cols_especificas:
            cols_existentes = list(dict.fromkeys([c for c in cols_especificas if c in df.columns]))
            df_to_save = df.select(cols_existentes)
        else:
            df_to_save = df
        
        df_to_save.write_parquet(output_path, compression="snappy")
        print(f"💾 Parquet guardado: {output_path}")
        return True
    except Exception as e:
        print(f"❌ Error guardando Parquet {output_path}: {e}")
        return False

def leer_hoja_excel(file_path, sheet_name, cols_requeridas, overrides):
    """
    Lee una hoja de Excel de forma resiliente y eficiente.
    """
    common_options = {
        "infer_schema_length": 10000, 
        "null_values": ["NA", "N/A", "null", "nan", "NO APLICA"], 
        "ignore_errors": False
    }
    
    try:
        opciones_lectura = {
            **common_options, 
            "schema_overrides": overrides
        }
        
        if cols_requeridas:
            opciones_lectura["columns"] = cols_requeridas

        df = pl.read_excel(
            file_path, 
            sheet_name=sheet_name, 
            engine="xlsx2csv",
            read_csv_options=opciones_lectura
        )
        return df
    except Exception as e:
        if "columns" in str(e) or "not found" in str(e):
            print(f"⚠️ Aviso: Columnas no coinciden en '{sheet_name}'. Leyendo todo...")
            try:
                df = pl.read_excel(
                    file_path, sheet_name=sheet_name, engine="xlsx2csv",
                    read_csv_options={**common_options, "schema_overrides": overrides}
                )
                if cols_requeridas:
                    return df.select([c for c in cols_requeridas if c in df.columns])
                return df
            except Exception as e2:
                print(f"❌ Error fatal leyendo '{sheet_name}': {e2}")
                return pl.DataFrame()
        else:
            print(f"⚠️ Error leyendo hoja '{sheet_name}': {e}")
            return pl.DataFrame()

def limpiar_texto_lote(df: pl.DataFrame, cols: list) -> pl.DataFrame:
    valid_cols = [c for c in cols if c in df.columns]
    if valid_cols:
        df = df.with_columns([pl.col(c).cast(pl.Utf8).str.strip_chars() for c in valid_cols])
    return df

def parsear_fechas(df: pl.DataFrame, cols: list) -> pl.DataFrame:
    for c in cols:
        if c in df.columns:
            # 1. Si Pandas/Orquestador ya lo leyó como fecha nativa, solo aseguramos que no tenga horas
            if df.schema[c] in [pl.Date, pl.Datetime]:
                df = df.with_columns(pl.col(c).cast(pl.Date))
            else:
                # 2. Si viene como Texto, aplicamos la magia para adivinar el formato
                df = df.with_columns(
                    pl.coalesce([
                        # Opcion A: Cortar los primeros 10 caracteres ("2026-02-16 08:31:21" -> "2026-02-16")
                        pl.col(c).cast(pl.Utf8).str.slice(0, 10).str.strptime(pl.Date, "%Y-%m-%d", strict=False),
                        
                        # Opcion B: Por si viene con formato de fecha y hora explícito
                        pl.col(c).cast(pl.Utf8).str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False).dt.date(),
                        
                        # Opcion C: Por si el Excel manda barras invertidas ("2026/02/16")
                        pl.col(c).cast(pl.Utf8).str.slice(0, 10).str.replace_all("/", "-").str.strptime(pl.Date, "%Y-%m-%d", strict=False)
                    ])
                )
    return df