import boto3
import polars as pl
import io
import os
import datetime
from dotenv import load_dotenv
from typing import BinaryIO, Dict, Any, List

from src.models.reporte_general import ReporteGneralSchema as Schema

load_dotenv()

class CloudStorageService:
    def __init__(self):
        # boto3 buscará AWS_ACCESS_KEY_ID y AWS_SECRET_ACCESS_KEY en las variables de entorno automáticamente
        self.bucket_name = os.getenv('S3_BUCKET_NAME')
        self.region = os.getenv('AWS_REGION', 'us-east-1')
        self.s3_client = boto3.client('s3', region_name=self.region)

    def procesar_y_subir_reporte(self, archivo_excel: BinaryIO) -> Dict[str, Any]:
        """
        Orquesta todo el proceso: Lee Excel -> Valida/Limpia -> Sube Parquets separados.
        """
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        resultados = {}
        errores = []

        print("⏳ Iniciando lectura del Excel (esto puede tardar un poco)...")
        
        # Leemos el Excel una sola vez en memoria para no depender del disco
        try:
            excel_file = io.BytesIO(archivo_excel.read())
        except Exception as e:
            raise ValueError(f"Error al leer el archivo binario: {e}")

        # 1. PROCESAR CARTERA (Crítico)
        try:
            print("   ↳ Procesando Cartera...")
            df_cartera = self._leer_hoja(excel_file, Schema.SHEET_CARTERA, Schema.COLS_CARTERA)
            
            if df_cartera is not None:
                # Limpieza específica de fechas
                cols_fechas = ["Fecha_Desembolso", "Fecha_Ultima_Novedad", "Fecha_Cuota_Atraso", "Primera_Cuota_Mora"]
                df_cartera = self._limpiar_fechas(df_cartera, cols_fechas)
                
                # Subir
                key = f"reportes/cartera/cartera_{timestamp}.parquet"
                self._subir_parquet(df_cartera, key)
                resultados["cartera"] = key
        except Exception as e:
            errores.append(f"Error en Cartera: {str(e)}")

        # 2. PROCESAR NOVEDADES (Crítico)
        try:
            print("   ↳ Procesando Novedades...")
            df_novedades = self._leer_hoja(excel_file, Schema.SHEET_NOVEDADES, Schema.COLS_NOVEDADES)
            
            if df_novedades is not None:
                cols_fechas_nov = ["Fecha_Novedad", "Fecha_Compromiso"]
                df_novedades = self._limpiar_fechas(df_novedades, cols_fechas_nov)
                
                key = f"reportes/novedades/novedades_{timestamp}.parquet"
                self._subir_parquet(df_novedades, key)
                resultados["novedades"] = key
        except Exception as e:
            errores.append(f"Error en Novedades: {str(e)}")

        # 3. PROCESAR LLAMADAS (Opcional)
        try:
            print("   ↳ Procesando Llamadas...")
            df_llamadas = self._leer_hoja(excel_file, Schema.SHEET_LLAMADAS, Schema.COLS_LLAMADAS)
            
            if df_llamadas is not None:
                if "Fecha_Llamada" in df_llamadas.columns:
                    df_llamadas = df_llamadas.with_columns(
                        pl.col("Fecha_Llamada").cast(pl.Date, strict=False)
                    )

                key = f"reportes/llamadas/llamadas_{timestamp}.parquet"
                self._subir_parquet(df_llamadas, key)
                resultados["llamadas"] = key
        except Exception as e:
            print(f"⚠️ Info: No se procesó Llamadas ({e})")

        # 4. PROCESAR MENSAJERÍA (Opcional)
        try:
            print("   ↳ Procesando Mensajería...")
            df_msj = self._leer_hoja(excel_file, Schema.SHEET_MENSAJES, Schema.COLS_MENSAJERIA)
            
            if df_msj is not None:
                if "Fecha_Llamada" in df_msj.columns:
                    df_msj = df_msj.with_columns(
                        pl.col("Fecha_Llamada").cast(pl.Date, strict=False)
                    )

                key = f"reportes/mensajeria/mensajeria_{timestamp}.parquet"
                self._subir_parquet(df_msj, key)
                resultados["mensajeria"] = key
        except Exception as e:
            print(f"⚠️ Info: No se procesó Mensajería ({e})")

        # 5. PROCESAR FNZ (Opcional)
        try:
            print("   ↳ Procesando FNZ...")
            # FNZ no tiene columnas fijas definidas en lista, leemos todo y renombramos
            try:
                df_fnz = pl.read_excel(excel_file, sheet_name=Schema.SHEET_FNZ)
            except Exception:
                # Si falla pl.read_excel suele ser porque la hoja no existe
                raise ValueError(f"Hoja {Schema.SHEET_FNZ} no encontrada")

            # Renombramos columnas usando tu MAPA_FNZ del modelo
            if Schema.MAPA_FNZ:
                # Filtramos el mapa para renombrar solo columnas que existen en el archivo
                mapa_valido = {k: v for k, v in Schema.MAPA_FNZ.items() if k in df_fnz.columns}
                df_fnz = df_fnz.rename(mapa_valido)
            
            if df_fnz is not None and not df_fnz.is_empty():
                 # Limpieza de strings
                df_fnz = df_fnz.with_columns(pl.col(pl.Utf8).str.strip_chars())
                
                key = f"reportes/fnz/fnz_{timestamp}.parquet"
                self._subir_parquet(df_fnz, key)
                resultados["fnz"] = key

        except Exception as e:
            print(f"⚠️ No se procesó FNZ: {e}")

        # 6. LIMPIEZA DE ANTIGUOS
        self._ejecutar_rotacion()

        return {"archivos_generados": resultados, "errores": errores}

    # MÉTODOS PRIVADOS (HELPERS)

    def _leer_hoja(self, excel_file, hoja: str, columnas_requeridas: List[str]) -> pl.DataFrame:
        """Lee una hoja específica y valida/selecciona columnas"""
        try:
            df = pl.read_excel(excel_file, sheet_name=hoja)
            
            # Validación: Intersección de columnas existentes vs requeridas
            cols_existentes = set(df.columns)
            cols_req = set(columnas_requeridas)
            
            # Seleccionamos solo las que existen para evitar errores si falta alguna no crítica
            cols_finales = list(cols_req.intersection(cols_existentes))
            
            # Si faltan muchas o todas, el dataframe quedará vacío o con pocas columnas.
            if not cols_finales:
                raise ValueError(f"La hoja '{hoja}' no contiene ninguna de las columnas esperadas.")

            df = df.select(cols_finales)
                
            # Limpieza básica de strings (strip) en todo el DF
            df = df.with_columns(pl.col(pl.Utf8).str.strip_chars())
            
            return df
        except Exception as e:
            # Re-lanzamos para que lo capture el bloque try-except principal
            raise ValueError(f"No se pudo leer la hoja '{hoja}': {e}")

    def _limpiar_fechas(self, df: pl.DataFrame, columnas_fechas: List[str]) -> pl.DataFrame:
        """Convierte columnas de texto a fecha, manejando errores (strict=False)"""
        for col in columnas_fechas:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col).cast(pl.Date, strict=False)
                )
        return df

    def _subir_parquet(self, df: pl.DataFrame, key_s3: str):
        """Convierte a Parquet en RAM y sube a S3"""
        buffer = io.BytesIO()
        df.write_parquet(buffer)
        buffer.seek(0)
        self.s3_client.upload_fileobj(buffer, self.bucket_name, key_s3)
        print(f"   ✅ Subido a S3: {key_s3}")

    def _ejecutar_rotacion(self):
        """Borra archivos viejos de cada carpeta (Mantiene los 3 últimos)"""
        print("🧹 Ejecutando limpieza de archivos antiguos...")
        carpetas = [
            "reportes/cartera/", 
            "reportes/novedades/", 
            "reportes/fnz/",
            "reportes/llamadas/",
            "reportes/mensajeria/"
        ]
        for carpeta in carpetas:
            self._eliminar_antiguos(prefix=carpeta)

    def _eliminar_antiguos(self, prefix: str, mantener: int = 3):
        """Implementación real de la rotación en S3"""
        try:
            # 1. Listar objetos
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
            
            if 'Contents' not in response:
                return

            # 2. Ordenar por fecha de modificación (Más nuevo primero)
            archivos = sorted(
                response['Contents'],
                key=lambda x: x['LastModified'],
                reverse=True
            )

            # 3. Borrar el exceso
            if len(archivos) > mantener:
                archivos_a_borrar = archivos[mantener:]
                print(f"   🗑️ Eliminando {len(archivos_a_borrar)} archivos antiguos en '{prefix}'")
                
                for obj in archivos_a_borrar:
                    self.s3_client.delete_object(Bucket=self.bucket_name, Key=obj['Key'])
                    
        except Exception as e:
            print(f"⚠️ Error al intentar limpiar archivos antiguos en {prefix}: {e}")