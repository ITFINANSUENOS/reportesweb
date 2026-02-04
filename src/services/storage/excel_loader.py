import polars as pl
from src.core.constants import (
    COLS_CARTERA, COLS_NOVEDADES, COLS_LLAMADAS, 
    COLS_MENSAJERIA, MAPA_FNZ
)
from src.utils.polars_utils import leer_hoja_excel, limpiar_texto_lote, parsear_fechas

class ExcelLoaderService:
    
    def cargar_cartera(self, file_path: str) -> pl.DataFrame:
        overrides = {
            "Valor_Desembolso": pl.Float64, "Valor_Cuota": pl.Float64, 
            "Valor_Cuota_Atraso": pl.Float64, "Total_Recaudo": pl.Float64,
            "Valor_Cuota_Vigente": pl.Utf8, "Valor_Vencido": pl.Float64,
            "Cedula_Cliente": pl.Utf8, "Celular": pl.Utf8, "Cobrador": pl.Utf8,
            "Telefono_Cobrador": pl.Utf8, "Telefono_Gestor": pl.Utf8,
            "Telefono_Codeudor1": pl.Utf8, "Telefono_Codeudor2": pl.Utf8,
            "Credito": pl.Utf8, "Movil_Lider": pl.Utf8,
            "Cantidad_Novedades": pl.Float64, "Meta_General": pl.Float64, 
            "Meta_$": pl.Float64, "Meta_Saldo": pl.Float64, 
            "Recaudo_Meta": pl.Float64, "Meta_Intereses": pl.Float64,
            "Meta_T.R_$": pl.Float64, "Total_Recaudo_Sin_Anti": pl.Float64 
        }
        
        df = leer_hoja_excel(file_path, "Analisis_de_Cartera", COLS_CARTERA, overrides)

        if not df.is_empty():
            # Limpieza específica de Cartera
            if "Valor_Cuota_Vigente" in df.columns:
                df = df.with_columns(
                    pl.when(pl.col("Valor_Cuota_Vigente").str.to_uppercase().str.contains("ANTICIPADO"))
                    .then(pl.lit("ANTICIPADO")).otherwise(pl.lit("NORMAL")).alias("Tipo_Vigencia_Temp")
                )
                df = df.with_columns(
                    pl.col("Valor_Cuota_Vigente").str.replace("(?i)ANTICIPADO", "0")
                      .str.replace(",", "").cast(pl.Float64, strict=False).fill_null(0)
                )
            else:
                df = df.with_columns(pl.lit("NORMAL").alias("Tipo_Vigencia_Temp"))
            
            df = parsear_fechas(df, ["Fecha_Desembolso", "Fecha_Ultima_Novedad", "Fecha_Cuota_Atraso", "Fecha_Cuota_Vigente"])
            df = limpiar_texto_lote(df, ["Empresa", "Regional_Venta", "Nombre_Ciudad", "Nombre_Vendedor", "Franja_Meta", "Rodamiento", "Regional_Cobro", "Zona", "Cedula_Cliente"])
            
            if "Cantidad_Novedades" in df.columns:
                df = df.with_columns(pl.col("Cantidad_Novedades").fill_null(0))
            
            # Normalización de Zonas y Call Center
            df = self._limpiar_zonas_y_callcenter(df)
            
        return df

    def cargar_novedades(self, file_path: str) -> pl.DataFrame:
        overrides = {
            "Celular_Cliente": pl.Utf8, "Telefono_Cliente": pl.Utf8, 
            "Cedula_Cliente": pl.Utf8, "Valor": pl.Float64, "Novedad": pl.Utf8
        }
        df = leer_hoja_excel(file_path, "Detalle_Novedades", COLS_NOVEDADES, overrides)

        if not df.is_empty():
            if "Celular_Cliente" in df.columns: 
                df = df.with_columns(pl.col("Celular_Cliente").str.replace(r"\.$", ""))
            
            df = parsear_fechas(df, ["Fecha_Novedad", "Fecha_Compromiso"])
            df = limpiar_texto_lote(df, ["Cedula_Cliente"])
            
        return df

    def cargar_llamadas(self, file_path: str) -> pl.DataFrame:
        overrides = {"Destino_Llamada": pl.Utf8, "Extension_Llamada": pl.Utf8, "Codigo_Llamada": pl.Utf8}
        df = leer_hoja_excel(file_path, "Reporte_Llamadas", COLS_LLAMADAS, overrides)
        if not df.is_empty():
            df = parsear_fechas(df, ["Fecha_Llamada"])
        return df

    def cargar_mensajeria(self, file_path: str) -> pl.DataFrame:
        overrides = {"Fecha_Llamada": pl.Utf8, "Numero_Telefono": pl.Utf8}
        df = leer_hoja_excel(file_path, "Reporte_Mensajes", COLS_MENSAJERIA, overrides)
        if not df.is_empty():
            df = parsear_fechas(df, ["Fecha_Llamada"])
        return df

    def cargar_fnz(self, file_path: str) -> pl.DataFrame:
        overrides = {
            "PAGARE": pl.Utf8, "CEDULA": pl.Utf8, "TELEFONO1": pl.Utf8, 
            "MOVIL": pl.Utf8, "VALOR_TOTA": pl.Float64, "DESEMBOLSO": pl.Utf8
        }
        df = leer_hoja_excel(file_path, "FNZ007", list(MAPA_FNZ.keys()), overrides)
        if not df.is_empty():
            df = df.rename({k:v for k,v in MAPA_FNZ.items() if k in df.columns})
            df = parsear_fechas(df, ["Fecha_Nacimiento"])
        return df

    def _limpiar_zonas_y_callcenter(self, df_cartera: pl.DataFrame) -> pl.DataFrame:
        """Normaliza columnas de zona y call center."""
        try:
            defaults = {"Zona": "SIN ZONA", "Call_Center_Apoyo": "SIN APOYO"}
            for col, val in defaults.items():
                if col not in df_cartera.columns:
                    df_cartera = df_cartera.with_columns(pl.lit(val).alias(col))

            df_cartera = df_cartera.with_columns([
                pl.col("Zona").cast(pl.Utf8).str.replace_all(" ", "").str.strip_chars().str.to_uppercase(),
                pl.col("Call_Center_Apoyo").cast(pl.Utf8).str.replace_all(" ", "").str.strip_chars().str.to_uppercase()
            ])

            zonas_cl = ['CL1', 'CL2', 'CL3', 'CL4']
            apoyo_cl = ['CL5', 'CL6', 'CL7', 'CL8', 'CL9']

            df_cartera = df_cartera.with_columns(
                pl.when(pl.col("Zona").is_in(zonas_cl)).then(pl.col("Zona"))
                .when(pl.col("Call_Center_Apoyo").is_in(apoyo_cl)).then(pl.col("Call_Center_Apoyo"))
                .otherwise(pl.lit("SIN CALL CENTER")).alias("CALL_CENTER_FILTRO"),
                
                pl.col("Regional_Cobro").fill_null("OTRAS ZONAS")
            )
            return df_cartera
        except Exception as e:
            print(f"⚠️ Error limpiando zonas (no crítico): {e}")
            return df_cartera