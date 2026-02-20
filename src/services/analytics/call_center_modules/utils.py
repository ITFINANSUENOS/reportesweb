import polars as pl

def agregar_columnas_calculadas(df: pl.DataFrame) -> pl.DataFrame:
    cols = df.columns
    exprs = []
    
    if "Total_Recaudo" in cols:
        exprs.append(pl.when(pl.col("Total_Recaudo") > 50000).then(pl.lit("PAGO")).otherwise(pl.lit("SIN PAGO")).alias("Estado_Pago"))
    else:
        exprs.append(pl.lit("SIN DATO").alias("Estado_Pago"))
        
    if "Cantidad_Novedades" in cols:
        exprs.append(pl.when(pl.col("Cantidad_Novedades") > 0).then(pl.lit("CON GESTIÓN")).otherwise(pl.lit("SIN GESTIÓN")).alias("Estado_Gestion"))
    else:
        exprs.append(pl.lit("SIN DATO").alias("Estado_Gestion"))
        
    return df.with_columns(exprs)

def exportar_a_json(df: pl.DataFrame) -> list:
    if df is None or df.is_empty():
        return []
    return df.with_columns(pl.col(pl.Date).dt.to_string("%Y-%m-%d")).to_dicts()
