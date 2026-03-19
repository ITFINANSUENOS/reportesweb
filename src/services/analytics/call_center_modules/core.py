import polars as pl

ALL_CALL_CENTERS = [f'CL{i}' for i in range(1, 10)]

def procesar_base_gestion(df: pl.DataFrame, df_novedades: pl.DataFrame):
    cols_num = ['Meta_General', 'Meta_$', 'Recaudo_Meta']
    df = df.with_columns([pl.col(c).cast(pl.Float64, strict=False).fill_null(0) for c in cols_num if c in df.columns])

    # 1. ZONA (Prioridad)
    filtro_zona = (pl.col("Zona").is_in(ALL_CALL_CENTERS) & (pl.col("Franja_Meta") == "AL DIA"))
    df_zona = df.filter(filtro_zona).with_columns(pl.col("Zona").alias("CALL_CENTER_FILTRO"))
    
    # 2. APOYO (Resto)
    df_apoyo = df.filter(pl.col("Call_Center_Apoyo").is_in(ALL_CALL_CENTERS)).filter(~filtro_zona).with_columns(pl.col("Call_Center_Apoyo").alias("CALL_CENTER_FILTRO"))

    # Normalización para cálculos
    df_zona_norm = df_zona.select([
        pl.col('Zona').alias('CALL_CENTER_ID'),
        pl.col('Cobrador').alias('NOMBRE_AGENTE'),
        pl.col('Meta_General').alias('META_UNIFICADA'),
        pl.col('Recaudo_Meta'), pl.col('Rodamiento'), pl.col('Cedula_Cliente')
    ])
    
    df_apoyo_norm = df_apoyo.select([
        pl.col('Call_Center_Apoyo').alias('CALL_CENTER_ID'),
        pl.col('Nombre_Call_Center').alias('NOMBRE_AGENTE'),
        pl.col('Meta_$').alias('META_UNIFICADA'),
        pl.col('Recaudo_Meta'), pl.col('Rodamiento'), pl.col('Cedula_Cliente')
    ])
    
    df_unificado = pl.concat([df_zona_norm, df_apoyo_norm])
    df_detalle = pl.concat([df_zona, df_apoyo])

    # Pegar Novedades y asegurar campos para filtros
    if not df_novedades.is_empty() and "Cedula_Cliente" in df_novedades.columns:
        cols_nov = [c for c in ['Cedula_Cliente', 'Tipo_Novedad', 'Novedad'] if c in df_novedades.columns]
        nov_simple = df_novedades.select(cols_nov).unique(subset=['Cedula_Cliente'], keep='last')
        df_detalle = df_detalle.join(nov_simple, on='Cedula_Cliente', how='left')
        df_detalle = df_detalle.with_columns([
            pl.col('Tipo_Novedad').fill_null('SIN NOVEDAD'),
            pl.col('Novedad').fill_null('')
        ])
    
    # Asegurar campos de filtro para frontend
    if "Cantidad_Novedades" not in df_detalle.columns:
        df_detalle = df_detalle.with_columns(pl.lit(0).cast(pl.Float64).alias("Cantidad_Novedades"))
    
    if "Estado_Gestion" not in df_detalle.columns:
        df_detalle = df_detalle.with_columns(
            pl.when(pl.col("Cantidad_Novedades") > 0).then(pl.lit("CON GESTIÓN"))
            .otherwise(pl.lit("SIN GESTIÓN")).alias("Estado_Gestion")
        )
    
    # Asegurar Estado_Vigencia si existe en df original
    if "Estado_Vigencia" not in df_detalle.columns and "Estado_Vigencia" in df.columns:
        df_detalle = df_detalle.with_columns(pl.col("Estado_Vigencia"))
    
    return df_unificado, df_detalle

def calcular_cumplimiento(df: pl.DataFrame):
    if df.is_empty(): return pl.DataFrame()
    agg = df.group_by(['CALL_CENTER_ID', 'NOMBRE_AGENTE']).agg([
        pl.col('META_UNIFICADA').sum().alias('META_$'),
        pl.col('Recaudo_Meta').sum().alias('Recaudo_Meta')
    ])
    return agg.with_columns([
        (pl.col('META_$') - pl.col('Recaudo_Meta')).alias('Faltante'),
        pl.when(pl.col('META_$') > 0).then(pl.col('Recaudo_Meta') / pl.col('META_$')).otherwise(0.0).alias('Cumplimiento')
    ]).rename({'CALL_CENTER_ID': 'CALL_CENTER', 'NOMBRE_AGENTE': 'NOMBRE'})

def calcular_rodamientos(df: pl.DataFrame):
    if df.is_empty(): return pl.DataFrame()
    return df.group_by('Rodamiento').len().rename({'len': 'count'})