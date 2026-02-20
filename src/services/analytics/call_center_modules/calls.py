import polars as pl
from .utils import exportar_a_json

def procesar_llamadas(df: pl.DataFrame) -> dict:
    stats_default = {
        "llamadas_stats": {"total_llamadas": 0, "con_respuesta": 0, "sin_respuesta": 0},
        "df_grafico_llamadas": [],
        "df_efectividad_call": [],
        "df_llamadas_por_dia": [],
        "alerta_umbral": 0
    }
    
    if df is None or df.is_empty(): 
        return stats_default

    # --- EL FILTRO QUE NECESITAMOS ---
    # Ignoramos "SIN DATO" desde el inicio para que todo cuadre
    if 'Call_Center_Limpio' in df.columns:
        df = df.filter(pl.col('Call_Center_Limpio') != 'SIN DATO')

    # 1. Stats Generales
    if 'Estado_Llamada' in df.columns:
        df = df.with_columns(pl.col('Estado_Llamada').cast(pl.Utf8).str.to_uppercase().str.strip_chars())
        total = df.height
        contestadas = df.filter(pl.col('Estado_Llamada') == 'ANSWERED').height
        sin_respuesta = total - contestadas
    else:
        total, contestadas, sin_respuesta = df.height, 0, df.height
        
    df_grafico_llamadas = [
        {"Tipo": "CON RESPUESTA", "Cantidad": contestadas},
        {"Tipo": "SIN RESPUESTA", "Cantidad": sin_respuesta}
    ]
    
    # 2. Efectividad y Umbral (Ahora dará 270 si hay 9 centros)
    df_efectividad_call = []
    alerta_umbral = 0
    if 'Call_Center_Limpio' in df.columns:
        n_call_centers = df.select('Call_Center_Limpio').n_unique()
        alerta_umbral = n_call_centers * 30
        
        agg = df.group_by("Call_Center_Limpio").agg([
            pl.len().alias('Total_Intentos'),
            (pl.col('Estado_Llamada') == 'ANSWERED').sum().alias('Con_Respuesta')
        ]).with_columns(
            (pl.col('Con_Respuesta') / pl.col('Total_Intentos')).alias('Efectividad')
        ).sort('Efectividad', descending=True).rename({"Call_Center_Limpio": "Call_Center"})
        df_efectividad_call = exportar_a_json(agg)

    # 3. Tendencia Diaria (Volvemos al formato que tu Frontend SÍ entiende)
    df_llamadas_por_dia = []
    col_fecha = next((c for c in df.columns if c.strip() == 'Fecha_Llamada'), None)
    
    if col_fecha:
        try:
            # Reutilizamos tu lógica de fechas que ya era exitosa
            df_fechas = df.with_columns(
                pl.coalesce([
                    pl.col(col_fecha).cast(pl.Date, strict=False),
                    pl.col(col_fecha).cast(pl.Utf8).str.strptime(pl.Date, "%Y-%m-%d", strict=False)
                ]).alias('Fecha')
            ).filter(pl.col('Fecha').is_not_null())
            
            if not df_fechas.is_empty():
                # Formato exacto que espera tu callCenterLogic.js
                df_fechas = df_fechas.with_columns(
                    pl.when(pl.col('Estado_Llamada') == 'ANSWERED')
                    .then(pl.lit('CON RESPUESTA'))
                    .otherwise(pl.lit('SIN RESPUESTA'))
                    .alias('Estado_Respuesta')
                )
                
                agg_tendencia = df_fechas.group_by(['Fecha', 'Estado_Respuesta']).len().rename({'len': 'Total_Llamadas'})
                
                # Muy importante: La fecha como String para el JSON
                agg_tendencia = agg_tendencia.with_columns(pl.col('Fecha').cast(pl.Utf8))
                
                df_llamadas_por_dia = exportar_a_json(agg_tendencia)
                
        except Exception as e: 
            print(f"⚠️ Error procesando fechas llamadas: {e}")

    return {
        "llamadas_stats": {"total_llamadas": total, "con_respuesta": contestadas, "sin_respuesta": sin_respuesta}, 
        "df_grafico_llamadas": df_grafico_llamadas,
        "df_efectividad_call": df_efectividad_call, 
        "df_llamadas_por_dia": df_llamadas_por_dia,
        "alerta_umbral": alerta_umbral
    } 