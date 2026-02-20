import polars as pl
from .utils import exportar_a_json

def procesar_mensajeria(df_msj: pl.DataFrame, df_nov: pl.DataFrame, df_cartera: pl.DataFrame) -> dict:
    stats_default = {"df_funnel_mensajeria": [], "df_efectividad_mensajeria": []}

    if df_msj is None or df_msj.is_empty(): return stats_default

    try:
        # --- FILTRO: Ignorar Call Centers nulos o sin dato ---
        if "Call_Center" in df_msj.columns:
            df_msj = df_msj.filter(
                pl.col("Call_Center").is_not_null() & 
                (pl.col("Call_Center").cast(pl.Utf8).str.to_uppercase() != "SIN DATO") &
                (pl.col("Call_Center").cast(pl.Utf8) != "")
            )

        if df_msj.is_empty(): return stats_default

        def norm_phone_expr(col_name):
            return pl.col(col_name).cast(pl.Utf8).str.replace(r"\.0$", "").str.strip_chars()

        # Paso 1: Identificar Conversaciones
        if "Tipo_Respuesta_Agente" in df_msj.columns:
            df_msj = df_msj.with_columns(
                pl.col("Tipo_Respuesta_Agente").cast(pl.Utf8).str.to_lowercase().str.strip_chars()
                .is_in(["text", "audio"]).fill_null(False).alias("Es_Conversacion")
            )
        else:
            df_msj = df_msj.with_columns(pl.lit(False).alias("Es_Conversacion"))

        total_mensajes = df_msj.height
        df_conversaciones = df_msj.filter(pl.col("Es_Conversacion"))
        total_conversaciones = df_conversaciones.height

        # --- Paso 2: Efectividad por Call Center ---
        df_efectividad_mensajeria = []
        if "Call_Center" in df_msj.columns:
            agg_msgs = df_msj.group_by("Call_Center").agg([
                pl.len().alias("entregados"),
                pl.col("Es_Conversacion").cast(pl.Int32).sum().alias("conversaciones")
            ]).with_columns(
                pl.when(pl.col("entregados") > 0)
                .then(pl.col("conversaciones") / pl.col("entregados") * 100)
                .otherwise(0.0).alias("porcentaje")
            ).sort("entregados", descending=True).rename({"Call_Center": "name"})
            
            df_efectividad_mensajeria = exportar_a_json(agg_msgs)

        # Paso 3: Gestión en Sistema (Cruce Teléfonos)
        total_gestion = 0
        df_gestion_final = pl.DataFrame()
        if total_conversaciones > 0 and not df_nov.is_empty() and "Numero_Telefono" in df_conversaciones.columns:
            df_conversaciones = df_conversaciones.with_columns(norm_phone_expr("Numero_Telefono").alias("Tel_Norm"))
            tels_nov = []
            if "Telefono_Cliente" in df_nov.columns: tels_nov.append(df_nov.select(norm_phone_expr("Telefono_Cliente").alias("Tel")))
            if "Celular_Cliente" in df_nov.columns: tels_nov.append(df_nov.select(norm_phone_expr("Celular_Cliente").alias("Tel")))
            
            if tels_nov:
                df_tels_validos = pl.concat(tels_nov).filter(pl.col("Tel").is_not_null() & (pl.col("Tel") != "")).unique()
                df_gestion_final = df_conversaciones.join(df_tels_validos, left_on="Tel_Norm", right_on="Tel", how="inner")
                total_gestion = df_gestion_final.height

        # Paso 4: Clientes con Pago (Cruce Cédula)
        total_pago = 0
        if total_gestion > 0 and not df_cartera.is_empty():
            condicion_pago = pl.lit(False)
            if "Estado_Pago" in df_cartera.columns: condicion_pago = pl.col("Estado_Pago") == "PAGO"
            elif "Total_Recaudo" in df_cartera.columns: condicion_pago = pl.col("Total_Recaudo") > 0 
            
            df_pagos = df_cartera.filter(condicion_pago).select(pl.col("Cedula_Cliente").cast(pl.Utf8)).unique()
            mapas = []
            if "Cedula_Cliente" in df_nov.columns:
                df_nov_map = df_nov.filter(pl.col("Cedula_Cliente").is_not_null())
                if "Telefono_Cliente" in df_nov.columns: mapas.append(df_nov_map.select([norm_phone_expr("Telefono_Cliente").alias("Tel"), pl.col("Cedula_Cliente").cast(pl.Utf8)]))
                if "Celular_Cliente" in df_nov.columns: mapas.append(df_nov_map.select([norm_phone_expr("Celular_Cliente").alias("Tel"), pl.col("Cedula_Cliente").cast(pl.Utf8)]))
            
            if not df_pagos.is_empty() and mapas:
                df_mapa_tel_cedula = pl.concat(mapas).filter(pl.col("Tel").is_not_null() & (pl.col("Tel") != "")).unique()
                df_pagos_final = df_gestion_final.join(df_mapa_tel_cedula, left_on="Tel_Norm", right_on="Tel", how="inner").join(df_pagos, on="Cedula_Cliente", how="inner")
                total_pago = df_pagos_final.height

        df_funnel_mensajeria = [
            {'name': 'Mensajes Entregados', 'value': total_mensajes},
            {'name': 'Conversaciones', 'value': total_conversaciones},
            {'name': 'Gestión en Sistema', 'value': total_gestion},
            {'name': 'Clientes con Pago', 'value': total_pago}
        ]

        return {
            "df_funnel_mensajeria": df_funnel_mensajeria,
            "df_efectividad_mensajeria": df_efectividad_mensajeria
        }
    except Exception as e:
        print(f"❌ Error procesando mensajería: {e}")
        return stats_default