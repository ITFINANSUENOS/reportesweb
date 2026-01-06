import polars as pl

class SeguimientosAnalyticsService:
    
    def calcular_metricas_seguimientos(self, df_cartera: pl.DataFrame, df_novedades: pl.DataFrame) -> dict:
        print("📊 ANALYTICS: Calculando métricas de Seguimientos...")
        
        if df_cartera.is_empty():
            return {}

        # 1. PREPARAR BASE (Lógica de negocio existente)
        df_base = df_cartera 
        
        # Validamos columnas auxiliares
        col_vigencia = pl.col("Tipo_Vigencia_Temp") if "Tipo_Vigencia_Temp" in df_base.columns else pl.lit("NORMAL")

        df_base = df_base.with_columns([
            pl.when(col_vigencia == "ANTICIPADO").then(pl.lit("ANTICIPADO"))
            .when(pl.col("Total_Recaudo") > 50000).then(pl.lit("PAGO"))
            .otherwise(pl.lit("SIN PAGO")).alias("Estado_Pago"),

            pl.when(pl.col("Cantidad_Novedades") > 0).then(pl.lit("CON GESTIÓN"))
            .otherwise(pl.lit("SIN GESTIÓN")).alias("Estado_Gestion")
        ])

        # --- CALCULOS DE GRÁFICOS (DONA, SUNBURST, BARRAS) ---
        # (Esta parte la mantenemos igual porque funciona bien para los gráficos)
        cols_filtro = [c for c in ["Empresa", "Regional_Cobro", "Zona", "Franja_Cartera", "CALL_CENTER_FILTRO", "Regional_Venta"] if c in df_base.columns]
        
        agg_donut = df_base.group_by(cols_filtro + ["Estado_Pago"]).len().rename({"len": "count"})
        
        agg_rodamiento = pl.DataFrame()
        if "Rodamiento" in df_base.columns:
            agg_rodamiento = df_base.group_by(cols_filtro + ["Rodamiento", "Estado_Gestion"]).len().rename({"len": "Número de Cuentas"}).sort("Rodamiento")

        # Cruce para Sunburst (necesita Cargo_Usuario)
        if not df_novedades.is_empty() and "Cedula_Cliente" in df_novedades.columns and "Cargo_Usuario" in df_novedades.columns:
            cargos_unicos = df_novedades.select(["Cedula_Cliente", "Cargo_Usuario"]).unique()
        else:
            cargos_unicos = pl.DataFrame(schema={"Cedula_Cliente": pl.Utf8, "Cargo_Usuario": pl.Utf8})

        df_merged = df_base.join(cargos_unicos, on="Cedula_Cliente", how="left").with_columns(pl.col("Cargo_Usuario").fill_null("SIN ASIGNAR"))

        grouped_sunburst = df_merged.filter(
            ~((pl.col("Estado_Gestion") == "CON GESTIÓN") & (pl.col("Cargo_Usuario") == "SIN ASIGNAR"))
        ).group_by(cols_filtro + ["Estado_Gestion", "Cargo_Usuario"]).len().rename({"len": "Cantidad"}).sort("Cantidad", descending=True)

        grouped_pago = df_merged.filter(pl.col("Estado_Pago") == "PAGO").filter(
            ~((pl.col("Estado_Gestion") == "CON GESTIÓN") & (pl.col("Cargo_Usuario") == "SIN ASIGNAR"))
        ).group_by(cols_filtro + ["Estado_Gestion", "Cargo_Usuario"]).len().rename({"len": "Cantidad"})

        grouped_sin_pago = df_merged.filter(pl.col("Estado_Pago") == "SIN PAGO").filter(
            ~((pl.col("Estado_Gestion") == "CON GESTIÓN") & (pl.col("Cargo_Usuario") == "SIN ASIGNAR"))
        ).group_by(cols_filtro + ["Estado_Gestion", "Cargo_Usuario"]).len().rename({"len": "Cantidad"})


        # --- CREACIÓN DE LA TABLA MAESTRA DETALLADA ---
        # Aquí es donde garantizamos que TODOS los campos lleguen al final
        
        df_tabla_final = df_merged # Ya tiene Cartera + Estado_Pago + Estado_Gestion + Cargo_Usuario

        if not df_novedades.is_empty():
            # 1. Contar novedades
            novedades_por_cargo = df_novedades.group_by(["Cedula_Cliente", "Cargo_Usuario"]).len().rename({"len": "Novedades_Por_Cargo"})
            
            # Unimos el conteo
            df_tabla_final = df_tabla_final.join(novedades_por_cargo, on=["Cedula_Cliente", "Cargo_Usuario"], how="left").with_columns(pl.col("Novedades_Por_Cargo").fill_null(0))
            
            # 2. Traer el detalle de la última novedad (o todas, segun lógica)
            # Para la tabla, necesitamos columnas que SOLO están en Novedades (Ej: Novedad, Tipo_Novedad, Nombre_Usuario)
            
            # Lista de columnas de Novedades que queremos pegar a la tabla final
            # Excluimos las que ya usamos para el join
            cols_nov_extra = [c for c in df_novedades.columns if c not in ["Cedula_Cliente", "Cargo_Usuario", "Empresa"]]
            
            df_nov_subset = df_novedades.select(["Cedula_Cliente", "Cargo_Usuario"] + cols_nov_extra)
            
            # Hacemos el Join. IMPORTANTE: Esto puede duplicar filas si un cliente tiene múltiples novedades con el mismo cargo.
            # Si tu lógica original en Python hacía pd.merge(..., how='left'), hacía lo mismo (expandía filas).
            df_tabla_final = df_tabla_final.join(df_nov_subset, on=["Cedula_Cliente", "Cargo_Usuario"], how="left", coalesce=True)
            
            # Llenamos nulos estéticos
            cols_texto_rellenar = ["Novedad", "Tipo_Novedad", "Nombre_Usuario"]
            for c in cols_texto_rellenar:
                if c in df_tabla_final.columns:
                    df_tabla_final = df_tabla_final.with_columns(pl.col(c).fill_null(""))

        else:
            df_tabla_final = df_tabla_final.with_columns([
                pl.lit(0).alias("Novedades_Por_Cargo"), 
                pl.lit("").alias("Novedad"), 
                pl.lit("").alias("Tipo_Novedad")
            ])

        return {
            "donut_data": agg_donut.to_dicts(),
            "sunburst_grouped": grouped_sunburst.to_dicts(),
            "detalle_pago": { "grouped": grouped_pago.to_dicts(), "counts": [] },
            "detalle_sin_pago": { "grouped": grouped_sin_pago.to_dicts(), "counts": [] },
            "rodamiento_data": agg_rodamiento.to_dicts() if not agg_rodamiento.is_empty() else [],
            
            # Retornamos df_tabla_final COMPLETO para que el DataProcessor lo guarde
            "_df_novedades_full": df_tabla_final, 
            "_df_cartera_base": df_base  
        }