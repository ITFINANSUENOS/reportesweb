import polars as pl

class SeguimientosAnalyticsService:
    
    def calcular_metricas_seguimientos(self, df_cartera: pl.DataFrame, df_novedades: pl.DataFrame) -> dict:
        print("📊 ANALYTICS: Calculando métricas de Seguimientos...")
        
        if df_cartera.is_empty():
            return {}

        # 1. PREPARAR BASE
        col_vigencia = pl.col("Tipo_Vigencia_Temp") if "Tipo_Vigencia_Temp" in df_cartera.columns else pl.lit("NORMAL")

        df_base = df_cartera.with_columns([
            pl.when(col_vigencia == "ANTICIPADO").then(pl.lit("ANTICIPADO"))
            .when(pl.col("Total_Recaudo") > 50000).then(pl.lit("PAGO"))
            .otherwise(pl.lit("SIN PAGO")).alias("Estado_Pago"),

            pl.when(pl.col("Cantidad_Novedades") > 0).then(pl.lit("CON GESTIÓN"))
            .otherwise(pl.lit("SIN GESTIÓN")).alias("Estado_Gestion")
        ])

        # --- A. CÁLCULOS PARA GRÁFICOS ---
        # Filtros dinámicos disponibles
        posibles_filtros = ["Empresa", "Regional_Cobro", "Zona", "Franja_Cartera", "CALL_CENTER_FILTRO", "Regional_Venta"]
        cols_filtro = [c for c in posibles_filtros if c in df_base.columns]
        
        # 1. Dona (Recaudo)
        agg_donut = df_base.group_by(cols_filtro + ["Estado_Pago"]).len().rename({"len": "count"})
        
        # 2. Barras (Rodamiento) - DATA CRUDA SIN CRUCE
        # Esta es la lógica que pediste: Usamos df_base directo (1 fila por crédito)
        agg_rodamiento = pl.DataFrame()
        if "Rodamiento" in df_base.columns:
            agg_rodamiento = (
                df_base
                .filter(pl.col("Rodamiento").is_not_null() & (pl.col("Rodamiento") != ""))
                .group_by(cols_filtro + ["Rodamiento", "Estado_Gestion"])
                .len()
                .rename({"len": "Número de Cuentas"})
                .sort("Rodamiento")
            )

        # 3. Sunburst (Gestión y Asignación) - DATA CRUZADA
        # Aquí sí necesitamos el cruce para ver "Cargo_Usuario"
        if not df_novedades.is_empty() and "Cedula_Cliente" in df_novedades.columns and "Cargo_Usuario" in df_novedades.columns:
            cargos_unicos = df_novedades.select(["Cedula_Cliente", "Cargo_Usuario"]).unique()
        else:
            cargos_unicos = pl.DataFrame(schema={"Cedula_Cliente": pl.Utf8, "Cargo_Usuario": pl.Utf8})

        df_merged = df_base.join(cargos_unicos, on="Cedula_Cliente", how="left").with_columns(
            pl.col("Cargo_Usuario").fill_null("SIN ASIGNAR")
        )

        # Filtro para sunburst válido
        filtro_valido = ~((pl.col("Estado_Gestion") == "CON GESTIÓN") & (pl.col("Cargo_Usuario") == "SIN ASIGNAR"))

        grouped_sunburst = (
            df_merged.filter(filtro_valido)
            .group_by(cols_filtro + ["Estado_Gestion", "Cargo_Usuario"])
            .len()
            .rename({"len": "Cantidad"})
            .sort("Cantidad", descending=True)
        )

        grouped_pago = (
            df_merged.filter(pl.col("Estado_Pago") == "PAGO")
            .filter(filtro_valido)
            .group_by(cols_filtro + ["Estado_Gestion", "Cargo_Usuario"])
            .len()
            .rename({"len": "Cantidad"})
        )

        grouped_sin_pago = (
            df_merged.filter(pl.col("Estado_Pago") == "SIN PAGO")
            .filter(filtro_valido)
            .group_by(cols_filtro + ["Estado_Gestion", "Cargo_Usuario"])
            .len()
            .rename({"len": "Cantidad"})
        )


        # --- B. TABLA MAESTRA DETALLADA (PARQUET) ---        
        df_tabla_final = df_merged

        if not df_novedades.is_empty():
            # Conteo
            novedades_por_cargo = (
                df_novedades.group_by(["Cedula_Cliente", "Cargo_Usuario"])
                .len()
                .rename({"len": "Novedades_Por_Cargo"})
            )
            df_tabla_final = df_tabla_final.join(
                novedades_por_cargo, on=["Cedula_Cliente", "Cargo_Usuario"], how="left"
            ).with_columns(pl.col("Novedades_Por_Cargo").fill_null(0))
            
            # Detalle
            cols_excluir = ["Cedula_Cliente", "Cargo_Usuario", "Empresa"]
            cols_nov_extra = [c for c in df_novedades.columns if c not in cols_excluir]
            df_nov_subset = df_novedades.select(["Cedula_Cliente", "Cargo_Usuario"] + cols_nov_extra)
            
            df_tabla_final = df_tabla_final.join(
                df_nov_subset, on=["Cedula_Cliente", "Cargo_Usuario"], how="left", coalesce=True
            )
            
            for c in ["Novedad", "Tipo_Novedad", "Nombre_Usuario"]:
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
            # Dataframes para Parquet
            "_df_novedades_full": df_tabla_final, 
            "_df_cartera_base": df_base 
        }