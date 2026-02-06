import polars as pl

class ResultadosAnalyticsService:
    
    def calcular_metricas_resultados(self, df: pl.DataFrame) -> dict:
        """
        Calcula las métricas para el Tablero de Resultados.
        Retorna dos datasets: 'resultados_zona' y 'resultados_cobrador'.
        """
        print("📊 ANALYTICS: Calculando métricas de Resultados...")

        if df.is_empty():
            return {}

        # 1. FILTROS GLOBALES
        franjas_a_usar = ['1 A 30', '31 A 90', '91 A 180', '181 A 360']
        zonas_a_excluir = ['CL1', 'CL2', 'CL3', 'CL4']

        # Verificar columnas críticas
        if "Franja_Meta" not in df.columns or "Zona" not in df.columns:
            print("⚠️ Faltan columnas críticas (Franja_Meta o Zona) en el DataFrame.")
            return {}

        df_filtrado = df.filter(
            (pl.col("Franja_Meta").is_in(franjas_a_usar)) &
            (~pl.col("Zona").is_in(zonas_a_excluir))
        )

        if df_filtrado.is_empty():
            return {"resultados_zona": [], "resultados_cobrador": []}

        # 2. ASEGURAR COLUMNAS NUMÉRICAS Y CATEGÓRICAS PARA FILTROS
        cols_numericas = ['Meta_$', 'Recaudo_Meta', 'Total_Recaudo_Sin_Anti', 'Meta_T.R_$']
        
        # A. Rellenar numéricos
        for col in cols_numericas:
            if col not in df_filtrado.columns:
                df_filtrado = df_filtrado.with_columns(pl.lit(0.0).alias(col))
            else:
                df_filtrado = df_filtrado.with_columns(pl.col(col).fill_null(0.0))

        # B. Asegurar columnas de FILTROS (Empresa, Call Center)
        if "Empresa" not in df_filtrado.columns:
            df_filtrado = df_filtrado.with_columns(pl.lit("SIN EMPRESA").alias("Empresa"))
        
        if "CALL_CENTER_FILTRO" not in df_filtrado.columns:
            if "Call_Center_Apoyo" in df_filtrado.columns:
                 df_filtrado = df_filtrado.with_columns(pl.col("Call_Center_Apoyo").alias("CALL_CENTER_FILTRO"))
            else:
                 df_filtrado = df_filtrado.with_columns(pl.lit("SIN CALL CENTER").alias("CALL_CENTER_FILTRO"))

        if "Regional_Cobro" not in df_filtrado.columns:
             df_filtrado = df_filtrado.with_columns(pl.lit("OTRAS ZONAS").alias("Regional_Cobro"))


        # 3. DEFINIR COLUMNAS DE AGRUPACIÓN (INCLUYENDO LOS FILTROS)
        cols_group_base = [
            'Empresa', 
            'CALL_CENTER_FILTRO', 
            'Regional_Cobro', 
            'Zona'
        ]

        # 4. AGREGACIÓN 1: POR ZONA Y FRANJA
        group_cols_zona = cols_group_base + ['Franja_Meta']
        
        resultados_zona = (
            df_filtrado.group_by(group_cols_zona)
            .agg([
                pl.col("Meta_$").sum().alias("Meta_Total"),
                pl.col("Recaudo_Meta").sum().alias("Recaudo_Total"),
                pl.col("Total_Recaudo_Sin_Anti").sum().alias("Recaudo_Sin_Anti_Total"),
                pl.col("Meta_T.R_$").sum().alias("Recaudo_Meta_Total")
            ])
            .with_columns(
                pl.when(pl.col("Meta_Total") > 0)
                .then(pl.col("Recaudo_Total") / pl.col("Meta_Total"))
                .otherwise(0.0)
                .alias("Cumplimiento_%")
            )
        )

        # 5. AGREGACIÓN 2: POR COBRADOR
        if "Cobrador" in df_filtrado.columns:
            df_cobrador = df_filtrado.filter(
                pl.col("Cobrador").is_not_null() & (pl.col("Cobrador") != "")
            )

            group_cols_cobrador = cols_group_base + ['Cobrador']

            resultados_cobrador = (
                df_cobrador.group_by(group_cols_cobrador)
                .agg([
                    pl.col("Meta_T.R_$").sum().alias("Meta_Total"),            
                    pl.col("Total_Recaudo_Sin_Anti").sum().alias("Recaudo_Total") 
                ])
                .with_columns(
                    pl.when(pl.col("Meta_Total") > 0)
                    .then(pl.col("Recaudo_Total") / pl.col("Meta_Total"))
                    .otherwise(0.0)
                    .alias("Cumplimiento_%")
                )
            )
            data_cobrador = resultados_cobrador.to_dicts()
        else:
            print("⚠️ Advertencia: No se encontró la columna 'Cobrador'. Se omitirá este cálculo.")
            data_cobrador = []

        return {
            "resultados_zona": resultados_zona.to_dicts(),
            "resultados_cobrador": data_cobrador
        }