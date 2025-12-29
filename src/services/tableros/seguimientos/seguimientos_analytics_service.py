# src/services/tableros/seguimientos/seguimientos_analytics_service.py
import polars as pl

class SeguimientosAnalyticsService:
    
    def calcular_metricas_seguimientos(self, df_cartera: pl.DataFrame, df_novedades: pl.DataFrame) -> dict:
            """
            Calcula las métricas para el Tab 2 (Seguimientos) con soporte para Arquitectura Híbrida.
            """
            print("📊 ANALYTICS: Calculando métricas de Seguimientos...")
            
            # 1. VALIDACIÓN
            if df_cartera.is_empty():
                return {}

            # --- CORRECCIÓN 1: NO FILTRAR NULOS ---
            # Usamos el DataFrame completo para no perder los 10.000 registros que faltaban.
            df_base = df_cartera

            # 2. DEFINIR COLUMNAS DE FILTRO GLOBAL (Para que el frontend pueda filtrar)
            posibles_filtros = [
                "Empresa", "Regional_Cobro", "Zona", "Franja_Cartera", 
                "CALL_CENTER_FILTRO", "Call_Center_Apoyo", "Regional_Venta"
            ]
            cols_filtro = [c for c in posibles_filtros if c in df_base.columns]

            # 3. LÓGICA DE NEGOCIO (Columnas Calculadas)
            df_base = df_base.with_columns([
                # Estado_Pago: Total_Recaudo > 50000 -> 'PAGO'
                pl.when(pl.col("Total_Recaudo") > 50000)
                .then(pl.lit("PAGO"))
                .otherwise(pl.lit("SIN PAGO"))
                .alias("Estado_Pago"),

                # Estado_Gestion: Cantidad_Novedades > 0 -> 'CON GESTIÓN'
                pl.when(pl.col("Cantidad_Novedades") > 0)
                .then(pl.lit("CON GESTIÓN"))
                .otherwise(pl.lit("SIN GESTIÓN"))
                .alias("Estado_Gestion")
            ])

            # 4. PREPARAR JOIN KEYS (Cargos únicos por Cédula)
            if not df_novedades.is_empty():
                cargos_unicos = df_novedades.select(["Cedula_Cliente", "Cargo_Usuario"]).unique()
            else:
                cargos_unicos = pl.DataFrame(schema={"Cedula_Cliente": pl.Utf8, "Cargo_Usuario": pl.Utf8})

            # 5. CRUCE: CARTERA + CARGO
            # Unimos para saber qué cargo tiene asignado cada crédito
            df_merged = df_base.join(cargos_unicos, on="Cedula_Cliente", how="left")
            
            # Rellenar nulos de cargo
            df_merged = df_merged.with_columns(pl.col("Cargo_Usuario").fill_null("SIN ASIGNAR"))

            # --- SECCIÓN DE GRÁFICOS (AGREGACIONES TIPO CUBO) ---
            # Nota: Agregamos 'cols_filtro' a todos los group_by para permitir filtrado en Frontend

            # A. CUBO DONA (PAGO vs SIN PAGO)
            agg_donut = (
                df_merged.group_by(cols_filtro + ["Estado_Pago"])
                .len()
                .rename({"len": "count"})
            )

            # B. CUBO SUNBURST (Gestión Global)
            # Filtro visual: Ocultar ruido (Opcional, si quieres ver todo comenta el filter)
            grouped_sunburst = (
                df_merged.filter(
                    ~((pl.col("Estado_Gestion") == "CON GESTIÓN") & (pl.col("Cargo_Usuario") == "SIN ASIGNAR"))
                )
                .group_by(cols_filtro + ["Estado_Gestion", "Cargo_Usuario"])
                .len()
                .rename({"len": "Cantidad"})
                .sort("Cantidad", descending=True)
            )
            
            # Conteos simples para tarjetas KPI (También con filtros)
            conteo_gestion = (
                df_merged.group_by(cols_filtro + ["Estado_Gestion"])
                .len()
                .rename({"len": "count"})
            )

            # C. DETALLES DE PAGO (PAGO vs SIN PAGO por Gestión)
            # PAGO
            grouped_pago = (
                df_merged.filter(pl.col("Estado_Pago") == "PAGO")
                .group_by(cols_filtro + ["Estado_Gestion", "Cargo_Usuario"])
                .len().rename({"len": "Cantidad"})
            )
            
            # SIN PAGO
            grouped_sin_pago = (
                df_merged.filter(pl.col("Estado_Pago") == "SIN PAGO")
                .group_by(cols_filtro + ["Estado_Gestion", "Cargo_Usuario"])
                .len().rename({"len": "Cantidad"})
            )

            # D. GRÁFICO DE BARRAS (RODAMIENTO)
            agg_rodamiento = pl.DataFrame()
            if "Rodamiento" in df_merged.columns:
                agg_rodamiento = (
                    df_merged.group_by(cols_filtro + ["Rodamiento", "Estado_Gestion"])
                    .len()
                    .rename({"len": "Número de Cuentas"})
                    .sort("Rodamiento")
                )

            # --- SECCIÓN DE TABLA (EXPANSIÓN DE FILAS) ---
            # Solo para el JSON de detalle (limitado). 
            # La tabla completa real se consultará vía endpoint '/filtrar-tabla-detalle'.
            
            cols_detalle = ["Cedula_Cliente", "Cargo_Usuario", "Novedad", "Tipo_Novedad", "Fecha_Novedad"]
            cols_existentes_nov = [c for c in cols_detalle if c in df_novedades.columns]
            
            df_nov_detalle = df_novedades.select(cols_existentes_nov)

            df_tabla_final = df_merged.join(
                df_nov_detalle, 
                on=["Cedula_Cliente", "Cargo_Usuario"], 
                how="left",
                coalesce=True
            )

            return {
                "donut_data": agg_donut.to_dicts(),
                "sunburst_grouped": grouped_sunburst.to_dicts(),
                "sunburst_counts": conteo_gestion.to_dicts(),
                "detalle_pago": {
                    "grouped": grouped_pago.to_dicts(), 
                    # "counts" ya no es tan necesario si el front suma grouped, pero lo dejamos por compatibilidad
                    "counts": [] 
                },
                "detalle_sin_pago": {
                    "grouped": grouped_sin_pago.to_dicts(), 
                    "counts": []
                },
                "rodamiento_data": agg_rodamiento.to_dicts() if not agg_rodamiento.is_empty() else [],
                # Muestra pequeña  para vista rápida, el resto va por paginación
                "tabla_detalle": df_tabla_final.head(100).to_dicts()
            }