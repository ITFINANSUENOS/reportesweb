import polars as pl
from .call_center_modules.utils import agregar_columnas_calculadas, exportar_a_json
from .call_center_modules.core import procesar_base_gestion, calcular_cumplimiento, calcular_rodamientos
from .call_center_modules.calls import procesar_llamadas
from .call_center_modules.messaging import procesar_mensajeria
from .call_center_modules.novedades import procesar_novedades_sistema

class CallCenterAnalyticsService:
    def calcular_metricas_call_center(self, 
                                     df_cartera: pl.DataFrame, 
                                     df_novedades: pl.DataFrame, 
                                     df_llamadas: pl.DataFrame, 
                                     df_mensajeria: pl.DataFrame,
                                     call_center_filtro: str = None) -> dict:
        """
        Calcula todas las métricas del call center.
        
        Args:
            df_cartera: DataFrame con cartera
            df_novedades: DataFrame con novedades
            df_llamadas: DataFrame con llamadas
            df_mensajeria: DataFrame con mensajería
            call_center_filtro: Opcional, nombre del call center para filtrar (ej. 'CL1')
        """
        if df_cartera is None or df_cartera.is_empty():
            return {}

        print(f"🚀 Iniciando cálculo de métricas" + (f" con filtro: {call_center_filtro}" if call_center_filtro else ""))

        # --- 0. PRE-PROCESAMIENTO ---
        df_cartera_enrich = agregar_columnas_calculadas(df_cartera)

        # Enriquecer Llamadas
        df_llamadas_proc = df_llamadas
        if not df_llamadas.is_empty() and "Call_Center" in df_llamadas.columns:
            df_llamadas_proc = df_llamadas.with_columns(
                pl.col("Call_Center").cast(pl.Utf8).str.strip_chars().str.to_uppercase().fill_null("SIN DATO").alias("Call_Center_Limpio")
            )
        elif "Call_Center_Limpio" not in df_llamadas.columns:
            df_llamadas_proc = df_llamadas.with_columns(pl.lit("SIN DATO").alias("Call_Center_Limpio"))

        # --- 1. PROCESAMIENTO MODULAR ---
        print("📊 Procesando base de gestión...")
        df_unificado, df_detalle_final = procesar_base_gestion(df_cartera_enrich, df_novedades)
        
        print("📈 Calculando cumplimiento...")
        metricas_cumplimiento = calcular_cumplimiento(df_unificado)
        
        print("🔄 Calculando rodamientos...")
        rodamiento_data = calcular_rodamientos(df_unificado)
        
        print("📞 Procesando llamadas...")
        stats_llamadas = procesar_llamadas(df_llamadas_proc) or {}  # <--- Asegurar que sea diccionario
        
        print("✉️ Procesando mensajería...")
        stats_mensajeria = procesar_mensajeria(df_mensajeria, df_novedades, df_cartera_enrich) or {}  # <--- Asegurar que sea diccionario
        
        print("📝 Procesando novedades con filtro:", call_center_filtro)
        stats_novedades = procesar_novedades_sistema(df_novedades, df_llamadas_proc, call_center_filtro) or {}  # <--- Asegurar que sea diccionario

        # --- 2. PREPARAR FILTROS GLOBALES ---
        cols_filtros = ['Empresa', 'Zona', 'Regional', 'Franja_Meta', 'Rodamiento', 'Regional_Cobro', 'CALL_CENTER_FILTRO']
        cols_existentes = [c for c in cols_filtros if c in df_detalle_final.columns]
        
        if cols_existentes:
            df_filtros = df_detalle_final.select(cols_existentes).unique()
        else:
            df_filtros = pl.DataFrame()

        print("✅ Cálculo completado exitosamente")

        # --- 3. RETORNO DE DATOS ---
        resultado = {
            "df_parquet_detalle": df_detalle_final,
            "reporte_raw": exportar_a_json(metricas_cumplimiento),
            "rodamiento_data": exportar_a_json(rodamiento_data),
            
            # Desempaquetamos los diccionarios de los submódulos (asegurándonos que sean diccionarios)
            **stats_llamadas,
            **stats_mensajeria,
            **stats_novedades,
            
            "filtros_disponibles": exportar_a_json(df_filtros),
            "meta": {
                "total_cuentas": df_unificado.height,
                "total_meta": df_unificado["META_UNIFICADA"].sum() if not df_unificado.is_empty() else 0
            }
        }
        
        return resultado