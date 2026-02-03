import pytest
import polars as pl
from src.services.tableros.resultados.resultados_analytics_service import ResultadosAnalyticsService

def test_calculo_kpis_resultados():
    # 1. ARRANGE (Preparar datos de prueba)
    # Creamos un DataFrame pequeño en memoria que simula tu Excel
    df_fake = pl.DataFrame({
        "Empresa": ["Empresa A", "Empresa A"],
        "Regional_Cobro": ["Norte", "Norte"],
        "Zona": ["Z1", "Z1"],
        "Franja_Meta": ["1 A 30", "1 A 30"],
        
        # Datos numéricos clave
        "Meta_$": [100.0, 200.0],          # Suma total esperada: 300
        "Recaudo_Meta": [50.0, 100.0],     # Suma total esperada: 150
        "Total_Recaudo_Sin_Anti": [0.0, 0.0],
        "Meta_T.R_$": [0.0, 0.0]
    })
    
    service = ResultadosAnalyticsService()
    
    # 2. ACT (Ejecutar la lógica)
    resultado = service.calcular_metricas_resultados(df_fake)
    
    # Extraemos la lista de resultados por zona
    data_zona = resultado["resultados_zona"]
    
    # 3. ASSERT (Verificar que la matemática no miente)
    
    # Solo debería haber 1 fila (porque agrupamos todo en Zona Z1)
    assert len(data_zona) == 1 
    
    fila = data_zona[0]
    
    # Validamos las sumas
    assert fila["Meta_Total"] == 300.0
    assert fila["Recaudo_Total"] == 150.0
    
    # Validamos el porcentaje (150 / 300 = 0.5)
    assert fila["Cumplimiento_%"] == 0.5