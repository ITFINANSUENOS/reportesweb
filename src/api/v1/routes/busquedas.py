from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
import polars as pl
import os
import traceback
from src.schemas.request_models import FiltrosTabla, ConsultaRelacionada
from src.schemas.response_models import BusquedaResponse
from src.services.analytics.call_center import CallCenterAnalyticsService   
from src.services.busquedas_service import BusquedasService 
router = APIRouter()
busquedas_svc = BusquedasService()

@router.post("/filtrar-tabla-detalle", response_model=BusquedaResponse)
def filtrar_tabla_detalle(payload: FiltrosTabla):
    try:
        return busquedas_svc.filtrar_tabla(payload)
    except Exception as e:
        traceback.print_exc() 
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/exportar-excel")
def exportar_excel(payload: FiltrosTabla):
    try:
        buffer = busquedas_svc.exportar_excel(payload)
        
        headers = {
            'Content-Disposition': f'attachment; filename="Exportacion_{payload.origen}.xlsx"'
        }
        
        return StreamingResponse(
            buffer, 
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 
            headers=headers
        )
    except Exception as e:
        traceback.print_exc() 
        raise HTTPException(status_code=500, detail=f"Error generando Excel: {str(e)}")

@router.get("/consultar-clientes")
def consultar_clientes(job_id: str, q: str, limit: int = 20, origen: str = "cartera"):
    try:
        return busquedas_svc.consultar_clientes(job_id, q, limit, origen)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/consultar-relacionados")
def consultar_relacionados(payload: ConsultaRelacionada):
    try:
        return busquedas_svc.consultar_relacionados(payload)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))  

@router.post("/metricas/call-center")
def obtener_metricas_call_center(payload: FiltrosTabla):
    try:
        archivos = busquedas_svc.descargar_dependencias_metricas(payload.job_id)
        if not archivos: return {}

        # Cargar DataFrames
        df_cartera = pl.read_parquet(archivos["cartera"][1])
        df_llamadas = pl.read_parquet(archivos["llamadas"][1]) if os.path.exists(archivos["llamadas"][1]) else pl.DataFrame()
        df_mensajes = pl.read_parquet(archivos["mensajes"][1]) if os.path.exists(archivos["mensajes"][1]) else pl.DataFrame()
        df_novedades = pl.read_parquet(archivos["novedades"][1]) if os.path.exists(archivos["novedades"][1]) else pl.DataFrame()

        # Filtrar Cartera
        condicion = pl.lit(True)
        filtros_map = {
            "Empresa": payload.empresa, "Zona": payload.zona, "Regional_Cobro": payload.regional,
            "Regional": payload.regional, "Franja_Cartera": payload.franja, 
            "CALL_CENTER_FILTRO": payload.call_center,
        }
        for col, val in filtros_map.items():
            if val and col in df_cartera.columns:
                condicion = condicion & pl.col(col).is_in(val)
        
        if payload.rodamiento and "Rodamiento" in df_cartera.columns:
            condicion = condicion & pl.col("Rodamiento").is_in(payload.rodamiento)

        df_cartera_filtrada = df_cartera.filter(condicion)

        # Filtrar auxiliares basados en Cartera
        if not df_cartera_filtrada.is_empty():
            cedulas_filtradas = df_cartera_filtrada["Cedula_Cliente"].unique()
            if "Cedula_Cliente" in df_novedades.columns:
                df_novedades = df_novedades.filter(pl.col("Cedula_Cliente").is_in(cedulas_filtradas))
            if payload.call_center and "Call_Center" in df_llamadas.columns:
                 df_llamadas = df_llamadas.filter(pl.col("Call_Center").is_in(payload.call_center))

        # Recalcular
        cc_service = CallCenterAnalyticsService()
        resultados = cc_service.calcular_metricas_call_center(
            df_cartera=df_cartera_filtrada,
            df_novedades=df_novedades,
            df_llamadas=df_llamadas,
            df_mensajeria=df_mensajes
        )
        
        if "df_parquet_detalle" in resultados:
            del resultados["df_parquet_detalle"]

        return resultados

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))