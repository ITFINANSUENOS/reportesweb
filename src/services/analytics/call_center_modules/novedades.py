import polars as pl
import pandas as pd
import unicodedata
from datetime import date
from .utils import exportar_a_json

def _normalize_tokens(name: str) -> set:
    if not isinstance(name, str) or name is None: return set()
    STOP_WORDS = {'de', 'del', 'la', 'las', 'los', 'el', 'y', 'e', 'i'}
    name = ''.join(c for c in unicodedata.normalize('NFD', name.lower()) if unicodedata.category(c) != 'Mn')
    return {t for t in name.split() if len(t) > 1 and t not in STOP_WORDS}

def procesar_novedades_sistema(df_nov: pl.DataFrame, df_llamadas: pl.DataFrame, call_center_filtro: str = None) -> dict:
    resultado_base = {
        "df_agg_call": [], "df_agg_tipo": [], "df_compromisos": [], 
        "kpis": {"total": 0, "sin_asignar": 0, "top_tipo": "N/A"}
    }
    
    if df_nov is None or df_nov.is_empty() or df_llamadas is None or df_llamadas.is_empty(): return resultado_base

    df_nov = df_nov.rename({c: c.strip() for c in df_nov.columns})
    df_llamadas = df_llamadas.rename({c: c.strip() for c in df_llamadas.columns})

    col_usuario = 'Nombre_Usuario' if 'Nombre_Usuario' in df_nov.columns else 'Usuario_Novedad'
    col_agente_ref = 'Nombre_Call'
    col_cc_ref = 'Call_Center_Limpio' if 'Call_Center_Limpio' in df_llamadas.columns else 'Call_Center'

    if col_usuario not in df_nov.columns or col_agente_ref not in df_llamadas.columns: return resultado_base

    df_ref = df_llamadas.select([col_agente_ref, col_cc_ref]).filter(
        pl.col(col_cc_ref).is_not_null() & (pl.col(col_cc_ref).cast(pl.Utf8) != "")
    ).drop_nulls().unique()

    agentes_ref = [{'tokens': list(_normalize_tokens(row[col_agente_ref])), 'cc': row[col_cc_ref], 'len': len(list(_normalize_tokens(row[col_agente_ref])))} for row in df_ref.iter_rows(named=True) if _normalize_tokens(row[col_agente_ref])]

    def find_best_match(nombre_usuario):
        import difflib
        tokens_u = list(_normalize_tokens(nombre_usuario))
        if not tokens_u: return 'SIN ASIGNAR'
        best_cc, best_score = 'SIN ASIGNAR', 0.0
        for agente in agentes_ref:
            score = sum(max([1.0 if t_a in _normalize_tokens(nombre_usuario) else difflib.SequenceMatcher(None, t_a, t_u).ratio() for t_u in tokens_u] + [0.0]) for t_a in agente['tokens']) / agente['len']
            if score >= 0.65: return agente['cc']
            if score > best_score: best_score, best_cc = score, agente['cc']
        return best_cc if best_score >= 0.60 else 'SIN ASIGNAR'

    df_proc = df_nov.with_columns(pl.col(col_usuario).map_elements(find_best_match, return_dtype=pl.Utf8).alias("Call_Center"))
    valid_ccs = [f'CL{i}' for i in range(1, 10)]
    df_proc = df_proc.with_columns(pl.when(pl.col("Call_Center").is_in(valid_ccs)).then(pl.col("Call_Center")).otherwise(pl.lit("SIN ASIGNAR")).alias("Call_Center"))

    if call_center_filtro: df_proc = df_proc.filter(pl.col("Call_Center") == call_center_filtro)

    col_tipo = 'Tipo_Novedad' if 'Tipo_Novedad' in df_proc.columns else df_proc.columns[0]
    df_compromisos_json = []

    df_comp = df_proc.filter(pl.col(col_tipo).cast(pl.Utf8).str.to_uppercase().str.contains("COMPROMISO") & (pl.col('Call_Center') != 'SIN ASIGNAR'))

    if not df_comp.is_empty():
        pdf_comp = df_comp.to_pandas()
        col_fecha = next((col for col in pdf_comp.columns if 'fecha' in col.lower() and 'cuota' not in col.lower() and 'nacimiento' not in col.lower()), None)
        if col_fecha:
            pdf_comp['Fecha_Obj'] = pd.to_datetime(pdf_comp[col_fecha].astype(str).str.strip(), errors='coerce', dayfirst=True)
            hoy, inicio_mes = pd.Timestamp.now().normalize(), pd.Timestamp.now().normalize().replace(day=1)
            pdf_comp['Estado_Acuerdo'] = pdf_comp['Fecha_Obj'].apply(lambda f: 'ACUERDOS SIN FECHA' if pd.isna(f) or f < inicio_mes else ('ACUERDOS VENCIDOS' if f < hoy else 'ACUERDOS VIGENTES'))
            agg_pd = pdf_comp.groupby(['Call_Center', 'Estado_Acuerdo']).size().reset_index(name='Cantidad')
            df_compromisos_json = agg_pd.to_dict(orient='records')

    df_validos = df_proc.filter(pl.col('Call_Center') != 'SIN ASIGNAR')
    return {
        "df_agg_call": exportar_a_json(df_validos.group_by('Call_Center').len().rename({'len': 'Cantidad'}).sort('Cantidad', descending=True)) if not df_validos.is_empty() else [],
        "df_agg_tipo": exportar_a_json(df_validos.group_by(['Call_Center', col_tipo]).len().rename({'len': 'Cantidad'})) if not df_validos.is_empty() else [],
        "df_compromisos": df_compromisos_json,
        "kpis": {"total": df_proc.height, "sin_asignar": df_proc.filter(pl.col('Call_Center') == 'SIN ASIGNAR').height if not df_proc.is_empty() else 0, "top_tipo": df_proc[col_tipo].mode()[0] if not df_proc.is_empty() and len(df_proc[col_tipo].mode()) > 0 else "N/A"}
    }