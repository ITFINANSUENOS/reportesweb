import pandas as pd
import numpy as np
from typing import Dict
from src.models.anticipos_model import AnticiposConfig

class AnticiposDataProcessor:
    """Contiene toda la lógica para transformar los datos de anticipos."""

    def __init__(self, config: AnticiposConfig):
        self.config = config

    def process_data(self, dfs: dict) -> pd.DataFrame:
        """Aplica la lógica de negocio para cruzar y calcular las observaciones."""
        df_online = dfs['ONLINE']
        df_ac_fs = dfs['AC FS']
        df_ac_arp = dfs['AC ARP']

        df_online['CEDULA'] = df_online['CEDULA'].astype(str).str.strip()
        df_ac_fs['CEDULA'] = df_ac_fs['CEDULA'].astype(str).str.strip()
        df_ac_arp['CEDULA'] = df_ac_arp['CEDULA'].astype(str).str.strip()

        df_online['CUENTAS_FS'] = df_online['CEDULA'].map(df_ac_fs['CEDULA'].value_counts()).fillna(0).astype(int)
        df_online['CUENTAS_ARP'] = df_online['CEDULA'].map(df_ac_arp['CEDULA'].value_counts()).fillna(0).astype(int)

        merged_df = pd.merge(df_online, df_ac_fs, on='CEDULA', how='left')
        final_df = pd.merge(merged_df, df_ac_arp, on='CEDULA', how='left')

        final_df['VALOR_POSITIVO'] = final_df['VALOR'].abs()
        resta_fs = final_df['ULTIMO_SALDO_FS'] - final_df['VALOR_POSITIVO']
        resta_arp = final_df['ULTIMO_SALDO_ARP'] - final_df['VALOR_POSITIVO']
        final_df['RESTA_SALDO'] = resta_fs.fillna(resta_arp)

        condiciones = [
            (pd.notna(final_df['FACTURA_FS'])) & (pd.notna(final_df['FACTURA_ARP'])),
            (final_df['RESTA_SALDO'] <= 0),
            (pd.notna(final_df['FACTURA_FS'])) & (pd.isna(final_df['FACTURA_ARP'])),
            (pd.isna(final_df['FACTURA_FS'])) & (pd.notna(final_df['FACTURA_ARP']))
        ]
        opciones = ['REVISAR TIENE 2 CARTERAS', 'PAGO TOTAL', 'CARTERA EN FINANSUEÑOS', 'CARTERA EN ARPESOD']
        final_df['OBSERVACIONES'] = np.select(condiciones, opciones, default='REVISAR SI ES CODEUDOR')
        
        multi_cartera_mask = final_df['OBSERVACIONES'] == 'REVISAR TIENE 2 CARTERAS'
        final_df.loc[multi_cartera_mask, ['FACTURA_FS', 'FACTURA_ARP']] = 'MAS DE UNA CARTERA'
        
        return final_df

    def prepare_output_sheets(self, final_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Separa el DataFrame procesado en las hojas finales para el reporte."""
        df_finansuenos = final_df[pd.notna(final_df['FACTURA_FS'])].copy()
        df_arpesod = final_df[pd.notna(final_df['FACTURA_ARP'])].copy()
        df_sin_cartera = final_df[(pd.isna(final_df['FACTURA_FS'])) & (pd.isna(final_df['FACTURA_ARP']))].copy()
        
        base_cols = list(self.config.rename_columns['ONLINE'].values())
        cols_sin_cartera = base_cols + ['CUENTAS_FS', 'CUENTAS_ARP', 'OBSERVACIONES']
        
        return {
            "FINANSUEÑOS": df_finansuenos.reindex(columns=self.config.column_order_fs),
            "ARPESOD": df_arpesod.reindex(columns=self.config.column_order_arp),
            "SIN CARTERA": df_sin_cartera.reindex(columns=cols_sin_cartera)
        }