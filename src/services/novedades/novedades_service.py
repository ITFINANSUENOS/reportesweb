# En: src/services/novedades_service.py
import pandas as pd

class NovedadesService:
    def __init__(self, config):
        self.config = config

    def _formatear_novedades(self, grupo):
        grupo = grupo.sort_values('Fecha_Novedad')
        lineas = [f"[{fila['Fecha_Novedad'].strftime('%d/%m/%Y')}] ({fila['Usuario_Novedad']}): {fila['Tipo_Novedad']}"
                  for _, fila in grupo.iterrows()]
        return "\n".join(lineas)

    def aplicar_novedades(self, df_base, df_novedades):
        print("🔄 Aplicando novedades...")
        # Se elimina la línea pd.read_excel. El resto de la lógica es la misma.
        
        df_novedades['Fecha_Novedad'] = pd.to_datetime(df_novedades['Fecha_Novedad'], errors='coerce')
        df_novedades.dropna(subset=['Cedula_Cliente', 'Fecha_Novedad'], inplace=True)
        df_novedades['Cedula_Cliente'] = df_novedades['Cedula_Cliente'].astype(str)

        mapa_novedades = df_novedades.groupby('Cedula_Cliente').apply(self._formatear_novedades)
        df_historial = mapa_novedades.reset_index(name='Historial_Novedades')

        df_base['Cedula_Cliente'] = df_base['Cedula_Cliente'].astype(str)
        df_actualizado = pd.merge(df_base, df_historial, on='Cedula_Cliente', how='left')
        df_actualizado['Historial_Novedades'].fillna('Sin Novedades', inplace=True)
        
        print("✅ Novedades aplicadas.")
        return df_actualizado