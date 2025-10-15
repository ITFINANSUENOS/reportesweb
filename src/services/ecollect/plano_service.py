# src/services/plano/plano_service.py

import pandas as pd
from typing import List

class PlanoService:
    """
    Contiene la lógica para convertir un DataFrame procesado
    a un archivo de texto plano (.txt) con un formato específico.
    """

    def _generar_linea_encabezado(self, df: pd.DataFrame) -> str:
        """Genera la primera línea (encabezado) del archivo plano."""
        fecha_actual = pd.Timestamp.now().strftime('%Y%m%d')
        num_registros = len(df)
        
        # Suma la columna 'Valor', convierte a entero para quitar decimales, y añade '00'
        valor_total = df['Valor'].sum()
        valor_total_formateado = str(int(valor_total)) + '00'
        
        return f"1,{fecha_actual},{num_registros},{valor_total_formateado},0"

    def _formatear_descripcion(self, row: pd.Series, prefijo: str) -> str:
        """
        Crea la descripción de las cuotas basado en si la primera y última son iguales.
        Ej: "Cuota No. 5" o "Cuota No. 5 a Cuota No. 14"
        """
        primera = row['Primera_Cuota_Atraso']
        ultima = row['Ultima_Cuota_Atraso']
        
        if primera == ultima:
            return f"{prefijo} No. {primera}"
        else:
            return f"{prefijo} No. {primera} a {prefijo} No. {ultima}"

    def _generar_lineas_datos(self, df: pd.DataFrame) -> List[str]:
        """Genera todas las líneas de datos (registros) del archivo plano."""
        lineas_datos = []
        
        for _, row in df.iterrows():
            # Formatear campos específicos
            desc_cuotas = self._formatear_descripcion(row, "Cuota")
            desc_pago = self._formatear_descripcion(row, "Pago de Cuota")
            fecha_atraso_formateada = row['Fecha_Atraso'].strftime('%Y%m%d')
            valor_formateado = str(int(row['Valor'])) + '00'
            
            # Construir la línea completa
            linea = (
                f"2,10791,1001,{row['Cedula_Cliente']},{row['Credito']},{desc_cuotas},"
                f"{row['Codigo']},{desc_pago},0,{fecha_atraso_formateada},{valor_formateado},"
                "0,0,0,0,0,0,0,0"
            )
            lineas_datos.append(linea)
            
        return lineas_datos

    def generar_archivo_plano(self, df: pd.DataFrame, ruta_guardado: str) -> bool:
        """
        Orquesta la creación del archivo .txt completo y lo guarda en la ruta especificada.
        """
        if df.empty:
            print("El DataFrame está vacío, no se generará el archivo plano.")
            return False
            
        try:
            linea_encabezado = self._generar_linea_encabezado(df)
            lineas_datos = self._generar_lineas_datos(df)
            
            with open(ruta_guardado, 'w', encoding='utf-8') as f:
                f.write(linea_encabezado + '\n')
                for linea in lineas_datos:
                    f.write(linea + '\n')
            return True
        except Exception as e:
            print(f"Error al generar o guardar el archivo plano: {e}")
            return False