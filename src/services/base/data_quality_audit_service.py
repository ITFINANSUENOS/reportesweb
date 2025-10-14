import pandas as pd
import numpy as np

class DataQualityAuditService:
    """
    Servicio para auditar la calidad de los datos de un DataFrame,
    identificando registros que requieren corrección según reglas de negocio.
    """
    def run_audit(self, df):
        """
        Crea un reporte de auditoría de calidad de datos.
        """
        print("🔍 Generando reporte de auditoría de calidad de datos...")
        
        df_auditoria = df.drop_duplicates(subset=['Credito']).copy()
        
        # Consolidar columna 'Celular'
        if 'Celular_y' in df_auditoria.columns and 'Celular_x' in df_auditoria.columns:
            df_auditoria['Celular'] = df_auditoria['Celular_y'].fillna(df_auditoria['Celular_x'])
        elif 'Celular_y' in df_auditoria.columns:
            df_auditoria['Celular'] = df_auditoria['Celular_y']
        elif 'Celular_x' in df_auditoria.columns:
            df_auditoria['Celular'] = df_auditoria['Celular_x']
        else:
            df_auditoria['Celular'] = np.nan

        # Reglas de Nulos o Valores Específicos
        df_auditoria['Estado_Fecha_Desembolso'] = np.where(pd.to_datetime(df_auditoria['Fecha_Desembolso'], errors='coerce').isnull(), 'CORREGIR', 'BIEN')
        df_auditoria['Estado_Fecha_Facturada'] = np.where(pd.to_datetime(df_auditoria['Fecha_Facturada'], errors='coerce').isnull(), 'CORREGIR', 'BIEN')
        df_auditoria['Estado_Factura'] = np.where(df_auditoria['Factura_Venta'] == 'NO ASIGNADA', 'CORREGIR', 'BIEN')
        df_auditoria['Estado_Producto'] = np.where(df_auditoria['Nombre_Producto'] == 'NO REGISTRA', 'CORREGIR', 'BIEN')
        df_auditoria['Estado_Obsequio'] = np.where((df_auditoria['Obsequio'] == 'SIN OBSEQUIOS') & (df_auditoria['Nombre_Producto'] == 'NO REGISTRA'), 'CORREGIR', 'BIEN')
        df_auditoria['Estado_Cant_Producto'] = np.where((pd.to_numeric(df_auditoria['Cantidad_Producto'], errors='coerce') == 0) & (df_auditoria['Factura_Venta'] == 'NO ASIGNADA'), 'CORREGIR', 'BIEN')
        df_auditoria['Estado_Cant_Obsequio'] = np.where((pd.to_numeric(df_auditoria['Cantidad_Obsequio'], errors='coerce') == 0) & (df_auditoria['Factura_Venta'] == 'NO ASIGNADA'), 'CORREGIR', 'BIEN')
        df_auditoria['Estado_Direccion'] = np.where(df_auditoria['Direccion'].isnull() | (df_auditoria['Direccion'] == ''), 'CORREGIR', 'BIEN')
        df_auditoria['Estado_Barrio'] = np.where(df_auditoria['Barrio'].isnull() | (df_auditoria['Barrio'] == ''), 'CORREGIR', 'BIEN')
        df_auditoria['Estado_Nombre_Ciudad'] = np.where(df_auditoria['Nombre_Ciudad'].isnull() | (df_auditoria['Nombre_Ciudad'] == ''), 'CORREGIR', 'BIEN')

        # ... (Agregar todas las demás reglas de `_detectar_problemas_calidad`)

        # Reglas de Formato (Regex)
        email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        mask_correo_ok = df_auditoria['Correo'].astype(str).str.match(email_regex, na=False)
        df_auditoria['Estado_Correo'] = np.where(mask_correo_ok, 'BIEN', 'CORREGIR')

        celular_regex = r'^(3\d{9}|60\d{8})$'
        celulares_str = df_auditoria['Celular'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        mask_celular_ok = celulares_str.str.match(celular_regex, na=False)
        df_auditoria['Estado_Celular'] = np.where(mask_celular_ok, 'BIEN', 'CORREGIR')

        # Reglas Numéricas
        for col in ['Valor_Desembolso', 'Total_Cuotas', 'Valor_Cuota']:
            df_auditoria[f'Estado_{col}'] = np.where(pd.to_numeric(df_auditoria[col], errors='coerce').fillna(0) == 0, 'CORREGIR', 'BIEN')
        
        # Filtrar y seleccionar columnas
        columnas_de_estado = [col for col in df_auditoria.columns if col.startswith('Estado_')]
        mascara_final = (df_auditoria[columnas_de_estado] == 'CORREGIR').any(axis=1)
        
        columnas_a_mostrar = ['Credito', 'Cedula_Cliente', 'Nombre_Cliente'] + sorted(columnas_de_estado)
        df_a_corregir = df_auditoria.loc[mascara_final, columnas_a_mostrar]
        
        if not df_a_corregir.empty:
            print(f"   - ✅ Se encontraron {len(df_a_corregir)} créditos únicos con problemas de calidad para revisar.")
        
        return df_a_corregir