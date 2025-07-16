from pathlib import Path
import os
import pandas as pd
import numpy as np
from typing import Dict
from src.models.convenios_model import ConveniosConfig

class ConveniosService:
    def __init__(self, config:ConveniosConfig = None):
        self.config = config if config else ConveniosConfig()

    def generate_report(self, file_path: str, status_callback):
        """
        Orquesta todo el proceso de generación del reporte Financiero.
        Este es el ÚNICO método público que el controlador llamará.
        """
        status_callback("Cargando y filtrando datos...", 10)
        dfs = self._load_and_filter_data(file_path)

        print("DEBUG: Hojas cargadas por el servicio ->", dfs.keys())

        # --- AÑADE ESTE BLOQUE DE DEPURACIÓN ---
        print("\n--- DEBUG: TAMAÑO DE HOJAS CARGADAS ---")
        for name, df_item in dfs.items():
            print(f"Hoja '{name}': {len(df_item)} filas")
        print("-----------------------------------------\n")
        
        status_callback("Preparando datos...", 30)
        dfs = self._prepare_data(dfs)

        status_callback("Procesando pagos de Bancolombia...", 50)
        df_bancolombia = self._process_payment_type(dfs, 'bancolombia')
        
        status_callback("Procesando pagos de Efecty...", 70)
        df_efecty = self._process_payment_type(dfs, 'efecty')
        
        return df_bancolombia, df_efecty
    
    def validate_input_file(self, file_path: str) -> bool:
        """Valida que el archivo Excel contenga todas las hojas requeridas."""
        try:
            with pd.ExcelFile(file_path) as xls:
                sheets = xls.sheet_names
                missing_sheets = [sheet for sheet in self.config.required_sheets if sheet not in sheets]
                
                if missing_sheets:
                    raise ValueError(f"Faltan hojas requeridas: {', '.join(missing_sheets)}")
                    
            return True
        except Exception as e:
            raise ValueError(f"Error al validar archivo: {str(e)}")

    def _load_and_filter_data(self, file_path: str) -> Dict[str, pd.DataFrame]:
        """Carga y filtra los datos del archivo Excel según la configuración."""
        try:
            dfs = pd.read_excel(file_path, sheet_name=None)
            self.validate_input_file(file_path)
            filtered_data = {}
            # El bucle procesa TODAS las hojas definidas en la configuración
            for sheet_name, columns in self.config.sheet_columns.items():
                if sheet_name not in dfs:
                    raise ValueError(f"Hoja '{sheet_name}' no encontrada en el archivo")
                df = dfs[sheet_name][columns].copy()
                if sheet_name in self.config.rename_columns:
                    df.rename(columns=self.config.rename_columns[sheet_name], inplace=True)
                filtered_data[sheet_name] = df
            return filtered_data
        except Exception as e:
            raise ValueError(f"Error al cargar datos: {str(e)}")
            

    def _prepare_data(self, dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Pre-procesa los DataFrames, convirtiendo columnas a string."""
        string_columns = {
            'PAGOS BANCOLOMBIA': ['Referencia 1', 'Referencia 2'], 'PAGOS EFECTY': ['Identificación'],
            'EMPLEADOS ACTUALES': ['vincedula'], 'AC FS': ['CEDULA_FS', 'FACTURA_FS'],
            'AC ARP': ['CEDULA_ARP', 'FACTURA_ARP'], 'CASA DE COBRANZA': ['FACTURA'],
            'CODEUDORES': ['DOCUMENTO_CODEUDOR']
        }
        for sheet, cols in string_columns.items():
            if sheet in dfs:
                for col in cols:
                    if col in dfs[sheet].columns:
                        dfs[sheet][col] = dfs[sheet][col].astype(str)
        return dfs      

    def _merge_dataframes(self, main_df: pd.DataFrame, to_merge: pd.DataFrame, left_on: str, right_on: str, how: str = 'left'):
        """Realiza un merge entre DataFrames de forma segura."""
        return main_df.merge(to_merge, how=how, left_on=left_on, right_on=right_on)  

    def _count_accounts(self, df: pd.DataFrame, id_column: str, count_column_name: str) -> pd.DataFrame:
        """CORREGIDO: Añadido 'self'."""
        counts = df[id_column].value_counts().reset_index()
        counts.columns = [id_column, count_column_name]
        return counts
    
    BANCOLOMBIA_SHEET_NAME = 'PAGOS BANCOLOMBIA'
    EFECTY_SHEET_NAME = 'PAGOS EFECTY'

    def _process_payment_type(self, dfs: Dict[str, pd.DataFrame], payment_type: str) -> pd.DataFrame:
        """Método de procesamiento principal, ahora con depuración detallada."""
        payment_df_name = self.BANCOLOMBIA_SHEET_NAME if payment_type == 'bancolombia' else self.EFECTY_SHEET_NAME
        
        if payment_df_name not in dfs:
            raise KeyError(f"La hoja de pago '{payment_df_name}' no se encontró en los datos cargados.")
            
        df = dfs[payment_df_name].copy()
        
        # Si el DataFrame de entrada ya está vacío, no tiene sentido procesarlo.
        if df.empty:
            print(f"DEBUG: La hoja '{payment_df_name}' está vacía. No se procesará.")
            return df

        # --- INICIO DE DEPURACIÓN DETALLADA ---
        print(f"\n--- Iniciando proceso para: {payment_type.upper()} ---")
        print(f"Paso 0: Filas iniciales: {len(df)}")
        
        df = self._perform_merges(df, dfs, payment_type)
        print(f"Paso 1: Filas después de TODOS los merges: {len(df)}")

        df = self._calculate_final_columns(df)
        print(f"Paso 2: Filas después de calcular columnas: {len(df)}")

        df = self._cleanup_dataframe(df)
        print(f"Paso 3: Filas después de la limpieza final: {len(df)}")
        print("--------------------------------------------------")
        # --- FIN DE DEPURACIÓN DETALLADA ---

        return df

    def _perform_merges(self, df: pd.DataFrame, dfs: Dict, payment_type: str) -> pd.DataFrame:
        """Realiza todas las fusiones de datos necesarias."""
        merge_conf = self.config.merge_config[payment_type]

        # Fusiones iniciales
        df = self._merge_dataframes(df, dfs['EMPLEADOS ACTUALES'], *merge_conf['empleados'])
        df = self._merge_dataframes(df, dfs['AC FS'], *merge_conf['ac_fs'])
        df = self._merge_dataframes(df, dfs['AC ARP'], *merge_conf['ac_arp'])

        # Conteo y fusión de cuentas
        for ac_type in ['FS', 'ARP']:
            key_col_name = self.config.merge_config[payment_type][f'ac_{ac_type.lower()}'][0]
            id_col_name = f'CEDULA_{ac_type}'
            count_col_name = f'CANTIDAD CUENTAS {ac_type}'
            
            counts_df = self._count_accounts(dfs[f'AC {ac_type}'], id_col_name, count_col_name)
            df = self._merge_dataframes(
                main_df=df, 
                to_merge=counts_df, 
                left_on=key_col_name, 
                right_on=id_col_name,
                how = 'left'
            )
            df[count_col_name] = df[count_col_name].fillna(0).astype(int)

        # Determinar Factura Final
        df['TOTAL_CUENTAS'] = df['CANTIDAD CUENTAS FS'] + df['CANTIDAD CUENTAS ARP']
        factura_original = np.where(df['FACTURA_FS'].notna(), df['FACTURA_FS'], df['FACTURA_ARP'])
        df['FACTURA FINAL'] = np.where(df['TOTAL_CUENTAS'] > 1, 'Mas de una cartera', factura_original).astype(str)
        df['FACTURA FINAL'].replace('nan', 'SIN CARTERA', inplace=True)
        df.drop_duplicates(subset=list(dfs[f'PAGOS {payment_type.upper()}'].columns), keep='first', inplace=True)

        # Fusión de saldos unificados
        df_saldos_unificados = pd.concat([
            dfs['AC FS'][['FACTURA_FS', 'SALDO_FS','CENTRO_COSTO_FS']].rename(columns={'FACTURA_FS': 'FACTURA', 'SALDO_FS': 'SALDO','CENTRO_COSTO_FS': 'CENTRO COSTO'}),
            dfs['AC ARP'][['FACTURA_ARP', 'SALDO_ARP','CENTRO_COSTO_ARP']].rename(columns={'FACTURA_ARP': 'FACTURA', 'SALDO_ARP': 'SALDO', 'CENTRO_COSTO_ARP': 'CENTRO COSTO'})
        ], ignore_index=True).drop_duplicates(subset='FACTURA')
        df = self._merge_dataframes(df, df_saldos_unificados, left_on='FACTURA FINAL', right_on='FACTURA')

        # Fusión con casa de cobranza y codeudores
        casa_cobranza_sin_duplicados = dfs['CASA DE COBRANZA'].drop_duplicates(subset=[merge_conf['casa_cobranza'][1]])
        df = self._merge_dataframes(df, casa_cobranza_sin_duplicados, *merge_conf['casa_cobranza'])
        df = self._merge_dataframes(df, dfs['CODEUDORES'], *merge_conf['codeudores'])

        return df

    def _calculate_final_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calcula todas las columnas derivadas para el reporte final."""
        df.rename(columns={'SALDO': 'SALDOS'}, inplace=True)
        df['SALDOS'] = pd.to_numeric(df['SALDOS'], errors='coerce').fillna(0)
        df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce').fillna(0)
        df.loc[df['FACTURA FINAL'] == 'Mas de una cartera', 'SALDOS'] = 0

        df['VALIDACION ULTIMO SALDO'] = np.where((df['SALDOS'] - df['Valor']) <= 0, 'pago total', (df['SALDOS'] - df['Valor']).astype(str))
        
        df['EMPLEADO'] = df['ESTADO_EMPLEADO'].fillna('NO')
        df['CASA COBRANZA'] = df['CASA COBRANZA'].fillna('SIN CASA DE COBRANZA')
        df['CODEUDOR'] = df['CODEUDOR'].fillna('SIN CODEUDOR')

        novedad_conds = [df['CASA COBRANZA'] != 'SIN CASA DE COBRANZA', df['CODEUDOR'] != 'SIN CODEUDOR', df['VALIDACION ULTIMO SALDO'] == 'pago total']
        novedad_vals = [df['CASA COBRANZA'], 'Codeudor:' + df['CODEUDOR'], 'Pago total']
        df['Novedad'] = np.select(novedad_conds, novedad_vals, default='Sin novedad')

        empresa_conds = [
            (df['FACTURA FINAL'] != 'SIN CARTERA') & df['FACTURA FINAL'].str.startswith('DF', na=False),
            (df['FACTURA FINAL'] != 'SIN CARTERA') & ~df['FACTURA FINAL'].str.startswith('DF', na=False),
            (df['FACTURA FINAL'] == 'SIN CARTERA') & (df['CODEUDOR'] != 'SIN CODEUDOR') & df['CODEUDOR'].str.startswith('DF', na=False),
            (df['FACTURA FINAL'] == 'SIN CARTERA') & (df['CODEUDOR'] != 'SIN CODEUDOR') & ~df['CODEUDOR'].str.startswith('DF', na=False)
        ]
        df['Empresa'] = np.select(empresa_conds, ['Finansueños', 'Arpesod', 'Finansueños', 'Arpesod'], default='')

        df['Valor Aplicar'] = np.where((df['VALIDACION ULTIMO SALDO'] == 'pago total') & (df['SALDOS'] != 0), df['SALDOS'], df['Valor'])
        
        dif_aprovechamiento = df['Valor'] - df['SALDOS']
        df['Valor Aprovechamientos'] = np.where((dif_aprovechamiento > 0) & (dif_aprovechamiento <= 10000), dif_aprovechamiento, 0)
        
        dif_anticipo = df['Valor'] - df['Valor Aplicar']
        df['Valor Anticipos'] = np.where(dif_anticipo >= 10000, dif_anticipo, 0)

        return df

    def _cleanup_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Elimina columnas temporales y renombra las finales para presentación."""
        # Lista exhaustiva de columnas a eliminar, incluyendo las generadas por los merges
        cols_to_drop = [
            'ESTADO_EMPLEADO', 'CEDULA_FS', 'FACTURA_FS', 'CEDULA_ARP', 'FACTURA_ARP', 
            'TOTAL_CUENTAS', 'SALDO_FS', 'SALDO_ARP', 'CENTRO_COSTO_FS', 'CENTRO_COSTO_ARP',
            'vincedula', 'FACTURA', 'DOCUMENTO_CODEUDOR', 'FACTURA_x', 'FACTURA_y',
            'CEDULA_FS_x', 'CEDULA_ARP_x', 'CEDULA_FS_y', 'CEDULA_ARP_y'
        ]
        df.drop(columns=[col for col in cols_to_drop if col in df.columns], inplace=True, errors='ignore')
        
        df.rename(columns={
            'FACTURA FINAL': 'Documento Cartera', 'CENTRO COSTO': 'C. Costo',
            'CASA COBRANZA': 'Casa cobranza', 'EMPLEADO': 'Empleado',
            'CANTIDAD CUENTAS ARP': 'Cuentas ARP', 'CANTIDAD CUENTAS FS': 'Cuentas FS'
        }, inplace=True)

        return df

    def _clean_reference(self, value):
        try: return str(int(float(value))) if pd.notna(value) and value != '' else ''
        except (ValueError, TypeError): return ''

    def _highlight_accounts(self, row):
        arp, fs = row['Cuentas ARP'], row['Cuentas FS']
        styles = [''] * len(row)
        if (arp >= 2) or (fs >= 2) or (arp == 1 and fs == 1):
            styles = ['background-color: lightcoral'] * len(row)
        return styles

    def _highlight_employees_and_duplicates(self, df_styled):
        styles = pd.DataFrame('', index=df_styled.index, columns=df_styled.columns)
        empleado_mask = df_styled['Empleado'].str.upper().str.strip() == 'SI'
        styles.loc[empleado_mask, :] = 'background-color: lightblue'
        dup_mask = df_styled.duplicated('Documento Cartera', keep=False) & (df_styled['Documento Cartera'] != 'SIN CARTERA')
        styles.loc[dup_mask, :] = 'background-color: yellow'
        return styles
    
    def save_report(self, output_path: str, df_bancolombia: pd.DataFrame, df_efecty: pd.DataFrame):
        
        if df_bancolombia.empty and df_efecty.empty:
            raise ValueError("No se encontraron datos de pago (Bancolombia o Efecty) para generar el reporte.")

        # --- INICIO DE CAMBIOS ---

        # 1. FORMATEAR FECHAS a dd/mm/yyyy
        for df in [df_bancolombia, df_efecty]:
            if 'Fecha' in df.columns:
                # errors='coerce' previene errores si alguna fecha no es válida
                df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce').dt.strftime('%d/%m/%Y')

        # --- FIN DE CAMBIOS ---

        # El resto del método se mantiene como lo tenías...
        if 'Referencia 1' in df_bancolombia.columns:
            df_bancolombia['Referencia 1'] = df_bancolombia['Referencia 1'].apply(self._clean_reference)
        if 'Referencia 2' in df_bancolombia.columns:
            df_bancolombia['Referencia 2'] = df_bancolombia['Referencia 2'].apply(self._clean_reference)

        COLUMN_ORDER_EFECTY = [
            'No', 'Identificación', 'Valor', 'N° de Autorización', 'Fecha', 'Documento Cartera', 
            'C. Costo', 'Empresa', 'Valor Aplicar', 'Valor Anticipos', 'Valor Aprovechamientos', 
            'Casa cobranza', 'Empleado', 'Novedad','Cuentas ARP', 'Cuentas FS','SALDOS','VALIDACION ULTIMO SALDO'
        ]
        COLUMN_ORDER_BANCOLOMBIA = [
            'No.', 'Fecha', 'Detalle 1', 'Detalle 2', 'Referencia 1', 'Referencia 2', 'Valor', 
            'Documento Cartera', 'C. Costo', 'Empresa', 'Valor Aplicar', 'Valor Anticipos', 
            'Valor Aprovechamientos', 'Casa cobranza', 'Empleado', 'Novedad', 'Cuentas ARP', 
            'Cuentas FS','SALDOS','VALIDACION ULTIMO SALDO'
        ]

        print("DEBUG columnas FALTANTES BANCOLOMBIA:", set(COLUMN_ORDER_BANCOLOMBIA) - set(df_bancolombia.columns))
        print("DEBUG columnas FALTANTES EFECTY:", set(COLUMN_ORDER_EFECTY) - set(df_efecty.columns))
        df_bancolombia = df_bancolombia.reindex(columns=COLUMN_ORDER_BANCOLOMBIA)
        df_efecty = df_efecty.reindex(columns=COLUMN_ORDER_EFECTY)

        def is_valid(df):
            return not df.empty and df.dropna(how='all').shape[0] > 0 and df.dropna(axis=1, how='all').shape[1] > 0

        try:
            temp_path = Path(output_path)
            temp_dir = temp_path.parent
            temp_file = temp_dir / ("temp_" + temp_path.name)

            with pd.ExcelWriter(temp_file, engine='openpyxl') as writer:
                wrote = False

                if is_valid(df_bancolombia):
                    styled_bancolombia = df_bancolombia.style \
                        .apply(self._highlight_accounts, axis=1) \
                        .apply(self._highlight_employees_and_duplicates, axis=None)
                    styled_bancolombia.to_excel(writer, sheet_name='Bancolombia', index=False)
                    wrote = True

                if is_valid(df_efecty):
                    styled_efecty = df_efecty.style \
                        .apply(self._highlight_accounts, axis=1) \
                        .apply(self._highlight_employees_and_duplicates, axis=None)
                    styled_efecty.to_excel(writer, sheet_name='Efecty', index=False)
                    wrote = True

                if not wrote:
                    pd.DataFrame({'Mensaje': ['No hay datos para mostrar']}).to_excel(writer, sheet_name='Diagnóstico')

            os.replace(temp_file, output_path)
            print(f"✅ Reporte guardado exitosamente en {Path(output_path).resolve()}")

        except Exception as e:
            raise ValueError(f"❌ Error al guardar archivo Excel con estilos: {str(e)}")
        

        