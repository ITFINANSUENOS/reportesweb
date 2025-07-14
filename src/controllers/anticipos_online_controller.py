import os
import numpy as np
import pandas as pd
from tkinter import filedialog, messagebox
from src.models.anticipos_online_processor import AnticiposOnlineProcessor

class AnticiposController:
    def __init__(self,view):
        self.view = view
        self.data_processor = AnticiposOnlineProcessor()
    
    def select_file(self):
        filetypes = [
            ('Archivos Excel','*xlsx'),
            ('Todos los archivos','*.*')
        ]
        
        file_path = filedialog.askopenfilename(
            title = 'Selecciona el archivo Excel a procesar',
            initialdir= os.path.expanduser('~'),
            filetypes=filetypes
        )    
        if file_path:
            self.view.progress_bar['value'] = 0
            self.view.root.after(100, lambda: self.process_file(file_path))
    
    def process_file(self, file_path: str):
        try:
            self.view.update_status("Validando archivo...")
            self.view.update_progress(10)
        
            self.view.update_status("Cargando y filtrando datos...")
            dfs = self.data_processor.load_and_filter_data(file_path)
            self.view.update_progress(20)
        
      
            self.view.update_status("Asegurando compatibilidad de tipos (CEDULA)...")
            df_online = dfs['ONLINE']
            df_ac_fs = dfs['AC FS']
            df_ac_arp = dfs['AC ARP']
        
            df_online['CEDULA'] = df_online['CEDULA'].astype(str).str.strip()
            df_ac_fs['CEDULA'] = df_ac_fs['CEDULA'].astype(str).str.strip()
            df_ac_arp['CEDULA'] = df_ac_arp['CEDULA'].astype(str).str.strip()
            self.view.update_progress(30)
            
             # Contar para AC FS
            counts_fs = df_ac_fs['CEDULA'].value_counts()
            df_online['CUENTAS_FS'] = df_online['CEDULA'].map(counts_fs).fillna(0).astype(int)

            # Contar para AC ARP
            counts_arp = df_ac_arp['CEDULA'].value_counts()
            df_online['CUENTAS_ARP'] = df_online['CEDULA'].map(counts_arp).fillna(0).astype(int)

            self.view.update_status("Fusionando datos de 'AC FS'...")
            merged_df = pd.merge(left=df_online, right=df_ac_fs, on='CEDULA', how='left')
            self.view.update_progress(50)
        
            self.view.update_status("Fusionando datos de 'AC ARP'...")
            final_df = pd.merge(left=merged_df, right=df_ac_arp, on='CEDULA', how='left')
            self.view.update_progress(70)
            
            
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

            opciones = [
                'REVISAR TIENE 2 CARTERAS',
                'PAGO TOTAL',
                'CARTERA EN FINANSUEÑOS',
                'CARTERA EN ARPESOD'
                ]
            
            final_df['OBSERVACIONES'] = np.select(condiciones, opciones, default='REVISAR SI ES CODEUDOR') 
            
            multi_cartera_mask = final_df['OBSERVACIONES'] == 'REVISAR TIENE 2 CARTERAS'
            final_df.loc[multi_cartera_mask, ['FACTURA_FS', 'FACTURA_ARP']] = 'MAS DE UNA CARTERA'
        
            self.view.update_status("Solicitando ubicación para guardar...")
            default_filename = self.data_processor.config.output_filename
        
            output_path = filedialog.asksaveasfilename(
               title="Guardar reporte como...",
               initialdir=os.path.expanduser("~/Desktop"),
               initialfile=default_filename,
               defaultextension=".xlsx",
               filetypes=[("Archivos de Excel", "*.xlsx"), ("Todos los archivos", "*.*")]
               )
            if output_path:
               self.view.update_status("Guardando el reporte final...")

               self.data_processor.save_formatted_excel(final_df, output_path)
               self.view.update_progress(90)
               messagebox.showinfo("Éxito", f"El archivo ha sido generado exitosamente en:\n{output_path}")
            else:
               self.view.update_status("Guardado cancelado por el usuario.")
               self.view.update_progress(0)
            return
            
        except Exception as e:
            messagebox.showerror("Error", f"Error al procesar el archivo:\n {str(e)}")
            self.view.update_status("Error al procesar el archivo.\n")
            return False
        finally:
           self.view.update_progress(100)
           self.view.update_status("Proceso completado.")