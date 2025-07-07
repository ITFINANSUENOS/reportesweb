# --- 1. CONFIGURACIÓN DE PROCESAMIENTO POR TIPO DE ARCHIVO ---
configuracion = {
    "ANALISIS": {
        "usecols": ["direccion", "barrio", "nomciudad",
                     "diasatras", "cuotaspag","cedula","saldofac","tipo","numero"],
        "rename_map": { "direccion": "Direccion",
                        "barrio": "Barrio",
                        "nomciudad": "Nombre_Ciudad",
                        "diasatras": "Dias_Atraso", 
                        "cuotaspag": "Cuotas_Pagadas", 
                        "cedula" : "Cedula_Cliente", 
                        "saldofac":"Saldo_Factura", 
                        "tipo":"Tipo_Credito", 
                        "numero":"Numero_Credito" }
    },
    "R91": {
        "usecols": ["VINNOMBRE", "VENNOMBRE", "MCDZONA", "MCDVINCULA", "MCDNUMCRU1", 
                    "MCDTIPCRU1","VENOMBRE","VENCODIGO", "MCDCCOSTO", "CCONOMBRE", 
                    "META_INTER", "META_DC_AL", "META_DC_AT", "META_ATRAS"],
        "rename_map": { 
                       "MCDTIPCRU1": "Tipo_Credito", 
                       "MCDNUMCRU1": "Numero_Credito", 
                       "MCDVINCULA" : "Cedula_Cliente", 
                       "VINNOMBRE": "Nombre_Cliente", 
                       "MCDZONA" : "Zona", 
                       "VENNOMBRE" : "Nombre_Vendedor", 
                       "VENCODIGO" : "Codigo_Vendedor", 
                       "CCONOMBRE" : "Centro_Costos", 
                       "MCDCCOSTO" : "Codigo_Centro_Costos", 
                       "META_INTER" : "Meta_Intereses", 
                       "META_DC_AL" : "Meta_DC_Al_Dia", 
                       "META_DC_AT" : "Meta_DC_Atraso", 
                       "META_ATRAS" : "Meta_Atraso" }
    },
    "VENCIMIENTOS": {
        "usecols": ["MCNVINCULA", "VINTELEFO3", "SALDODOC", "VENCE", "VINTELEFON", "MCNCUOCRU1","MCNTIPCRU1","MCNNUMCRU1"],
        "rename_map": {"MCNTIPCRU1":"Tipo_Credito", 
                       "MCNNUMCRU1":"Numero_Credito", 
                       "MCNVINCULA": "Cedula_Cliente", 
                       "VINTELEFO3": "Celular", 
                       "VINTELEFON" : "Telefono", 
                       "SALDODOC": "Valor_Cuota_Vigente", 
                       "MCNCUOCRU1": "Cuota_Vigente", 
                       "VENCE": "Fecha_Cuota_Vigente" }
    },
    "R03":{
        "usecols": ["CODEUDOR1","NOMBRE1","VINTELEFON","CIUNOMBRE1","CODEUDOR2","NOMBRE2","VINTELEFO2","CIUNOMBRE2","CEDULA"],
        "rename_map": { 
                       "CODEUDOR1": "Codeudor1", 
                       "NOMBRE1": "Nombre_Codeudor1", 
                       "VINTELEFON": "Telefono_Codeudor1", 
                       "CIUNOMBRE1": "Ciudad_Codeudor1", 
                       "CODEUDOR2": "Codeudor2", 
                       "NOMBRE2": "Nombre_Codeudor2", 
                       "VINTELEFO2": "Telefono_Codeudor2", 
                       "CIUNOMBRE2": "Ciudad_Codeudor2", 
                       "CEDULA": "Cedula_Cliente" }
    },
    "SC04":{
        "usecols":["FACTURA","SLCVALOR","SLCNCUOTAS"],
        "rename_map":{ 
                        "FACTURA": "Factura_Venta",
                        "SLCVALOR": "Valor_Cuota", 
                        "SLCNCUOTAS": "Total_Cuotas"
        }
    },
    "CRTMPCONSULTA1":{
        "usecols":["CORREO","FECHA_FACT","TIPO_DOCUM","NUMERO_DOC","IDENTIFICA"],
        "rename_map":{ 
                      "CORREO": "Correo", 
                      "FECHA_FACT":"Fecha_Facturada", 
                      "TIPO_DOCUM":"Tipo_Credito", 
                      "NUMERO_DOC":"Numero_Credito", 
                      "IDENTIFICA":"Cedula_Cliente" }
    },
    "FNZ003":{
        "usecols":["CONCEPTO","SALDO","DESEMBOLSO", "NUMERO"],
        "rename_map":{ 
                      "DESEMBOLSO":"Tipo_Credito",
                      "NUMERO": "Numero_Credito",  
                      "CONCEPTO":"Concepto", 
                      "SALDO":"Saldo" }
    },
    "MATRIZ_CARTERA": {
        "skiprows": 2, "header": None, 
        "new_names": ['Zona', 
                      'Cobrador', 
                      'Telefono_Cobrador', 
                      'Regional', 
                      'Gestor', 
                      'Telefono_Gestor', 
                      'call_center_1_30_dias', 'call_center_nombre_1_30', 'call_center_telefono_1_30', 
                      'call_center_31_90_dias', 'call_center_nombre_31_90', 'call_center_telefono_31_90', 
                      'call_center_91_360_dias', 'call_center_nombre_91_360', 'call_center_telefono_91_360'],
        "merge_on": "Zona" 
    },
     "METAS_FRANJAS":{
        "usecols":["ZONA","1 A 30","31 A 90","91 A 180","181 A 360","T.R"],
        "rename_map":{ "ZONA":"Zona", 
                      "1 A 30":"Meta_1_A_30", 
                      "31 A 90":"Meta_31_A_90",
                      "91 A 180":"Meta_91_A_180",
                      "181 A 360":"Meta_181_A_360",
                      "T.R":"Total_Recaudo" }
    },
    "ASESORES": {
        "sheets": [{ 
              "sheet_name": "ASESORES", 
              "usecols": ["NOMBRE ASESOR", "MOVIL ASESOR", "LIDER ZONA", "MOVIL LIDER"], 
              "rename_map": { "NOMBRE ASESOR": "Nombre_Vendedor",
                              "MOVIL ASESOR": "Movil_Vendedor", 
                              "LIDER ZONA": "Lider_Zona", 
                              "MOVIL LIDER": "Movil_Lider" }, 
              "merge_on": "Nombre_Vendedor"
              },
            { 
                "sheet_name": "Centro Costos",
              "usecols": ["CENTRO DE COSTOS", "ACTIVO"], 
              "rename_map": { "CENTRO DE COSTOS": "Codigo_Centro_Costos",
                              "ACTIVO": "Activo_Centro_Costos" }, 
              "merge_on": "Codigo_Centro_Costos" 
              }
        ]
    },
    "DESEMBOLSOS_FINANSUEÑOS": {
        "sheets": [{ 
              "sheet_name": "Page 001", 
              "usecols": ["CRÉDITO","VLR_FNZ","CUOTAS","VLR_CUOTA"], 
              "rename_map": { 
                              "CRÉDITO":"Credito",
                              "VLR_FNZ":"Valor_Desembolso",
                              "CUOTAS":"Total_Cuotas",
                              "VLR_CUOTA":"Valor_Cuota",
               }, 
              "merge_on": "Credito" 
              },
        ]
    }
}

ORDEN_COLUMNAS_FINAL = [
    # --- Identificadores Principales ---
    'Credito',
    'Factura_Venta',
    'Empresa',
    'Cedula_Cliente',
    'Nombre_Cliente',
    # --- Fechas ---
    'Fecha_Facturada',
    # --- Información de Contacto y Ubicación ---
    'Direccion',
    'Barrio',
    'Nombre_Ciudad',
    'Telefono',
    'Celular',
    'Correo',
    # --- Detalles del Crédito ---
    'Tipo_Credito',
    'Numero_Credito',
    'Total_Cuotas',
    'Valor_Cuota',
    'Valor_Desembolso',
    # --- Estado de Mora ---
    'Dias_Atraso',
    'Franja_Mora',
    'Cuotas_Pagadas',
    'Cuota_Vigente',
    'Valor_Cuota_Vigente',
    'Fecha_Cuota_Vigente',
    # --- Saldos ---
    'Saldo_Factura',
    'Saldo_Capital',
    'Saldo_Avales',
    'Saldo_Interes_Corriente',
    # --- Personal Asignado ---
    'Zona',
    'Cobrador',
    'Telefono_Cobrador',
    'Centro_Costos',
    'Codigo_Centro_Costos',
    'Activo_Centro_Costos',
    'Codigo_Vendedor',
    'Nombre_Vendedor',
    'Movil_Vendedor',
    'Lider_Zona',
    'Movil_Lider',
    "Regional",
    "Gestor",
    "Telefono_Gestor",
    # --- Información de Call Center ---
    'Call_Center_Apoyo',
    'Nombre_Call_Center',
    'Telefono_Call_Center',
    # --- Codeudores ---
    'Codeudor1',
    'Nombre_Codeudor1',
    'Telefono_Codeudor1',
    'Ciudad_Codeudor1',
    'Codeudor2',
    'Nombre_Codeudor2',
    'Telefono_Codeudor2',
    'Ciudad_Codeudor2',
    # --- Datos Internos/Metas ---
    'Meta_Intereses',
    'Meta_DC_Al_Dia',
    'Meta_DC_Atraso',
    'Meta_Atraso',
    'Meta_General',
    'Meta_%',
    'Meta_$',
    'Meta_T.R_%',
    'Meta_T.R_$' 
]