configuracion = {
    "NOVEDADES":{
        "usecols":["VNTCEDULA","NOTA","VNTNEWFEC","VNTNEWUSER","VNTTIPO",
                   "VTTNOMBRE","VALOR","ALARMA"],
        "rename_map":{
                      "VNTCEDULA":"Cedula_Cliente",
                      "VNTTIPO":"Codigo_Novedad",
                      "VTTNOMBRE":"Tipo_Novedad",
                      "ALARMA":"Fecha_Compromiso",
                      "VNTNEWFEC":"Fecha_Novedad",
                      "VNTNEWUSER":"Usuario_Novedad",
                      "NOTA":"Novedad",
                      "VALOR":"Valor"
                      }
    },
    "ANALISIS":{
        "engine": "xlrd",
        "usecols":["diasatras","tipo","numero","ultpago"],
        "rename_map":{
                      "tipo":"Tipo_Credito", 
                      "numero":"Numero_Credito",
                      "diasatras":"Dias_Atraso_Final",
                      "ultpago":"Fecha_Ultimo_pago"
        }
    },
    "R91": {
        "usecols": ["COBRO_DC_A","COBRO_DC_2","COBRO_ATRA","COBRO_ANTI","MCDTIPCRU1","MCDNUMCRU1"],
        "rename_map": { 
                       "MCDNUMCRU1": "Numero_Credito",
                       "MCDTIPCRU1": "Tipo_Credito", 
                       "COBRO_DC_A": "Recaudo_DC_Al_Dia",
                       "COBRO_DC_2": "Recaudo_DC_Atraso",
                       "COBRO_ATRA": "Recaudo_Atraso",
                       "COBRO_ANTI":"Recaudo_Anticipado"
                        }
    },
    "USUARIOS": {
        "usecols": ["CEDULA","NOMBRE","USUARIO","AGRUPACIÓN","NUMERO CORPORATIVO"],
        "rename_map": { 
                       "AGRUPACIÓN": "Cargo_Usuario",
                       "NOMBRE": "Nombre_Usuario",
                       "CEDULA":"Cedula_Usuario", 
                       "USUARIO": "Usuario_Novedad",
                       "NUMERO CORPORATIVO": "Celular_Corporativo"
                        }
    },
    #Tipos de datos para reporte base
    "BASE_MENSUAL": {
        "dtype_map": {
            'Cedula_Cliente': str,
            'Numero_Credito': str,
            'Primera_Cuota_Mora':str,
            'Cantidad_Producto':int,                    
            'Cantidad_Obsequio':int,
            'Valor_Cuota': float,        
            'Saldo_Capital': float,
            'Valor_Desembolso': float,               
            'Meta_Intereses':float,          
            'Meta_General':float,
            'Meta_Saldo':float,
            'Meta_$':float,
            'Meta_T.R_$':float,
            'Valor_Cuota_Atraso':str,
            'Valor_Vencido':float,
            'Valor_Cuota_Vigente':str,
            'Total_Cuotas': 'Int64',
            'Cuotas_Pagadas':'Int64',                    
            'Dias_Atraso':'Int64',

        }
    }
}