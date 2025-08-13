configuracion = {
    "NOVEDADES":{
        "usecols":["VNTCEDULA","NOTA","VNTNEWFEC","VNTNEWUSER",
                   "VTTNOMBRE","VALOR","ALARMA"],
        "rename_map":{
                      "VNTCEDULA":"Cedula_Cliente",
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
        "usecols":["diasatras","tipo","numero"],
        "rename_map":{
                      "tipo":"Tipo_Credito", 
                      "numero":"Numero_Credito",
                      "diasatras":"Dias_Atraso_Final"
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
    }
}