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
        "usecols": ["COBRO_DC_A","COBRO_DC_2","COBRO_ATRA","COBRO_ANTI"],
        "rename_map": { 
                        }
    }
}