configuracion = {
     "VENCIMIENTOS": {
        "usecols": ["MCNVINCULA", "SALDODOC", "VENCE","MCNCUOCRU1","MCNTIPCRU1","MCNNUMCRU1","INTERES", "VINNOMBRE"],
        "rename_map": {"MCNTIPCRU1":"Tipo_Credito", 
                       "MCNNUMCRU1":"Numero_Credito", 
                       "MCNVINCULA": "Cedula_Cliente", 
                       "VINNOMBRE":"Nombre_Cliente",
                       "SALDODOC": "Valor_Cuota", 
                       "MCNCUOCRU1": "Cuota_Vigente",
                       "INTERES":"Intereses", 
                       "VENCE": "Fecha_Cuota_Vigente" }
    },
    "CRTMPCONSULTA1":{
        "usecols":["CORREO","TIPO_DOCUM","NUMERO_DOC","IDENTIFICA"],
        "rename_map":{ 
                      "CORREO": "Correo", 
                      "TIPO_DOCUM":"Tipo_Credito", 
                      "NUMERO_DOC":"Numero_Credito", 
                      "IDENTIFICA":"Cedula_Cliente" }
    },
}