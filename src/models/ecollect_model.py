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
    "COLABORADORES": {
        "sheets": [
            { 
              "sheet_name": "CARTERA",
              "usecols": ["CEDULA", "CREDITO","CUOTA", "MENSAJE PAGO","FECHA PAGO","VLR PAGO"], 
              "rename_map": { 
                              "CEDULA": "Cedula_Cliente",
                              "CREDITO": "Credito",
                              "CUOTA": "Cuota_Vigente",
                              "MENSAJE PAGO":"Primera_Cuota_Atraso",
                              "FECHA PAGO":"Fecha_Atraso",
                              "VLR PAGO":"Valor"},
              },
              { 
              "sheet_name": "USUARIOS",
              "usecols": ["CEDULA", "NOMBRE","CORREO"], 
              "rename_map": { 
                              "CEDULA": "Cedula_Cliente",
                              "NOMBRE": "Nombre_Cliente",
                              "CORREO":"Correo"},
              }
        ]
    }
}