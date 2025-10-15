configuracion = {
     "VENCIMIENTOS": {
        "usecols": ["MCNVINCULA", "SALDODOC", "VENCE","MCNCUOCRU1","MCNTIPCRU1","MCNNUMCRU1","INTERES"],
        "rename_map": {"MCNTIPCRU1":"Tipo_Credito", 
                       "MCNNUMCRU1":"Numero_Credito", 
                       "MCNVINCULA": "Cedula_Cliente", 
                       "SALDODOC": "Valor_Cuota", 
                       "MCNCUOCRU1": "Cuota_Vigente",
                       "INTERES":"Intereses", 
                       "VENCE": "Fecha_Cuota_Vigente" }
    }
}