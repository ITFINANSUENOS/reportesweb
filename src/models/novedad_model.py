configuracion = {
    "NOVEDADES":{
        "usecols":["VNTCEDULA","NOTA","VNTNEWFEC","VNTNEWUSER"],
        "rename_map":{
                      "VNTCEDULA":"Cedula_Cliente",
                      "NOTA":"Tipo_Novedad",
                      "VNTNEWFEC":"Fecha_Novedad",
                      "VNTNEWUSER":"Usuario_Novedad"
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
    }
}