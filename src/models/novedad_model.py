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
        "usecols":["cedula","diasatras",""]
    }
}