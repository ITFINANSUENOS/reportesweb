# Columnas específicas para diferentes conjuntos de datos
# --- COLUMNAS PARA CARTERA ---
COLS_CARTERA = [
        "Fecha_Desembolso", "Fecha_Ultima_Novedad", "Empresa", "Regional_Venta",
        "Nombre_Ciudad", "Nombre_Vendedor", "Franja_Meta", "Rodamiento", "Gestor",
        "Regional_Cobro", "Zona_Cobro", "Zona","Factura_Venta","Fecha_Facturada",
        "Cantidad_Novedades", "Cedula_Cliente", "Credito", "Nombre_Producto",
        "Obsequio", "Nombre_Cliente", "Correo", "Celular", "Direccion", "Barrio",
        "Nombre_Codeudor2", "Cobrador", "Telefono_Cobrador", "Call_Center_Apoyo",
        "Codigo_Vendedor", "Nombre_Call_Center", "Telefono_Call_Center",
        "Telefono_Gestor", "Valor_Desembolso", "Movil_Vendedor", "Vendedor_Activo",
        "Lider_Zona", "Codeudor1", "Total_Cuotas", "Nombre_Codeudor1",
        "Telefono_Codeudor1", "Ciudad_Codeudor1", "Codeudor2",
        "Telefono_Codeudor2", "Ciudad_Codeudor2", "Valor_Cuota", "Dias_Atraso", "Franja_Cartera",
        "Meta_Intereses", "Meta_Saldo", "Meta_%", "Meta_$", "Meta_T.R_%","Meta_General",
        "Meta_T.R_$", "Cuotas_Pagadas", "Fecha_Cuota_Atraso", "Primera_Cuota_Mora",
        "Valor_Cuota_Atraso", "Valor_Vencido", "Dias_Atraso_Final","Fecha_Ultimo_pago","Rango_Ultimo_pago",
        "Franja_Meta_Final", "Franja_Cartera_Final", "Rodamiento_Cartera",'Cuota_Vigente',"Valor_Cuota_Vigente",
        "Recaudo_Anticipado", "Recaudo_Meta", "Total_Recaudo", "Fecha_Cuota_Vigente","Total_Recaudo_Sin_Anti"
]

# --- COLUMNAS PARA NOVEDADES ---
COLS_NOVEDADES = [
    "Fecha_Novedad", "Cedula_Cliente", "Nombre_Cliente", "Usuario_Novedad",
    "Nombre_Usuario", "Cargo_Usuario", "Celular_Corporativo", "Tipo_Novedad",
    "Novedad", "Fecha_Compromiso", "Valor","Empresa","Celular_Cliente","Telefono_Cliente"
]

# --- COLUMNAS PARA LLAMADAS ---
COLS_LLAMADAS = [
    "Fecha_Llamada", "Extension_Llamada", "Destino_Llamada", "Estado_Llamada", "Duracion_Llamada",
    "Codigo_Llamada", "Grabacion_Llamada", "Call_Center", "Nombre_Call"
]

# --- COLUMNAS PARA MENSAJERIA ---
COLS_MENSAJERIA = [
    "Codigo_Pais", "Numero_Telefono", "Nombre_Saliente", "Estado", "Estado_Mensaje", "Estado_Respuesta_Saliente",
    "Respuesta_Saliente", "Flujo_Truora", "Primer_Mensaje_Agente", "Fecha_Llamada", "Call_Center", "Nombre_Call"
]
# --- MAPEO DE COLUMNAS PARA FNZ ---
MAPA_FNZ = {
    'ESTADO':'Estado', 'ANALISTA':'Analista_Asociado', 'FECHA':'Fecha', 'REGIONAL':'Regional_Venta',
    'DESEMBOLSO':'Credito_Desembolsado', 'CEDULA':'Cedula_Cliente', 'FS1NACFEC':'Fecha_Nacimiento',
    'APELLIDOS':'Apellidos', 'NOMBRES':'Nombres', 'TELEFONO1':'Celular1', 'MOVIL':'Celular2',
    'FS1EMAIL':'Correo', 'CARGO':'Cargo', 'DIRECCION':'Direccion', 'CODCIUDAD':'Codigo_Ciudad',
    'CIUDAD':'Ciudad', 'BARRIO':'Barrio', 'VENNOMBRE':'Nombre_Vendedor', 'CCONOMBRE':'Centro_Costo',
    'CUOTAS':'Total_Cuotas', 'FS0NOTA':'Nota', 'VALOR_TOTA':'Valor_Total', 'INGRESOS':'Ingresos', 
    'GASTOS':'Gastos', 'PAGARE': 'Pagare'
}
# Columnas específicas para guardado final segun su tabla 
COLS_TABLA_NOVEDADES = [
    'Empresa','Credito', 'Nombre_Cliente', 'Cedula_Cliente', 'Celular', 'Nombre_Ciudad', 'Zona','Dias_Atraso_Final', 
    'Total_Recaudo', 'Valor_Vencido', 'Estado_Pago','Estado_Gestion',"Regional_Cobro", 'Cargo_Usuario', 'Nombre_Usuario','Novedades_Por_Cargo',
    'Codeudor1', 'Nombre_Codeudor1', 'Telefono_Codeudor1','Codeudor2', 'Nombre_Codeudor2','Telefono_Codeudor2', 
    'Fecha_Cuota_Vigente', 'Valor_Cuota_Vigente','Meta_$','Novedad', 'Tipo_Novedad',
    'CALL_CENTER_FILTRO', 'Tipo_Vigencia_Temp'
]

COLS_TABLA_RODAMIENTOS = [
    'Empresa', 'Credito', 'Cedula_Cliente', 'Nombre_Cliente', 'Celular', 'Fecha_Cuota_Vigente', 'Valor_Cuota_Vigente',
    'Nombre_Ciudad',"Regional_Cobro", 'Zona', 'Codeudor1', 'Nombre_Codeudor1', 'Telefono_Codeudor1','Codeudor2', 'Nombre_Codeudor2', 'Franja_Cartera',
    'Telefono_Codeudor2','Dias_Atraso_Final', 'Total_Recaudo', 'Meta_Intereses', 'Meta_Saldo', 'Valor_Vencido','Rodamiento',
    'Rodamiento_Cartera','Estado_Pago', 'Estado_Gestion', 'Meta_$',
    'CALL_CENTER_FILTRO', 'Tipo_Vigencia_Temp'
]