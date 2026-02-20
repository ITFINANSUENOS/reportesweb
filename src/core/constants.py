COLS_CARTERA = [
    'Empresa', 'Credito', 'Fecha_Desembolso', 'Factura_Venta', 'Fecha_Facturada',
    'Nombre_Producto', 'Cantidad_Producto', 'Obsequio', 'Cantidad_Obsequio',
    'Cedula_Cliente', 'Nombre_Cliente', 'Correo','Celular','Direccion', 'Barrio',
    'Nombre_Ciudad','Zona', 'Cobrador','Telefono_Cobrador', 'Zona_Cobro',
    'Call_Center_Apoyo', 'Nombre_Call_Center','Telefono_Call_Center', 'Regional_Cobro',
    'Gestor', 'Telefono_Gestor','Jefe_ventas', 'Codigo_Vendedor','Cedula_Vendedor',
    'Nombre_Vendedor','Vendedor_Activo','Zona_Venta','Lider_Zona', 'Codigo_Centro_Costos',
    'Regional_Venta', 'Codeudor1', 'Nombre_Codeudor1', 'Telefono_Codeudor1',
    'Ciudad_Codeudor1', 'Codeudor2', 'Nombre_Codeudor2', 'Telefono_Codeudor2',
    'Ciudad_Codeudor2', 'Valor_Desembolso', 'Total_Cuotas', 'Valor_Cuota',
    'Dias_Atraso', 'Franja_Meta','Franja_Cartera', 'Saldo_Capital', 'Saldo_Interes_Corriente',
    'Saldo_Avales', 'Meta_Intereses', 'Meta_General','Meta_Saldo', 'Meta_%', 'Meta_$',
    'Meta_T.R_%', 'Meta_T.R_$', 'Cuotas_Pagadas', 'Cuota_Vigente',
    'Fecha_Cuota_Vigente', 'Valor_Cuota_Vigente', 'Fecha_Cuota_Atraso',
    'Primera_Cuota_Mora', 'Fecha_Ultimo_Pago_Inicial', 'Rango_Ultimo_pago_Inicial',
    'Valor_Cuota_Atraso', 'Valor_Vencido','Fecha_Ultima_Novedad', 'Cantidad_Novedades','Fecha_Ultimo_pago','Rango_Ultimo_pago', 'Dias_Atraso_Final',
    'Franja_Meta_Final','Franja_Cartera_Final', 'Rodamiento','Rodamiento_Cartera' ,
    'Recaudo_Anticipado', 'Recaudo_Meta','Total_Recaudo','Total_Recaudo_Sin_Anti'
]

COLS_NOVEDADES = [
    'Empresa','Cedula_Cliente', 'Nombre_Cliente', 'Fecha_Novedad', 'Usuario_Novedad','Nombre_Usuario',
    'Telefono_Cliente','Celular_Cliente','Cargo_Usuario', 'Celular_Corporativo','Codigo_Novedad', 'Tipo_Novedad',
    'Novedad', 'Fecha_Compromiso', 'Valor'
]

COLS_LLAMADAS = [
    "Fecha_Llamada", "Extension_Llamada", "Destino_Llamada", "Estado_Llamada", "Duracion_Llamada",
    "Codigo_Llamada", "Grabacion_Llamada", "Call_Center", "Nombre_Call"
]

COLS_MENSAJERIA = [
    "Fecha_Mensaje", "Numero_Telefono", "Nombre_Saliente", "Estado", "Estado_Mensaje", "Estado_Respuesta_Entrante",
    "Flujo_Truora", "Estado_Proceso", "Fallo_Proceso","Tipo_Respuesta_Agente","Call_Center", "Nombre_Call"
]

# 2. MAPEOS Y DICCIONARIOS DE TRANSFORMACIÓN
# Diccionarios usados para renombrar columnas o mapear códigos.

MAPA_FNZ = {
    'ESTADO': 'Estado', 'ANALISTA': 'Analista_Asociado', 'FECHA': 'Fecha', 'REGIONAL': 'Regional_Venta',
    'DESEMBOLSO': 'Credito_Desembolsado', 'CEDULA': 'Cedula_Cliente', 'FS1NACFEC': 'Fecha_Nacimiento',
    'APELLIDOS': 'Apellidos', 'NOMBRES': 'Nombres', 'TELEFONO1': 'Celular1', 'MOVIL': 'Celular2',
    'FS1EMAIL': 'Correo', 'CARGO': 'Cargo', 'DIRECCION': 'Direccion', 'CODCIUDAD': 'Codigo_Ciudad',
    'CIUDAD': 'Ciudad', 'BARRIO': 'Barrio', 'VENNOMBRE': 'Nombre_Vendedor', 'CCONOMBRE': 'Centro_Costo',
    'CUOTAS': 'Total_Cuotas', 'FS0NOTA': 'Nota', 'VALOR_TOTA': 'Valor_Total', 'INGRESOS': 'Ingresos',
    'GASTOS': 'Gastos', 'PAGARE': 'Pagare'
}

# Mapeo de zonas de cobro (Códigos legacy -> Nombres legibles)
ZONA_COBRO_MAP = {
    'ZCN': 'CASA COBRANZA',
    '1AB': 'ABOGADO',
    'CC01': 'CASTIGO',
    '1CE': 'OTROS CASOS',
    'CEC': 'OTROS CASOS'
}

# 3. LÓGICA DE NEGOCIO Y ORDENAMIENTO
# Constantes usadas para visualización o reglas de negocio.

# Orden lógico para gráficas (evita orden alfabético incorrecto)
ORDEN_FRANJAS = ['AL DIA', '1 A 30', '31 A 90', '91 A 180', '181 A 360', 'MAS DE 360']

# 4. COLUMNAS DE SALIDA (PARQUETS FINALES)
# Define qué columnas se guardan en los archivos maestros para el buscador.

COLS_TABLA_NOVEDADES = [
    'Empresa', 'Credito', 'Nombre_Cliente', 'Cedula_Cliente', 'Celular', 'Nombre_Ciudad', 'Zona', 'Dias_Atraso_Final',
    'Total_Recaudo', 'Valor_Vencido', 'Estado_Pago', 'Estado_Gestion', "Regional_Cobro", 'Cargo_Usuario', 'Nombre_Usuario', 'Novedades_Por_Cargo',
    'Codeudor1', 'Nombre_Codeudor1', 'Telefono_Codeudor1', 'Codeudor2', 'Nombre_Codeudor2', 'Telefono_Codeudor2',
    'Fecha_Cuota_Vigente', 'Valor_Cuota_Vigente', 'Meta_$', 'Novedad', 'Tipo_Novedad',
    'CALL_CENTER_FILTRO', 'Tipo_Vigencia_Temp'
]

COLS_TABLA_RODAMIENTOS = [
    'Empresa', 'Credito', 'Cedula_Cliente', 'Nombre_Cliente', 'Celular', 'Fecha_Cuota_Vigente', 'Valor_Cuota_Vigente',
    'Nombre_Ciudad', "Regional_Cobro", 'Zona', 'Codeudor1', 'Nombre_Codeudor1', 'Telefono_Codeudor1', 'Codeudor2', 'Nombre_Codeudor2', 'Franja_Cartera',
    'Telefono_Codeudor2', 'Dias_Atraso_Final', 'Total_Recaudo', 'Meta_Intereses', 'Meta_Saldo', 'Valor_Vencido', 'Rodamiento',
    'Rodamiento_Cartera', 'Estado_Pago', 'Estado_Gestion', 'Meta_$',
    'CALL_CENTER_FILTRO', 'Tipo_Vigencia_Temp'
]

COLS_MASTER_CARTERA = COLS_CARTERA + ["Tipo_Vigencia_Temp", "CALL_CENTER_FILTRO", "Estado_Pago", "Estado_Gestion"]
# Novedades suele usarse completa
COLS_MASTER_NOVEDADES = COLS_NOVEDADES