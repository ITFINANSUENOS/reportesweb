from dataclasses import dataclass
from typing import List, Dict

@dataclass(frozen=True)
class ReporteGneralSchema:
    # 1. Definición de Hojas Esperadas
    SHEET_CARTERA: str = "Analisis_de_Cartera"
    SHEET_NOVEDADES: str = "Detalle_Novedades"
    SHEET_LLAMADAS: str = "Reporte_Llamadas" # Opcional
    SHEET_MENSAJES: str = "Reporte_Mensajes" # Opcional
    SHEET_FNZ: str = "FNZ007" # Opcional

    # 2. Columnas de "Analisis_de_Cartera"
    COLS_CARTERA = [                                                                                                                                       
        "Fecha_Desembolso", "Fecha_Ultima_Novedad", "Empresa", "Regional_Venta",
        "Nombre_Ciudad", "Nombre_Vendedor", "Franja_Meta", "Rodamiento", "Gestor",
        "Regional_Cobro", "Zona_Cobro", "Zona", "Cantidad_Novedades", "Cedula_Cliente", 
        "Credito", "Nombre_Producto", "Obsequio", "Nombre_Cliente", "Correo", "Celular", 
        "Direccion", "Barrio", "Nombre_Codeudor2", "Cobrador", "Telefono_Cobrador", 
        "Call_Center_Apoyo", "Codigo_Vendedor", "Nombre_Call_Center", "Telefono_Call_Center",
        "Telefono_Gestor", "Valor_Desembolso", "Movil_Vendedor", "Vendedor_Activo",
        "Lider_Zona", "Codeudor1", "Total_Cuotas", "Nombre_Codeudor1", "Telefono_Codeudor1", 
        "Ciudad_Codeudor1", "Codeudor2", "Nombre_Codeudor2", "Telefono_Codeudor2", 
        "Ciudad_Codeudor2", "Valor_Cuota", "Dias_Atraso", "Franja_Cartera", "Meta_Intereses", 
        "Meta_Saldo", "Meta_%", "Meta_$", "Meta_T.R_%","Meta_General", "Meta_T.R_$", 
        "Cuotas_Pagadas", "Fecha_Cuota_Atraso", "Primera_Cuota_Mora", "Valor_Cuota_Atraso", 
        "Valor_Vencido", "Dias_Atraso_Final","Fecha_Ultimo_pago","Rango_Ultimo_pago",
        "Franja_Meta_Final", "Franja_Cartera_Final", "Rodamiento_Cartera","Valor_Cuota_Vigente",
        "Recaudo_Anticipado", "Recaudo_Meta", "Total_Recaudo", "Fecha_Cuota_Vigente","Total_Recaudo_Sin_Anti"
    ]

    # 3. Columnas de "Detalle_Novedades"
    COLS_NOVEDADES = [
        "Fecha_Novedad", "Cedula_Cliente", "Nombre_Cliente", "Usuario_Novedad",
        "Nombre_Usuario", "Cargo_Usuario", "Celular_Corporativo", "Tipo_Novedad",
        "Novedad", "Fecha_Compromiso", "Valor","Empresa","Celular_Cliente","Telefono_Cliente"
    ]
    
    # 4. Columnas de "Reporte_Llamadas"
    COLS_LLAMADAS = [
        "Fecha_Llamada", "Extension_Llamada", "Destino_Llamada", "Estado_Llamada", 
        "Duracion_Llamada", "Codigo_Llamada", "Grabacion_Llamada", "Call_Center", 
        "Nombre_Call"
    ]

    # 5. Columnas de "Reporte_Mensajes"
    COLS_MENSAJERIA = [
        "Codigo_Pais", "Numero_Telefono", "Nombre_Saliente", "Estado", "Estado_Mensaje", 
        "Estado_Respuesta_Saliente", "Respuesta_Saliente", "Flujo_Truora", 
        "Primer_Mensaje_Agente", "Fecha_Llamada", "Call_Center", "Nombre_Call"
    ]
    

    # 6. Mapeo para FNZ007 (Para renombrar columnas después)
    MAPA_FNZ: Dict[str, str] = None
    
    def __post_init__(self):
        # Inicializamos el diccionario aquí porque dataclass frozen no deja hacerlo arriba fácilmente
        object.__setattr__(self, 'MAPA_FNZ', {
            'ESTADO':'Estado', 'ANALISTA':'Analista_Asociado', 'FECHA':'Fecha',
            'DESEMBOLSO':'Credito_Desembolsado', 'CEDULA':'Cedula_Cliente', 
            'FS1NACFEC':'Fecha_Nacimiento', 'APELLIDOS':'Apellidos', 'NOMBRES':'Nombres',
            'TELEFONO1':'Celular1', 'MOVIL':'Celular2', 'FS1EMAIL':'Correo',
            'CARGO':'Cargo', 'DIRECCION':'Direccion', 'CODCIUDAD':'Codigo_Ciudad',
            'CIUDAD':'Ciudad', 'BARRIO':'Barrio', 'VENNOMBRE':'Nombre_Vendedor',
            'CCONOMBRE':'Centro_Costo', 'CUOTAS':'Total_Cuotas', 'FS0NOTA':'Nota',
            'VALOR_TOTA':'Valor_Total', 'INGRESOS':'Ingresos', 'GASTOS':'Gastos'
        })