import pandas as pd 


class CifinModel:
    
    def __init__(self):
        self.df = None
        self.colspec = [
            
        ]
        self.names = [
            "tipo_identificacion","Nº_identificacion","nombre_tercero","fecha_limite_pago","numero_obligacion",
            "codigo_sucursal","calidad","estado_obligacion","edad_mora","años_mora","fecha_corte","fecha_inicio",
            "fecha_terminacion","fecha_exigibilidad","fecha_prescripcion","fecha_pago","modo_extincion","tipo_pago",
            "periodicidad","cuotas_pagadas","cuotas_pactadas","cuotas_mora","valor_inicial","valor_mora",
            "valor_saldo","valor_cuota","cargo_fijo","linea_credito","tipo_contrato","estado_contrato","vigencia_contrato",
            "numero_meses_contrato","obligacion_reestructurada","naturaleza_reestructuracion","numero_reestructuraciones",
            "Nº_cheques_devueltos","plazo","dias_cartera","direccion_casa","telefono_casa","codigo_ciudad_casa",
            "ciudad_casa","codigo_departamento","departamento_casa","nombre_empresa","direccion_empresa","telefono_empresa",
            "codigo_ciudad_empresa","ciuda_empresa","codigo_departamento_empresa","departamento_empresa","correo_electronico",
            "numero_celular","valor_real_pagado"

        ]
        