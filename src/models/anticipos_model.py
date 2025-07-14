from dataclasses import dataclass, field
from typing import Dict, List

@dataclass
class AnticiposConfig:
    """
    Define las REGLAS y PARÁMETROS para generar el reporte de Anticipos.
    Esta clase ahora vive en su propio archivo de modelo.
    """
    required_sheets: List[str] = field(default_factory=lambda: ['ONLINE', 'AC FS', 'AC ARP'])
    sheet_columns: Dict[str, List[str]] = field(default_factory=lambda: {
        'ONLINE': ['MCNTIPCRU1', 'MCNNUMCRU1', 'MCNVINCULA', 'VINNOMBRE', 'SALDODOC'],
        'AC FS': ['cobra', 'ccosto', 'FACTURA', 'CEDULA', 'saldofac'],
        'AC ARP': ['cobra', 'ccosto', 'FACTURA', 'CEDULA', 'saldofac']
    })
    rename_columns: Dict[str, Dict[str, str]] = field(default_factory=lambda: {
        'ONLINE': {
            'MCNTIPCRU1': 'TIPO_RECIBO', 'MCNNUMCRU1': 'No', 'MCNVINCULA': 'CEDULA',
            'VINNOMBRE': 'NOMBRE', 'SALDODOC': 'VALOR'
        },
        'AC FS': {
            'saldofac': 'ULTIMO_SALDO_FS', 'cobra': 'ZONA_COBRADOR_FS',
            'ccosto': 'CENTRO_COSTO_FS', 'FACTURA': 'FACTURA_FS', 'CEDULA': 'CEDULA'
        },
        'AC ARP': {
            'saldofac': 'ULTIMO_SALDO_ARP', 'cobra': 'ZONA_COBRADOR_ARP',
            'ccosto': 'CENTRO_COSTO_ARP', 'FACTURA': 'FACTURA_ARP', 'CEDULA': 'CEDULA'
        }
    })
    output_filename: str = "reporte_anticipos.xlsx"
    column_order_fs: List[str] = field(default_factory=lambda: [
        'ITEM', 'TIPO_RECIBO', 'No', 'CEDULA', 'NOMBRE', 'CENTRO_COSTO_FS',
        'VALOR', 'FACTURA_FS', 'ZONA_COBRADOR_FS', 'OBSERVACIONES', 'CUENTAS_FS',
        'ULTIMO_SALDO_FS', 'VALOR_POSITIVO', 'RESTA_SALDO'
    ])
    column_order_arp: List[str] = field(default_factory=lambda: [
        'ITEM', 'TIPO_RECIBO', 'No', 'CEDULA', 'NOMBRE', 'CENTRO_COSTO_ARP',
        'VALOR', 'FACTURA_ARP', 'ZONA_COBRADOR_ARP', 'OBSERVACIONES', 'CUENTAS_ARP',
        'ULTIMO_SALDO_ARP', 'VALOR_POSITIVO', 'RESTA_SALDO'
    ])