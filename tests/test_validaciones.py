import pytest
from fastapi import HTTPException
from unittest.mock import MagicMock
from src.controllers.reportes import ReportesController

# --- FIXTURE: PREPARACIÓN DEL ESCENARIO ---
@pytest.fixture
def controller_mock():
    """
    Crea una instancia del controlador pero con S3 y SQS 'falsos'.
    Así no necesitamos internet ni credenciales reales.
    """
    # 1. Instanciamos la clase
    controller = ReportesController()
    
    # 2. Reemplazamos el cliente S3 real por uno Mágico (Mock)
    controller.s3_client = MagicMock()
    
    # Le decimos al Mock: "Cuando te llamen a generate_presigned_url, devuelve esto:"
    controller.s3_client.generate_presigned_url.return_value = "https://s3.aws.com/fake-upload-url"
    
    return controller

# --- TEST 1: EL CAMINO FELIZ (Todo correcto) ---
def test_generar_url_exito(controller_mock):
    # Datos válidos (Nombre correcto, XLSX, peso bajo)
    resultado = controller_mock.generar_url_subida(
        filename="Reporte_General Enero.xlsx",
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        file_size=1024 # 1KB
    )
    
    assert "upload_url" in resultado
    assert resultado["upload_url"] == "https://s3.aws.com/fake-upload-url"
    assert "file_key" in resultado

# --- TEST 2: FALLO POR NOMBRE ---
def test_fallo_nombre_incorrecto(controller_mock):
    # Nombre sin 'Reporte_General'
    with pytest.raises(HTTPException) as error:
        controller_mock.generar_url_subida(
            filename="Ventas_2026.xlsx", 
            content_type="application/excel", 
            file_size=1000
        )
    
    # Verificamos que el error sea 400 y tenga el mensaje correcto
    assert error.value.status_code == 400
    assert "Nombre de archivo inválido" in error.value.detail

# --- TEST 3: FALLO POR TAMAÑO ---
def test_fallo_archivo_muy_pesado(controller_mock):
    # 30 MB (El límite es 25)
    peso_excesivo = 30 * 1024 * 1024
    
    with pytest.raises(HTTPException) as error:
        controller_mock.generar_url_subida(
            filename="Reporte_General.xlsx", 
            content_type="application/excel", 
            file_size=peso_excesivo
        )
            
    assert error.value.status_code == 400
    assert "excede el tamaño máximo" in error.value.detail