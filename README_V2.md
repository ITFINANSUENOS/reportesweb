# ReportesWeb - Motor de Analítica Financiera

> **Versión 2.0** - Documentación Técnica Completa

---

## Tabla de Contenidos

1. [Resumen Ejecutivo](#resumen-ejecutivo)
2. [Arquitectura del Sistema](#arquitectura-del-sistema)
3. [Módulos de Negocio](#módulos-de-negocio)
4. [Stack Tecnológico](#stack-tecnológico)
5. [Estructura del Proyecto](#estructura-del-proyecto)
6. [API Reference](#api-reference)
7. [Flujo de Datos (Pipeline)](#flujo-de-datos-pipeline)
8. [Despliegue](#despliegue)
9. [Desarrollo Local](#desarrollo-local)
10. [Testing](#testing)

---

## Resumen Ejecutivo

**ReportesWeb** es un motor de analítica financiera que procesa grandes volúmenes de datos operativos (cartera, call center, comercial) provenientes de archivos Excel, transformándolos en métricas accionables y visualizaciones en tiempo real.

### Problema que resuelve

Las empresas financieras manejan miles de créditos diarios con datos dispersos en archivos Excel gigantescos. Extraer conclusiones manualmente toma horas/días y está sujeto a errores humanos.

### Solución

Un backend automatizado que ingiere datos crudos, los limpia, cruza información inteligentemente y sirve resultados a un Dashboard web de forma instantánea (< 1 segundo).

---

## Arquitectura del Sistema

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              USUARIO FINAL                                   │
│                  (Dashboard Web - Frontend)                                  │
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │ HTTP Requests
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         AWS APP RUNNER                                       │
│                    API de Reportes v2                                        │
│                 (FastAPI + Uvicorn)                                          │
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │
          ┌───────────────────────┼───────────────────────┐
          ▼                       ▼                       ▼
   ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
   │   /reportes  │      │ /tableros/   │      │ /busquedas   │
   │   (Upload)   │      │  cartera     │      │  (Search)    │
   └──────────────┘      └──────────────┘      └──────────────┘
          │                       │                       │
          ▼                       ▼                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        AMAZON S3                                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐       │
│  │  /graficos  │  │   /data     │  │  /temp      │  │  /exports   │       │
│  │  (.json)    │  │  (.parquet) │  │  (uploads)  │  │  (.xlsx)    │       │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘       │
└─────────────────────────────────────────────────────────────────────────────┘
          ▲
          │ (SQS Message)
          │
┌─────────────────────────────────────────────────────────────────────────────┐
│                      WORKER (Fargate Spot)                                   │
│              Proceso en Segundo Plano                                        │
│  ┌──────────────────────────────────────────────────────────────────┐       │
│  │                    ReportesOrchestrator                          │       │
│  │  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐   │       │
│  │  │  Cartera   │ │Call Center │ │  Comercial │ │Seguimientos│   │       │
│  │  │ Service    │ │  Service   │ │  Service   │ │  Service   │   │       │
│  │  └────────────┘ └────────────┘ └────────────┘ └────────────┘   │       │
│  └──────────────────────────────────────────────────────────────────┘       │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Componentes Principales

| Componente | Descripción |
|------------|-------------|
| **FastAPI** | Framework web async para la API REST |
| **Worker** | Proceso consumidor de SQS que procesa archivos |
| **S3 Service** | Gestión de almacenamiento en Amazon S3 |
| **Orchestrator** | Coordina el pipeline de procesamiento |
| **Analytics Services** | Módulos de procesamiento por área de negocio |

---

## Módulos de Negocio

### 1. Cartera (Finanzas)

Analiza el estado de la deuda y calcula métricas financieras:

- **Monto Total Cartera**: Suma de todos los créditos activos
- **Monto Recaudado**: Pagos realizados vs. esperados
- **Indicador de Cumplimiento**: Porcentaje de recuperación
- **Rodamiento**: Evolución del estado de cuenta (mejora/peora/normaliza)

**Ubicación**: `src/services/analytics/cartera.py`

### 2. Call Center (Operaciones)

Evalúa el rendimiento humano y crea embudos de conversión:

- **Llamadas**: Registros telefónicos entrantes/salientes
- **Mensajería**: Conversaciones por WhatsApp
- **Novedades**: Gestiones realizadas por asesores
- **Embudo de Conversión**: 
  ```
  Mensajes Enviados → Conversaciones → Registros → Pagos Reales
  ```

**Ubicación**: `src/services/analytics/call_center.py`

### 3. Comercial

Evalúa estrategias de venta y rendimiento de créditos:

- **Cosechas**: Créditos originados en los últimos 6 meses
  - Clasificación: Sin pago, Fallo 2da cuota, Fallo 3ra+
- **Retanqueos**: Refinanciaciones de créditos existentes
  - Condiciones: 6-8 cuotas con 1-2 restantes, o +8 cuotas con 1-4 restantes
- **FNZ**: Análisis de fondo(z) por zona

**Ubicación**: `src/services/analytics/comercial.py`

### 4. Seguimientos

Cruza cartera con novedades para seguimiento:

- **Gestiones**: Acciones realizadas por asesores
- **Rodamientos**: Estado de cuenta por cliente

**Ubicación**: `src/services/analytics/seguimientos.py`

---

## Stack Tecnológico

| Capa | Tecnología |
|------|------------|
| **Framework API** | FastAPI 0.109+ |
| **Servidor** | Uvicorn |
| **Procesamiento** | Polars (Lazy Evaluation, multihilo) |
| **Datos** | Pandas, NumPy, PyArrow |
| **Excel** | openpyxl, xlsxwriter, fastexcel |
| **Cloud** | AWS S3, SQS, App Runner |
| **Matching** | RapidFuzz, Levenshtein |
| **Testing** | Pytest |

---

## Estructura del Proyecto

```
reportesweb/
├── main_api.py                 # Punto de entrada FastAPI
├── worker.py                   # Consumidor SQS (Background)
├── requirements.txt            # Dependencias Python
├── Dockerfile                  # Imagen del contenedor
├── deploy_to_aws.bat          # Script de despliegue
├── apprunner.yaml             # Config AWS App Runner
├── check_env.py               # Validador de variables de entorno
├── .env.template              # Plantilla de configuración
│
├── src/
│   ├── api/
│   │   └── v1/
│   │       ├── router.py              # Router principal
│   │       └── routes/
│   │           ├── reportes.py        # Endpoints de upload/procesamiento
│   │           ├── cartera_analytics.py  # Endpoints de métricas cartera
│   │           └── busquedas.py       # Motor de búsqueda
│   │
│   ├── core/
│   │   ├── config.py          # Configuración (Settings)
│   │   └── constants.py       # Constantes globales
│   │
│   ├── schemas/
│   │   ├── request_models.py # Modelos Pydantic (input)
│   │   └── response_models.py # Modelos Pydantic (output)
│   │
│   ├── services/
│   │   ├── orchestrator.py    # Coordinator del pipeline
│   │   ├── busquedas_service.py
│   │   ├── storage/
│   │   │   ├── s3_service.py     # Cliente S3
│   │   │   └── excel_loader.py   # Cargador Excel
│   │   └── analytics/
│   │       ├── cartera.py        # Métricas financieras
│   │       ├── call_center.py    # Métricas call center
│   │       ├── comercial.py       # Métricas comerciales
│   │       ├── seguimientos.py   # Seguimientos/rodamientos
│   │       ├── resultados.py     # KPIs generales
│   │       ├── detallados.py      # Reportes detallados
│   │       └── call_center_modules/
│   │           ├── core.py
│   │           ├── calls.py
│   │           ├── messaging.py
│   │           └── novedades.py
│   │
│   └── utils/
│       └── polars_utils.py     # Utilidades Polars
│
├── tests/
│   ├── test_calculos.py       # Tests de cálculos
│   └── test_validaciones.py   # Tests de validaciones
│
└── temp/                      # Archivos temporales
```

---

## API Reference

### Base URL

```
https://{dominio}/api/v1
```

### Endpoints Principales

#### 1. Procesar Reporte Datacrédito

```http
POST /reportes/datacredito/process
```

Sube y procesa un archivo Excel de Datacrédito.

**Request**: `multipart/form-data`

| Campo | Tipo | Descripción |
|-------|------|-------------|
| file | file | Archivo Excel (.xlsx) |
| empresa | string | Nombre de la empresa |

**Response**:

```json
{
  "job_id": "uuid-generated",
  "status": "PROCESSING",
  "message": "Archivo subido, procesamiento en background"
}
```

#### 2. Obtener Métricas Cartera

```http
GET /tableros/cartera/metricas/{job_id}
```

Retorna métricas financieras del tablero principal.

#### 3. Búsqueda Detallada

```http
POST /busquedas/detallados
```

Búsqueda avanzada en datos detallados.

**Request**:

```json
{
  "modulo": "cartera",
  "job_id": "uuid",
  "filtros": {
    "zona": "Norte",
    "estado": "Activo"
  }
}
```

### Documentación Interactiva

Accede a `http://localhost:8000/docs` para ver la documentación Swagger UI.

---

## Flujo de Datos (Pipeline)

### Fase 1: Ingesta y ETL Asíncrono

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Usuario   │────▶│   FastAPI   │────▶│     S3      │────▶│     SQS     │
│  (Sube XLSX)│     │ (Presigned  │     │  (Storage)  │     │   (Queue)   │
└─────────────┘     │    URL)     │     └─────────────┘     └──────┬──────┘
                    └─────────────┘                                    │
                                                                     ▼
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   .JSON     │◀────│   Worker    │◀────│ Orchestrator│◀────│   Worker    │
│ (Resumen)   │     │ (Proceso)   │     │             │     │  (Consume)  │
└─────────────┘     └─────────────┘     └──────┬──────┘     └─────────────┘
                                               │
                    ┌───────────────────────────┼───────────────────────────┐
                    ▼                           ▼                           ▼
             ┌─────────────┐            ┌─────────────┐            ┌─────────────┐
             │  Cartera    │            │ Call Center │            │  Comercial  │
             │  Service    │            │   Service   │            │   Service   │
             └─────────────┘            └─────────────┘            └─────────────┘
                    │                           │                           │
                    ▼                           ▼                           ▼
             ┌─────────────┐            ┌─────────────┐            ┌─────────────┐
             │   .Parquet  │            │   .Parquet   │            │   .Parquet  │
             │  (Detalle)  │            │   (Detalle)  │            │   (Detalle)  │
             └─────────────┘            └─────────────┘            └─────────────┘
```

### Fase 2: Consultas Dinámicas en Vivo

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Usuario   │────▶│   FastAPI   │────▶│     S3      │────▶│   Polars    │
│ (Dashboard)│     │ (Filtros)   │     │ (.Parquet)  │     │ (Consulta)  │
└─────────────┘     └─────────────┘     └─────────────┘     └──────┬──────┘
                                                                      │
                                                                      ▼
                                                              ┌─────────────┐
                                                              │    JSON     │
                                                              │ (Resultados)│
                                                              └─────────────┘
```

### Procesamiento de Datos

1. **Limpieza**: Fechas mixtas, espacios, teléfonos mal formateados
2. **Lógica de Cascada**: Prioridad de asignación de créditos
3. **Fuzzy Matching**: Cruce de nombres de agentes mal escritos
4. **Generación de Archivos**:
   - `.JSON`: Resúmenes globales para carga rápida (< 0.1s)
   - `.Parquet`: Detalle fila por fila en formato columnar

---

## Despliegue

### AWS App Runner

El proyecto está configurado para despliegue en AWS App Runner con CI/CD automático desde GitHub.

```yaml
# apprunner.yaml
version: 1.0
runtime: python3
build:
  commands:
    build:
      - pip install -r requirements.txt
run:
  command: uvicorn main_api:app --host 0.0.0.0 --port $PORT
```

### Variables de Entorno Requeridas

| Variable | Descripción | Ejemplo |
|----------|-------------|---------|
| `AWS_REGION` | Región AWS | `us-east-1` |
| `S3_BUCKET_NAME` | Nombre bucket S3 | `finansuenos-reportes-privados` |
| `SQS_QUEUE_URL` | URL cola SQS | `https://sqs.us-east-1.amazonaws.com/...` |
| `ENVIRONMENT` | Entorno | `production` / `local` |

### Comandos de Despliegue

```bash
# 1. Construir imagen Docker
docker build -t api-reportes:v2.0.0 .

# 2. Taggear para ECR
docker tag api-reportes:v2.0.0 039612863646.dkr.ecr.us-east-1.amazonaws.com/api-reportes:v2.0.0

# 3. Push a ECR
docker push 039612863646.dkr.ecr.us-east-1.amazonaws.com/api-reportes:v2.0.0

# 4. Versionar Git
git tag v2.0.0
git push origin v2.0.0
```

---

## Desarrollo Local

### Requisitos

- Python 3.12+
- Docker (opcional)

### Instalación

```bash
# 1. Clonar repositorio
git clone https://github.com/tu-repo/reportesweb.git
cd reportesweb

# 2. Crear entorno virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Configurar variables
cp .env.template .env
# Editar .env con tus credenciales
```

### Ejecutar API

```bash
# Modo desarrollo (auto-reload)
uvicorn main_api:app --reload --host 0.0.0.0 --port 8000

# URL docs: http://localhost:8000/docs
```

### Ejecutar Worker

```bash
# En otra terminal
python worker.py
```

### Docker

```bash
# Construir
docker build -t reportesweb .

# Ejecutar
docker run -p 8000:8000 --env-file .env reportesweb
```

---

## Testing

### Ejecutar Tests

```bash
# Todos los tests
pytest

# Tests específicos
pytest tests/test_calculos.py
pytest tests/test_validaciones.py

# Coverage
pytest --cov=src --cov-report=html
```

### Estructura de Tests

```
tests/
├── __init__.py
├── test_calculos.py       # Tests de funciones de cálculo
│   ├── test_calculo_porcentajes
│   ├── test_calculo_metrics
│   └── ...
│
└── test_validaciones.py   # Tests de validaciones
    ├── test_validacion_fechas
    ├── test_validacion_campos
    └── ...
```

### Buenas Prácticas

- Cada test debe ser independiente
- Usar fixtures para datos de prueba
- Nombrar tests con convención: `test_{funcion}_{caso}`

---

## Changelog

### v2.0.0 (Actual)
- Módulo Call Center completo con embudos de conversión
- Búsqueda detallada en datos Parquet
- Optimización de consultas Polars
- Documentación técnica completa

### v1.0.0
- Pipeline básico de cartera
- Módulos Comercial (Cosechas, Retanqueos, FNZ)
- Integración S3/SQS

---

## Contribuir

1. Crear rama desde `develop`: `git checkout -b feature/nueva-funcionalidad`
2. Hacer commit con mensajes descriptivos
3. Crear Pull Request a `develop`
4. Esperar review y merge

---

## Licencia

Proprietario - © 2026 Finansueños
