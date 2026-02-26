# Data Analytics Engine

Bienvenido al motor central (Backend) de nuestra plataforma de inteligencia de negocio. Esta aplicación transforma el caos de los datos operativos y financieros en gráficas y métricas en tiempo real.

---

## Parte 1: Contexto General (¿Qué hace la aplicación?)

### El Problema
Imagina una empresa financiera que gestiona miles de créditos al día. Tienen agencias de Call Center, asesores enviando mensajes por WhatsApp, llamadas telefónicas y registros de pagos. Toda esta información vive dispersa en archivos Excel gigantescos y desordenados. Extraer conclusiones reales (como saber qué agente es más efectivo o cuánta cartera se recuperó en una zona) de forma manual toma horas o días, y está sujeto a errores humanos.

### La Solución
Este backend es el "cerebro" que automatiza todo ese trabajo. Su misión principal es ingerir esos reportes crudos, limpiarlos, cruzar la información inteligentemente y servirla a un Dashboard web (Frontend) de manera instantánea. 

El sistema entiende y procesa tres grandes mundos (Módulos de Negocio):
1. **Cartera (Finanzas):** Analiza el estado de la deuda. Calcula cuánto dinero se debía recaudar vs. cuánto se recaudó realmente, y evalúa el "Rodamiento" (si el estado de cuenta de un cliente mejoró, empeoró o se normalizó).
2. **Call Center (Operaciones):** Evalúa el rendimiento humano. Cruza los registros telefónicos y de mensajería para crear un **Embudo de Conversión** (Ej: *De 1,000 mensajes de WhatsApp enviados -> 500 generaron conversación -> 100 llegaron al sistema -> 20 terminaron en un pago real*).
3. **Comercial:** Evalúa estrategias de venta, como el rendimiento de créditos por "Cosechas" (cuándo se originó el crédito) y "Retanqueos" (refinanciaciones).

**El resultado final:** Un gerente entra a la web, hace clic en "Zona Norte", y en menos de 1 segundo ve toda la realidad de su negocio actualizada.

---

## Parte 2: Contexto Técnico (¿Cómo lo hace?)

Para lograr que el procesamiento de millones de celdas de Excel no bloquee el sistema y que las respuestas al Frontend sean en milisegundos, el sistema abandona los enfoques tradicionales y adopta una arquitectura de **Data Lake ligero y asíncrono**.

### 🚀 Stack Tecnológico
* **Framework:** FastAPI (Python)
* **Motor de Procesamiento:** Polars (Evaluación Lazy, multihilo, escrito en Rust).
* **Almacenamiento:** Amazon S3 y archivos `.Parquet`.
* **Colas de Mensajería:** Amazon SQS.

### El Ciclo de Vida del Dato (Data Pipeline)

El sistema funciona en dos fases distintas para garantizar el máximo rendimiento:

#### Fase 1: Ingesta y ETL Asíncrono (Background)
1. **Subida Directa a S3:** Cuando el usuario sube el Excel, el backend no lo recibe en su memoria. En su lugar, genera una *Presigned URL* para que el archivo vaya directo a Amazon S3.
2. **Desencadenamiento SQS:** Un mensaje es enviado a Amazon SQS, despertando a un `worker.py` independiente.
3. **Limpieza y Cruce (Polars):** El `Orchestrator` toma el control. Polars limpia los datos "sucios" (fechas mixtas, textos con espacios, teléfonos mal formateados), aplica lógicas de *Cascada* (prioridad de asignación de créditos) y *Fuzzy Matching* (para cruzar nombres de agentes mal escritos).
4. **Guardado Estratégico:** Se generan dos tipos de archivos en S3:
   * **Archivos `.JSON`:** Contienen resúmenes globales estáticos. El Frontend los lee para cargar la vista inicial en 0.1 segundos.
   * **Archivos `.Parquet`:** Contienen el detalle fila por fila comprimido de forma columnar.

#### Fase 2: Consultas Dinámicas en Vivo (API REST)
Cuando el usuario en el Frontend aplica un filtro complejo (Ej: *Empresa X + Zona Sur + Franja de mora alta*):
1. FastAPI recibe la petición POST.
2. Se lee el archivo `.Parquet` desde S3 (o caché local).
3. **Polars** escanea solo las columnas requeridas, filtra en memoria, recalcula todas las métricas matemáticas (Cumplimientos, Embudos) y devuelve un nuevo JSON al momento.

###  Arquitectura de Despliegue (AWS)
El sistema se despliega bajo un enfoque Serverless con CI/CD automático:

```mermaid
graph TD
%% Usuarios y Trigger
User((Usuario/Sistema Externo)) -->|Solicitud de Reporte| AR[AWS App Runner: api-reportes-v2]

   subgraph "Flujo de Despliegue Continuo (CI/CD)"
       GH[GitHub: reportesweb - Rama develop] -->|WebHook: Despliegue Automático| AR
   end

   subgraph "Capa de Ejecución (Serverless)"
       AR -->|Gestión de Permisos| IAM[IAM Role: AppRunner-Finansuenos-Rol]
       AR -.->|Runtime: Compilación en Caliente| Env[Entorno Managed: 1 vCPU / 2GB RAM]
   end

   subgraph "Capa de Persistencia y Salida"
       IAM -->|Permisos de Escritura| S3[Amazon S3: finansuenos-reportes-privados]
       AR -->|Generación de Archivo| S3
       S3 -->|URL Temporal / Presigned| User
   end

   %% Estilos
   style AR fill:#ff9900,color:#fff
   style S3 fill:#2E7D32,color:#fff
   style GH fill:#24292e,color:#fff
