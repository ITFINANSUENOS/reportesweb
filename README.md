Documentación funcional

Que hace, como lo hace, quien lo toma


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
