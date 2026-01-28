# ETAPA 1: Builder (Compilación)
FROM python:3.12-slim as builder

WORKDIR /app

# Instalar herramientas de compilación
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Crear entorno virtual para aislar librerías
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .
# Instalar dependencias en el entorno virtual
RUN pip install --no-cache-dir -r requirements.txt


# ETAPA 2: Final (Ejecución)
FROM python:3.12-slim

WORKDIR /app

# Crear usuario sin privilegios
RUN useradd -m appuser

# Copiar solo el entorno virtual desde la etapa Builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copiar el código fuente (Al final, para aprovechar caché de capas anteriores)
COPY . .

# Cambiar permisos al usuario appuser
RUN chown -R appuser:appuser /app

# Cambiar al usuario no-root
USER appuser

# Variables de entorno optimizadas
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

EXPOSE 8000

# Comando de arranque
CMD ["uvicorn", "main_api:app", "--host", "0.0.0.0", "--port", "8000"]