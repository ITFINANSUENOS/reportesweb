# USAMOS PYTHON 3.12 SLIM (Ligero y rápido)
FROM python:3.12-slim

# VARIABLES DE ENTORNO PARA PYTHON
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on

# DIRECTORIO DE TRABAJO
WORKDIR /app

# INSTALAR DEPENDENCIAS DE SISTEMA (GCC necesario para Pandas/Numpy/Polars a veces)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    libpq-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# COPIAR REQUIREMENTS E INSTALAR
COPY requirements.txt .
RUN pip install -r requirements.txt

# COPIAR EL CÓDIGO FUENTE
# (El .dockerignore evitará copiar basura)
COPY . .

# EXCEPTO LA API, EL CONTENEDOR NO EXPONE PUERTOS POR DEFECTO
# (AWS Batch sobrescribirá el comando CMD, pero dejamos este por defecto para la API)
EXPOSE 8000
CMD ["uvicorn", "main_api:app", "--host", "0.0.0.0", "--port", "8000"]