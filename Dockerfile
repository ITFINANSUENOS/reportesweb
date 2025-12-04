
# 1. Usa una imagen base oficial de Python 3.11
FROM python:3.11-slim

# 2. Establece el directorio de trabajo DENTRO del contenedor
WORKDIR /app

# --- NUEVO: Instala las librerías de SQLite ---
# Esto es necesario para que Python hable con la BD
RUN apt-get update && apt-get install -y sqlite3 && rm -rf /var/lib/apt/lists/*

# 3. Instala las dependencias de Python
COPY requirements.txt .
# Arregla el bug de 'pip'
RUN pip install --upgrade pip
# Instala las librerías (debe tener 'boto3', 'pandas', 'openpyxl', 'sqlalchemy')
RUN pip install --no-cache-dir -r requirements.txt

# --- Etapa 4: Copia de tu Código ---
COPY ./src ./src
COPY main_api.py .

# --- ¡NUEVO: Copia tu base de datos! ---
# Asume que 'corrections.db' está en la misma carpeta que este Dockerfile
COPY corrections.db .

# --- Etapa 5: Comando de Ejecución ---
CMD ["uvicorn", "main_api:app", "--host", "0.0.0.0", "--port", "8000"]