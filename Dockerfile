# --- Dockerfile Final (Soluciona error de pip Y soporta S3) ---

# 1. Usa una imagen base oficial de Python 3.11
FROM python:3.11-slim

# 2. Establece el directorio de trabajo DENTRO del contenedor
WORKDIR /app

# 3. Copia la lista de librerías
# (Asegúrate que requirements.txt incluya 'boto3')
COPY requirements.txt .

# --- ¡ESTA ES LA LÍNEA QUE ARREGLA EL ERROR DE 'pip'! ---
RUN pip install --upgrade pip

# 4. Instala las dependencias (usando el pip actualizado)
RUN pip install --no-cache-dir -r requirements.txt

# --- Etapa 5: Copia de tu Código ---
# Copiamos la lógica de la API y el archivo principal
# (Asegúrate que main_api.py sea el nuevo con los endpoints de S3)
COPY ./src ./src
COPY main_api.py .

# --- Etapa 6: Comando de Ejecución ---
# Apuntamos a tu 'main_api.py' (el que tiene la lógica de S3)
CMD ["uvicorn", "main_api:app", "--host", "0.0.0.0", "--port", "8000"]