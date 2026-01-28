@echo off
SETLOCAL

:: --- CONFIGURACIÓN ---
SET AWS_REGION=us-east-1
SET AWS_ACCOUNT_ID=039612863646
SET REPO_NAME=api-reportes
SET IMAGE_TAG=latest
:: Puedes cambiar 'latest' por una version especifica ej: v1.0.0

:: URI Completa (No editar)
SET ECR_URI=%AWS_ACCOUNT_ID%.dkr.ecr.%AWS_REGION%.amazonaws.com
SET FULL_IMAGE_NAME=%ECR_URI%/%REPO_NAME%:%IMAGE_TAG%

echo ========================================================
echo  DESPLIEGUE AUTOMATICO A AWS ECR
echo ========================================================
echo  Region: %AWS_REGION%
echo  Repo:   %REPO_NAME%
echo  Tag:    %IMAGE_TAG%
echo ========================================================
echo.

:: 1. LOGIN EN AWS ECR
echo [1/4] Autenticando Docker con AWS ECR...
aws ecr get-login-password --region %AWS_REGION% | docker login --username AWS --password-stdin %ECR_URI%
IF %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Fallo el login en AWS. Verifica tus credenciales o si Docker esta corriendo.
    pause
    EXIT /B %ERRORLEVEL%
)

:: 2. VERIFICAR SI EXISTE EL REPO (O CREARLO)
echo.
echo [2/4] Verificando repositorio en ECR...
aws ecr describe-repositories --repository-names %REPO_NAME% --region %AWS_REGION% >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo El repositorio no existe. Creando '%REPO_NAME%'...
    aws ecr create-repository --repository-name %REPO_NAME% --region %AWS_REGION%
)

:: 3. CONSTRUIR IMAGEN DOCKER
echo.
echo [3/4] Construyendo imagen Docker (esto puede tardar)...
docker build -t %REPO_NAME% .
IF %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Fallo la construccion de la imagen.
    pause
    EXIT /B %ERRORLEVEL%
)

:: 4. ETIQUETAR Y SUBIR
echo.
echo [4/4] Subiendo imagen a ECR...
docker tag %REPO_NAME%:latest %FULL_IMAGE_NAME%
docker push %FULL_IMAGE_NAME%
IF %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Fallo la subida de la imagen.
    pause
    EXIT /B %ERRORLEVEL%
)

echo.
echo ========================================================
echo  EXITO! La imagen se subio correctamente.
echo  URI: %FULL_IMAGE_NAME%
echo ========================================================
echo.
echo PASOS SIGUIENTES EN AWS APP RUNNER:
echo 1. Ve a la consola de AWS App Runner.
echo 2. Crea un servicio nuevo.
echo 3. Selecciona "Container Image URI".
echo 4. Pega la URI de arriba: %FULL_IMAGE_NAME%
echo ========================================================
pause