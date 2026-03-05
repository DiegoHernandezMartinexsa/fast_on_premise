# Usa Python 3.11 slim como base para minimizar el tamaño
FROM python:3.11-slim

# Evita que Python escriba archivos .pyc y fuerza salida sin buffer
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Instalar dependencias del sistema necesarias
# - gnupg2 y curl para descargar las llaves de Microsoft
# - unixodbc-dev para compilar pyodbc
RUN apt-get update && apt-get install -y \
    curl \
    gnupg2 \
    unixodbc \
    unixodbc-dev \
    ca-certificates \
    build-essential \   
    && rm -rf /var/lib/apt/lists/*

# 2. Configurar el repositorio de Microsoft para Debian 12
# Usamos 'gpg --dearmor' en lugar de 'apt-key' (que ya no existe)
RUN curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list

# 3. Instalar el driver ODBC (msodbcsql18)
# Nota: msodbcsql18 es el recomendado para Debian 12. 
RUN apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    # Opcional: herramientas mssql-tools si las necesitas para debugging
    # && ACCEPT_EULA=Y apt-get install -y mssql-tools18 \
    && rm -rf /var/lib/apt/lists/*

# 4. Instalar dependencias de Python
COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copiar el código fuente
COPY . .

ENV MODE=PROD

ENTRYPOINT ["python", "-u", "main.py"]
