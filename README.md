# Fast OnPremise ETL 🚀

ETL de alto rendimiento diseñado para extraer datos desde un servidor espejo (SQL Server) y cargarlos en Google Cloud Storage (GCS) en formato Parquet, optimizado para cargas masivas y CDC.

## 📋 Descripción

Este proyecto implementa un flujo de extracción y carga (EL) que:

1.  **Lee configuración** desde Firestore (metadata de la tabla, query base, etc.).
2.  **Transforma la query** dinámicamente:
    *   Castea todas las columnas a `VARCHAR` para evitar problemas de tipos.
    *   Adapta la query según el modo: `FULL` (sin filtros) o `CDC` (por rangos de fecha).
3.  **Extrae datos** en modo *streaming* desde SQL Server usando SQLAlchemy + PyODBC.
4.  **Carga en paralelo** a GCS:
    *   Convierte chunks de datos a DataFrames de **Polars** (alto rendimiento).
    *   Sube los archivos Parquet a GCS usando un `ThreadPoolExecutor` para maximizar el ancho de banda.

## 🛠️ Tecnologías

*   **Python 3.11+**
*   **Polars**: Manipulación de datos ultra rápida.
*   **SQLAlchemy + PyODBC**: Conexión robusta a SQL Server.
*   **Google Cloud Platform**:
    *   Firestore (Configuración)
    *   Secret Manager (Credenciales)
    *   Cloud Storage (Destino)
*   **sqlglot**: Parseo y transpilación de SQL.

## 🚀 Instalación en Local

### Prerrequisitos

*   Python 3.11 o superior.
*   Driver ODBC 18 para SQL Server instalado en tu sistema.
*   Credenciales de GCP (Service Account) con permisos para Firestore, Secret Manager y Storage.

### Pasos

1.  **Clonar el repositorio**:
    ```bash
    git clone <url-del-repo>
    cd fast_onpremise_etl
    ```

2.  **Crear y activar entorno virtual**:
    ```bash
    python -m venv .venv
    source .venv/bin/activate  # En Windows: .venv\Scripts\activate
    ```

3.  **Instalar dependencias**:
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configurar variables de entorno**:
    Crea un archivo `.env` en la raíz (basado en el ejemplo):
    ```env
    MODE=DEV
    PAGE_SIZE=50000
    # Otras variables necesarias según tu entorno
    ```

5.  **Ejecutar**:
    ```bash
    # Modo CDC (por defecto o si no se especifica)
    python main.py <FIRESTORE_ID> 2024-01-01 2024-01-02

    # Modo FULL
    python main.py <FIRESTORE_ID> 2024-01-01 2024-01-02 full
    ```

## 🐳 Docker

El proyecto incluye un `Dockerfile` optimizado que instala los drivers ODBC de Microsoft necesarios.

### Construir imagen
```bash
docker build -t fast-etl .
```

### Ejecutar contenedor
```bash
docker run --env-file .env \
  -v $(pwd)/credentials:/app/credentials \
  fast-etl python main.py <ARGS>
```

## 📂 Estructura del Proyecto

*   `main.py`: Punto de entrada. Orquesta el flujo.
*   `tools/`: Módulos de utilidad.
    *   `extract.py`: Lógica de extracción (streaming + generador).
    *   `load.py`: Subida a GCS.
    *   `casting_query.py`: Transpilación SQL.
    *   `engine.py`: Configuración de conexión DB.
*   `constants.py`: Constantes globales.

---
Hecho con ❤️ por el equipo de Datos.
