import os

from google.cloud import secretmanager
from sqlalchemy import create_engine


def get_secret(secret_name: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/icasa-dtlk-dev/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(name=name, timeout=30)
    return response.payload.data.decode("UTF-8")


def create_sql_engine():
    mode = os.getenv("MODE")

    server = get_secret("mirror_host")
    usuario = get_secret("mirror_username")
    contraseña = get_secret("mirror_password")
    database = get_secret("mirror_database")
    mode = os.getenv("MODE")

    driver = "ODBC+Driver+18+for+SQL+Server" if mode == "DEV" else "ODBC+Driver+18+for+SQL+Server"

    connection_string = (
        f"mssql+pyodbc:///?odbc_connect="
        f"DRIVER={driver};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={usuario};"
        f"PWD={contraseña};"
        f"TrustServerCertificate=yes;"
        f"Encrypt=yes;"
    )
    return create_engine(connection_string, connect_args={"timeout": 30})
