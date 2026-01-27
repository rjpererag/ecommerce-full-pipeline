from dataclasses import dataclass
from decouple import config

@dataclass(frozen=True)
class DatabaseCredentials:
    database: str = config("POSTGRES_DB", cast=str, default="postgres")
    user: str = config("POSTGRES_USER", cast=str, default="postgres")
    host: str = config("POSTGRES_HOST", cast=str, default="localhost")
    port: int = config("POSTGRES_PORT", cast=int, default=5432)
    password: str = config("POSTGRES_PASSWORD", cast=str, default="mypassword")
