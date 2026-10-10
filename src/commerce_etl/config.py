import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    root: Path
    dsn: str
    token_key: str

    @classmethod
    def from_env(cls):
        key = os.environ.get("CUSTOMER_TOKEN_KEY", "")
        if len(key) < 32:
            raise ValueError("CUSTOMER_TOKEN_KEY must have at least 32 characters")
        return cls(
            Path(os.environ.get("ETL_DATA_ROOT", "/opt/airflow/data")),
            os.environ["ORDERS_DATABASE_URL"],
            key,
        )
