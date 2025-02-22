from typing import Optional

from sqlalchemy import Engine, URL, create_engine
from pydantic.types import SecretStr
from urllib3 import disable_warnings, exceptions

from vault_connector import vault_connector

disable_warnings(exceptions.InsecureRequestWarning)


class PostgresConfiguration:
    def __init__(self, vault_config: str, database: str, schema: str, with_echo: bool = False):
        self._drivername: str = "postgresql"
        self._db_host: Optional[str] = None
        self._db_port: Optional[int] = None
        self._db_username: Optional[str] = None
        self._db_password: Optional[SecretStr] = None
        self._database: str = database
        self._database_schema: str = schema
        self._with_echo: bool = with_echo
        self._vault_config: str = vault_config
        self._load_from_vault()

    @property
    def drivername(self) -> str:
        return self._drivername

    @property
    def db_host(self) -> str:
        return self._db_host

    @property
    def db_port(self) -> int:
        return self._db_port

    @property
    def database(self) -> str:
        return self._database

    @property
    def db_username(self) -> str:
        return self._db_username

    @property
    def db_password(self) -> SecretStr:
        return self._db_password

    def _load_from_vault(self) -> None:
        """Loading DB configuration from the vault connector library"""
        try:
            credentials = vault_connector.get(self._vault_config)
            self._db_host = credentials.db_host
            self._db_port = credentials.db_port
            self._db_username = credentials.db_username
            self._db_password = SecretStr(credentials.db_password)

        except KeyError as exc:
            raise KeyError("Unable to get database config from Vault") from exc

    def get_engine(self) -> Engine:
        """
        This method returns a SqlAlchemy engine based on an instance of the 'PostgresConfiguration' class
        """
        return create_engine(
            URL.create(
                drivername=self.drivername,
                host=self._db_host,
                port=self._db_port,
                username=self._db_username,
                password=self._db_password.get_secret_value() if self.db_password else None,
                database=self._database
            ),
            echo=self._with_echo,
            connect_args={"options": f"-csearch_path={self._database_schema}"}
            if self._database_schema is not None else {},
            pool_pre_ping=True
        )