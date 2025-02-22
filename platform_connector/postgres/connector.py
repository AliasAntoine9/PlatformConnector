from typing import List, Union, Iterator, Literal

import pandas as pd
from sqlalchemy import Engine, inspect, text, Connection
from sqlalchemy.engine.cursor import CursorResult

from platform_connector import logger
from platform_connector.postgres.configuration import PostgresConfiguration


class PostgresConnector:
    def __init__(self, vault_config: str, database: str, schema: str):
        self._engine: Union[Engine, None] = None
        self._db_host: Union[str, None] = None
        self._db_port: Union[int, None] = None
        self._database: Union[str, None] = None
        self._schema: Union[str, None] = None
        self._setup(vault_config, database, schema)

    @property
    def engine(self) -> Engine:
        return self._engine

    @property
    def database(self) -> str:
        return self._database

    @property
    def schema(self) -> str:
        return self._schema

    def get_table(
            self,
            table_name: str,
            index_col=None,
            coerce_float=True,
            params=None,
            parse_dates=None,
            columns=None,
            chunksize=None,
            dtype=None
    ) -> [pd.DataFrame, Iterator[pd.DataFrame]]:
        """See pandas doc for more explanations"""
        return pd.read_sql(
            sql=table_name,
            con=self._engine,
            index_col=index_col,
            coerce_float=coerce_float,
            params=params,
            parse_dates=parse_dates,
            columns=columns,
            chunksize=chunksize,
            dtype=dtype
        )

    def write_table(
            self,
            df: pd.DataFrame,
            name: str,
            connection: Union[Engine, Connection, None] = None,
            if_exists: Literal["fail", "replace", "append"]="fail",
            index=True,
            index_label=None,
            chunksize=None,
            dtype=None,
            method=None
    ) -> None:
        """See pandas doc for more explanations"""
        connection = self._engine if connection else connection
        df.to_sql(
            name=name,
            con=connection,
            schema=self.schema,
            if_exists=if_exists,
            index=index,
            index_label=index_label,
            chunksize=chunksize,
            dtype=dtype,
            method=method
        )

    def get_table_names(self) -> List[str]:
        """This method is listing all the tables presents in the Database"""
        inspector = inspect(self.engine)
        return sorted(inspector.get_table_names())

    def execute_query(self, query: str) -> CursorResult:
        """This method can be used when you want to execute a SQL query in the DB"""
        with self.engine.connect() as connection:
            response = connection.execute(text(query))
            connection.commit()
        logger.info("Query executed")
        return response

    def _setup(self, vault_config: str, database: str, schema: str) -> None:
        """This method is setting up the PostgresConnector attributes"""
        postgres_configuration = PostgresConfiguration(vault_config, database, schema)
        self._engine = postgres_configuration.get_engine()
        self._db_host = postgres_configuration.db_host
        self._db_port = postgres_configuration.db_port
        self._database = database
        self._schema = schema
