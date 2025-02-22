from typing import Dict
from pathlib import Path

import pandas as pd

from platform_connector.postgres.connector import PostgresConnector
from platform_connector.s3.connector import S3Connector


class Postgres:
    connectors: Dict[str, PostgresConnector] = {}

    @staticmethod
    def setup_connector(vault_config: str, database: str, schema: str) -> PostgresConnector:
        if not Postgres.connectors[f"{vault_config}-{database}-{schema}"]:
            Postgres.connectors[f"{vault_config}-{database}-{schema}"] = PostgresConnector(
                vault_config, database, schema
            )
        return Postgres.connectors[f"{vault_config}-{database}-{schema}"]

class S3:
    connectors: Dict[str, S3Connector] = {}

    @staticmethod
    def setup_connector(vault_config: str, database: str, schema: str) -> S3Connector:
        if not S3.connectors[vault_config]:
            S3.connectors[vault_config] = S3Connector(vault_config)
        return S3.connectors[vault_config]


class PlatformConnector:
    postgres = Postgres()
    s3 = S3()

    @property
    def summary(self) -> None:
        summary_postgres = pd.read_csv("data/summary_postgres.csv", sep=";")
        summary_s3 = pd.read_csv("data/summary_s3.csv", sep=";")
        print(summary_postgres, "\n\n", summary_s3)
        return
