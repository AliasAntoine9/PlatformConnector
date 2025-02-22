from typing import Optional, Dict, List
from io import BytesIO

import pandas as pd
from botocore.response import StreamingBody
import boto3
from boto3.s3.transfer import TransferConfig

from platform_connector.s3.configuration import S3Configuration


class S3Connector:
    """
    This class is overloading boto3 client methods
    Boto3 doc:
    """

    def __init__(self, vault_config: str):
        self._s3_client: Optional[boto3.client] = None
        self._bucket: Optional[str] = None
        self._setup(vault_config)

    def get_object(self, key: str, **kwargs) -> bytes:
        """This method is creating a byte object which contains the object retrieved from the S3"""
        try:
            s3_object = self._s3_client.get_object(Bucket=self._bucket, Key=key, **kwargs)
            return s3_object.get("Body").read()
        except Exception as exc:
            raise Exception(f"Impossible to retrieve S3 object. Error: {exc}")


    def read_csv(self, filepath_or_buffer: str, **kwargs) -> pd.DataFrame:
        """This method is retrieving a CSV file store in the S3 and load it in a pandas DataFrame"""
        try:
            pandas_kwargs = {key: kwargs[key] for key in kwargs.keys() if not key.startswith("s3_")}
            s3_kwargs = {key: kwargs[key] for key in kwargs.keys() if key.startswith("s3_")}

            s3_object = self.get_object(Bucket=self._bucket, Key=filepath_or_buffer, **s3_kwargs)
            return pd.read_csv(filepath_or_buffer=filepath_or_buffer, **pandas_kwargs)
        except Exception as exc:
            raise Exception(f"Impossible to retrieve S3 object. Error: {exc}")

    def _setup(self, vault_config: str) -> None:
        """This method is setting up the S3 connector"""
        s3_configuration = S3Configuration(vault_config)
        self._s3_client = s3_configuration.get_boto3_client()
        self._bucket = s3_configuration.bucket
