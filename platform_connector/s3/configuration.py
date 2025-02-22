from typing import Union

import boto3

from vault_connector import vault_connector


class S3Configuration:
    """
    This class is a connector to the COS (Cloud Object Storage) of IBM
    Boto3 (the AWS library's) is used to connect to the COS
    """
    _instance = None

    def __init__(self, vault_config: str) -> None:
        self._vault_config: str = vault_config
        self._cos_endpoint: str = "https://s3.direct.eu-fr0.cloud-object-storage"
        self._hmac_access_key_id: Union[str, None] = None
        self._hmac_secret_access_key: Union[str, None] = None
        self._bucket: Union[str, None] = None
        self._load_from_vault()

    def __new__(cls, *args, **kwargs):
        """Overload __new__ method to create a singleton for the COSConfiguration object"""
        if cls._instance is None:
            cls._instance = super(S3Configuration, cls).__new__(cls)
        return cls._instance

    @property
    def bucket(self) -> str:
        return self._bucket

    def _load_from_vault(self) -> None:
        """Loading COS credentials from Vault Connector"""
        try:
            vault = vault_connector.get(self._vault_config)
            self._hmac_access_key_id = vault.hmac_access_key_id
            self._hmac_secret_access_key = vault.hmac_secret_access_key
            self._bucket = vault.bucket
        except KeyError as err:
            raise KeyError(f"Unable to get COS credentials from Vault Connector") from err

    def get_boto3_client(self) -> boto3.client:
        """This method creates a boto3 client"""
        return boto3.client(
            service_name="s3",
            aws_access_key_id=self._hmac_access_key_id,
            aws_secret_access_key=self._hmac_secret_access_key,
            endpoint_url=self._cos_endpoint
        )
