from valkey import Valkey
from django_valkey.base_client import BaseClient


class DefaultClient(BaseClient[Valkey]):
    connection_factory_path = "django_valkey.pool.ConnectionFactory"
