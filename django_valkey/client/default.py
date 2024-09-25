from valkey import Valkey
from django_valkey.base_client import BaseClient


class DefaultClient(BaseClient[Valkey]):
    CONNECTION_FACTORY_PATH = "django_valkey.pool.ConnectionFactory"
