from valkey.cluster import ValkeyCluster

from django_valkey.base import BaseValkeyCache, BackendCommands
from django_valkey.cluster_cache.client import DefaultClusterClient


class ClusterCommands:
    def msetnx(self: "ClusterValkeyCache", *args, **kwargs):
        return self.client.msetnx(*args, **kwargs)

    def mget_nonatomic(self: "ClusterValkeyCache", *args, **kwargs):
        return self.client.mget_nonatomic(*args, **kwargs)

    def mset_nonatomic(self: "ClusterValkeyCache", *args, **kwargs):
        return self.client.mset_nonatomic(*args, **kwargs)

    def readonly(self: "ClusterValkeyCache", *args, **kwargs):
        return self.client.readonly(*args, **kwargs)

    def readwrite(self: "ClusterValkeyCache", *args, **kwargs):
        return self.client.readwrite(*args, **kwargs)

    def keyslot(self: "ClusterValkeyCache", *args, **kwargs):
        return self.client.keyslot(*args, **kwargs)

    def flushall(self: "ClusterValkeyCache", *args, **kwargs):
        return self.client.flushall(*args, **kwargs)

    def invalidate_key_from_cache(self: "ClusterValkeyCache", *args, **kwargs):
        return self.client.invalidate_key_from_cache(*args, **kwargs)


class ClusterValkeyCache(
    BaseValkeyCache[DefaultClusterClient, ValkeyCluster],
    ClusterCommands,
    BackendCommands,
):
    DEFAULT_CLIENT_CLASS = "django_valkey.cluster_cache.client.DefaultClusterClient"
