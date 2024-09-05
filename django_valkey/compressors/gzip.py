import gzip

from django_valkey.compressors.base import BaseCompressor


class GzipCompressor(BaseCompressor):
    def _compress(self, value: bytes) -> bytes:
        return gzip.compress(value)

    def _decompress(self, value: bytes) -> bytes:
        return gzip.decompress(value)
