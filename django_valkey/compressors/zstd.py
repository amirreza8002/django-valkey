import pyzstd

from django_valkey.compressors.base import BaseCompressor


class ZStdCompressor(BaseCompressor):
    def _compress(self, value: bytes) -> bytes:
        return pyzstd.compress(value)

    def _decompress(self, value: bytes) -> bytes:
        return pyzstd.decompress(value)
