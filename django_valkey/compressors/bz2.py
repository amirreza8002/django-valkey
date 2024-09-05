import bz2

from django_valkey.compressors.base import BaseCompressor


class Bz2Compressor(BaseCompressor):
    def _compress(self, value: bytes) -> bytes:
        return bz2.compress(value)

    def _decompress(self, value: bytes) -> bytes:
        return bz2.decompress(value)
