import zlib

from django_valkey.compressors.base import BaseCompressor


class ZlibCompressor(BaseCompressor):
    preset = 6

    def _compress(self, value: bytes) -> bytes:
        return zlib.compress(value, self.preset)

    def _decompress(self, value: bytes) -> bytes:
        return zlib.decompress(value)
