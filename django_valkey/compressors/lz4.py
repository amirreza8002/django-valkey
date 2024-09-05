from lz4.frame import compress as _compress
from lz4.frame import decompress as _decompress

from django_valkey.compressors.base import BaseCompressor


class Lz4Compressor(BaseCompressor):
    def _compress(self, value: bytes) -> bytes:
        return _compress(value)

    def _decompress(self, value: bytes) -> bytes:
        return _decompress(value)
