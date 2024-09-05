import lzma

from django_valkey.compressors.base import BaseCompressor


class LzmaCompressor(BaseCompressor):
    min_length = 100

    def _compress(self, value: bytes) -> bytes:
        return lzma.compress(value, preset=self.preset)

    def _decompress(self, value: bytes) -> bytes:
        return lzma.decompress(value)
