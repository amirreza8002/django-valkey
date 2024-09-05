from django_valkey.compressors.base import BaseCompressor


class IdentityCompressor(BaseCompressor):
    min_length = 0

    def _compress(self, value: bytes) -> bytes:
        return value

    def _decompress(self, value: bytes) -> bytes:
        return value
