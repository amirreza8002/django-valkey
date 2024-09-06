from django_valkey.compressors.base import BaseCompressor


class IdentityCompressor(BaseCompressor):
    """
    the default class used by django_valkey
    it simple returns the value, with no change
    exists to simplify switching to compressors
    """

    def _compress(self, value: bytes) -> bytes:
        return value

    def _decompress(self, value: bytes) -> bytes:
        return value
