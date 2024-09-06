from django.conf import settings

from django_valkey.exceptions import CompressorError


class BaseCompressor:
    min_length = getattr(settings, "CACHE_COMPRESS_MIN_LENGTH", 15)
    level: int | None = getattr(settings, "CACHE_COMPRESS_LEVEL", None)

    def __init__(self, options):
        self._options: dict = options

    def compress(self, value):
        if len(value) > self.min_length:
            return self._compress(value)
        return value

    def _compress(self, value: bytes) -> bytes:
        raise NotImplementedError

    def decompress(self, value: bytes) -> bytes:
        try:
            return self._decompress(value)
        except Exception as e:
            raise CompressorError from e

    def _decompress(self, value: bytes) -> bytes:
        raise NotImplementedError
