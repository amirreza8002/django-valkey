import bz2

from django_valkey.compressors.base import BaseCompressor


class Bz2Compressor(BaseCompressor):
    """
    Bz2Compressor
    set with:
    CACHES = {
        "default": {
            # ...
            "OPTIONS": {
                "COMPRESSOR": "django_valkey.compressors.bz2.Bz2Compressor",
                }
            }
        }

    compression parameters:
    to set `compresslevel` use `CACHE_COMPRESS_LEVEL` in your settings, defaults to 4.
    to set `minimum size` set `CACHE_COMPRESS_MIN_LENGTH` in your settings, defaults to 15.
    """

    def _compress(self, value: bytes) -> bytes:
        return bz2.compress(value, compresslevel=self.level or 9)

    def _decompress(self, value: bytes) -> bytes:
        return bz2.decompress(value)
