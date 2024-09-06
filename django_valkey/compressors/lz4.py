from lz4.frame import compress
from lz4.frame import decompress

from django.conf import settings

from django_valkey.compressors.base import BaseCompressor


class Lz4Compressor(BaseCompressor):
    """
    Lz4 compressor
    set with:
    CACHES = {
        "default": {
            # ...
            "OPTIONS": {
                "COMPRESSOR": "django_valkey.compressors.lz4.Lz4Compressor",
                }
            }
        }

    compression parameters:
    to set `compression_level` use `CACHE_COMPRESS_LEVEL` in your settings, defaults to 4.
    to set `minimum size` set `CACHE_COMPRESS_MIN_LENGTH` in your settings, defaults to 15.
    to set `block_size` use `COMPRESS_LZ4_BLOCK_SIZE` in your settings, it defaults to 0.
    to set `content_checksum` use `COMPRESS_LZ4_CONTENT_CHECKSUM` in your settings, it defaults to 0.
    to set `blocked_linked` use `COMPRESS_LZ4_BLOCK_LINKED` in your settings, it defaults to True.
    to set `store_size` use `COMPRESS_LZ4_STORE_SIZE` in your settings, it defaults to True.
    """

    block_size = getattr(settings, "COMPRESS_LZ4_BLOCK_SIZE", 0)
    content_checksum = getattr(settings, "COMPRESS_LZ4_CONTENT_CHECKSUM", 0)
    block_linked = getattr(settings, "COMPRESS_LZ4_BLOCK_LINKED", True)
    store_size = getattr(settings, "COMPRESS_LZ4_STORE_SIZE", True)

    def _compress(self, value: bytes) -> bytes:
        return compress(
            value,
            compression_level=self.level or 0,
            block_size=self.block_size,
            content_checksum=self.content_checksum,
            block_linked=self.block_linked,
            store_size=self.store_size,
        )

    def _decompress(self, value: bytes) -> bytes:
        return decompress(value)
