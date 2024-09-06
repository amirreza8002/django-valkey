import pyzstd

from django.conf import settings

from django_valkey.compressors.base import BaseCompressor


class ZStdCompressor(BaseCompressor):
    """
    ZStdCompressor
    set with:
    CACHES = {
        "default": {
            # ...
            "OPTIONS": {
                "COMPRESSOR": "django_valkey.compressors.zstd.ZStdCompressor",
                }
            }
        }

    compression parameters:
    to set `level_or_option` use either `CACHE_COMPRESS_LEVEL` or `COMPRESS_ZSTD_OPTIONS` in your settings.
    if `COMPRESSION_ZSTD_OPTIONS` is set, level won't be used

    to set `minimum size` set `CACHE_COMPRESS_MIN_LENGTH` in your settings, defaults to 15.

    to set `zstd_dict` use `COMPRESS_ZSTD_DICT` in your settings, it defaults to None.

    decompression parameters:
    to set `options` use `DECOMPRESS_ZSTD_OPTIONS` in your settings,
    if no value provided, it uses `COMPRESS_ZSTD_OPTIONS`.

    to set `zstd_dict` use `DECOMPRESS_ZSTD_DICT` in your settings,
    if no value is provided `COMPRESS_ZSTD_DICT` is used.

    """

    options: dict | None = getattr(settings, "COMPRESS_ZSTD_OPTIONS", None)
    decomp_options = getattr(settings, "DECOMPRESS_ZSTD_OPTIONS", None)
    zstd_dict = getattr(settings, "COMPRESS_ZSTD_DICT", None)
    decomp_zstd_dict = getattr(settings, "DECOMPRESS_ZSTD_DICT", None)

    def _compress(self, value: bytes) -> bytes:
        return pyzstd.compress(
            value,
            level_or_option=self.options or self.level or 1,
            zstd_dict=self.zstd_dict,
        )

    def _decompress(self, value: bytes) -> bytes:
        return pyzstd.decompress(
            value,
            zstd_dict=self.decomp_zstd_dict or self.zstd_dict,
            option=self.decomp_options or self.options,
        )
