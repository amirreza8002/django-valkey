import brotli

from django.conf import settings

from django_valkey.compressors.base import BaseCompressor


class BrotliCompressor(BaseCompressor):
    """
     Brotli compressor
     set with:
     CACHES = {
         "default": {
             # ...
             "OPTIONS": {
                 "COMPRESSOR": "django_valkey.compressors.brotli.BrotliCompressor",
                 }
             }
         }

    compression parameters:
     to set `quality` use `CACHE_COMPRESS_LEVEL` in your settings, defaults to 4.
     to set `minimum size` set `CACHE_COMPRESS_MIN_LENGTH` in your settings, defaults to 15.
     to set `lgwin` use `COMPRESS_BROTLI_LGWIN` in your settings, defaults to 22.
     to set `lgblock` use `COMPRESS_BROTLI_LGBLOCK` in your settings, defaults to 0.

     to set `mode` use `COMPRESS_BROTLI_MODE` in your settings,
     the accepted values are: "GENERIC", "TEXT", "FONT"
     the default is GENERIC, if other value is written, GENERIC will be used instead
    """

    mode = getattr(settings, "COMPRESS_BROTLI_MODE", "GENERIC")
    lgwin = getattr(settings, "COMPRESS_BROTLI_LGWIN", 22)
    lgblock = getattr(settings, "COMPRESS_BROTLI_LGBLOCK", 0)

    match mode.upper():
        case "TEXT":
            mode = brotli.MODE_TEXT
        case "FONT":
            mode = brotli.MODE_FONT
        case _:
            mode = brotli.MODE_GENERIC

    def _compress(self, value):
        return brotli.compress(
            value,
            quality=self.level or 11,
            lgwin=self.lgwin,
            lgblock=self.lgblock,
            mode=self.mode,
        )

    def _decompress(self, value):
        return brotli.decompress(value)
