import pyzstd

from django_valkey.compressors.base import BaseCompressor
from django_valkey.exceptions import CompressorError


class ZStdCompressor(BaseCompressor):
    min_length = 15

    def compress(self, value: bytes) -> bytes:
        if len(value) > self.min_length:
            return pyzstd.compress(value)
        return value

    def decompress(self, value: bytes) -> bytes:
        try:
            return pyzstd.decompress(value)
        except pyzstd.ZstdError as e:
            raise CompressorError from e
