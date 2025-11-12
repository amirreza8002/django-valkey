from typing import Any

import msgspec

from django_valkey.serializers.base import BaseSerializer


class MsgSpecJsonSerializer(BaseSerializer):
    def dumps(self, value: Any) -> bytes:
        return msgspec.json.encode(value)

    def loads(self, value: bytes) -> Any:
        return msgspec.json.decode(value)


class MsgSpecMsgPackSerializer(BaseSerializer):
    def dumps(self, value: Any) -> bytes:
        return msgspec.msgpack.encode(value)

    def loads(self, value: bytes) -> Any:
        return msgspec.msgpack.decode(value)
