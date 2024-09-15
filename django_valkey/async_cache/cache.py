import contextlib

from valkey.asyncio.client import Valkey as AValkey

from django_valkey.base import BaseValkeyCache
from django_valkey.cache import omit_exception, CONNECTION_INTERRUPTED
from django_valkey.async_cache.client.default import AsyncDefaultClient


class AsyncValkeyCache(BaseValkeyCache[AsyncDefaultClient, AValkey]):
    DEFAULT_CLIENT_CLASS = "django_valkey.async_cache.client.default.AsyncDefaultClient"

    @omit_exception
    async def set(self, *args, **kwargs):
        return await self.client.aset(*args, **kwargs)

    aset = set

    @omit_exception
    async def incr_version(self, *args, **kwargs):
        return await self.client.aincr_version(*args, **kwargs)

    aincr_version = incr_version

    @omit_exception
    async def add(self, *args, **kwargs):
        return await self.client.aadd(*args, **kwargs)

    aadd = add

    async def get(self, key, default=None, version=None, client=None):
        value = await self._get(key, default, version, client)
        if value is CONNECTION_INTERRUPTED:
            value = default
        return value

    aget = get

    @omit_exception(return_value=CONNECTION_INTERRUPTED)
    async def _get(self, key, default=None, version=None, client=None):
        return await self.client.aget(key, default, version, client)

    async def delete(self, *args, **kwargs):
        result = await self.client.adelete(*args, **kwargs)
        return bool(result)

    adelete = delete

    @omit_exception
    async def delete_pattern(self, *args, **kwargs):
        kwargs.setdefault("itersize", self._default_scan_itersize)
        return await self.client.adelete_pattern(*args, **kwargs)

    adelete_pattern = delete_pattern

    @omit_exception
    async def delete_many(self, *args, **kwargs):
        return await self.client.adelete_many(*args, **kwargs)

    adelete_many = delete_many

    @omit_exception
    async def clear(self):
        return await self.client.aclear()

    aclear = clear

    @omit_exception(return_value={})
    async def get_many(self, *args, **kwargs):
        return await self.client.aget_many(*args, **kwargs)

    aget_many = get_many

    @omit_exception
    async def set_many(self, *args, **kwargs):
        return await self.client.aset_many(*args, **kwargs)

    aset_many = set_many

    @omit_exception
    async def incr(self, *args, **kwargs):
        return await self.client.aincr(*args, **kwargs)

    aincr = incr

    @omit_exception
    async def decr(self, *args, **kwargs):
        return await self.client.adecr(*args, **kwargs)

    adecr = decr

    @omit_exception
    async def has_key(self, *args, **kwargs):
        return await self.client.ahas_key(*args, **kwargs)

    ahas_key = has_key

    @omit_exception
    async def keys(self, *args, **kwargs):
        return await self.client.akeys(*args, **kwargs)

    akeys = keys

    @omit_exception
    async def iter_keys(self, *args, **kwargs):
        async with contextlib.aclosing(self.client.aiter_keys(*args, **kwargs)) as it:
            async for key in it:
                yield key

    aiter_keys = iter_keys

    @omit_exception
    async def ttl(self, *args, **kwargs):
        return await self.client.attl(*args, **kwargs)

    attl = ttl

    @omit_exception
    async def pttl(self, *args, **kwargs):
        return await self.client.apttl(*args, **kwargs)

    apttl = pttl

    @omit_exception
    async def persist(self, *args, **kwargs):
        return await self.client.persist(*args, **kwargs)

    apersist = persist

    @omit_exception
    async def expire(self, *args, **kwargs):
        return await self.client.expire(*args, **kwargs)

    aexpire = expire

    @omit_exception
    async def expire_at(self, *args, **kwargs):
        return await self.client.expire_at(*args, **kwargs)

    aexpire_at = expire_at

    @omit_exception
    async def pexpire(self, *args, **kwargs):
        return await self.client.pexpire(*args, **kwargs)

    apexpire = pexpire

    @omit_exception
    async def pexpire_at(self, *args, **kwargs):
        return await self.client.pexpire_at(*args, **kwargs)

    apexpire_at = pexpire_at

    @omit_exception
    async def get_lock(self, *args, **kwargs):
        return await self.client.aget_lock(*args, **kwargs)

    lock = alock = aget_lock = get_lock

    @omit_exception
    async def aclose(self, *args, **kwargs):
        return await self.client.aclose()

    @omit_exception
    async def touch(self, *args, **kwargs):
        return await self.client.touch(*args, **kwargs)

    atouch = touch

    @omit_exception
    async def sadd(self, *args, **kwargs):
        return await self.client.sadd(*args, **kwargs)

    asadd = sadd

    @omit_exception
    async def scard(self, *args, **kwargs):
        return await self.client.scard(*args, **kwargs)

    ascard = scard

    @omit_exception
    async def sdiff(self, *args, **kwargs):
        return await self.client.sdiff(*args, **kwargs)

    asdiff = sdiff

    @omit_exception
    async def sdiffstore(self, *args, **kwargs):
        return await self.client.sdiffstore(*args, **kwargs)

    asdiffstore = sdiffstore

    @omit_exception
    async def sinter(self, *args, **kwargs):
        return await self.client.sinter(*args, **kwargs)

    asinter = sinter

    @omit_exception
    async def sinterstore(self, *args, **kwargs):
        return await self.client.sinterstore(*args, **kwargs)

    asinterstore = sinterstore

    @omit_exception
    async def sismember(self, *args, **kwargs):
        return await self.client.sismember(*args, **kwargs)

    asismember = sismember

    @omit_exception
    async def smembers(self, *args, **kwargs):
        return await self.client.smembers(*args, **kwargs)

    asmembers = smembers

    @omit_exception
    async def smove(self, *args, **kwargs):
        return await self.client.smove(*args, **kwargs)

    asmove = smove

    @omit_exception
    async def spop(self, *args, **kwargs):
        return await self.client.spop(*args, **kwargs)

    aspop = spop

    @omit_exception
    async def srandmember(self, *args, **kwargs):
        return await self.client.srandmember(*args, **kwargs)

    asrandmember = srandmember

    @omit_exception
    async def srem(self, *args, **kwargs):
        return await self.client.srem(*args, **kwargs)

    asrem = srem

    @omit_exception
    async def sscan(self, *args, **kwargs):
        return await self.client.sscan(*args, **kwargs)

    asscan = sscan

    @omit_exception
    async def sscan_iter(self, *args, **kwargs):
        async with contextlib.aclosing(self.client.sscan_iter(*args, **kwargs)) as it:
            async for key in it:
                yield key

    asscan_iter = sscan_iter

    @omit_exception
    async def smismember(self, *args, **kwargs):
        return await self.client.smismember(*args, **kwargs)

    asmismember = smismember

    @omit_exception
    async def sunion(self, *args, **kwargs):
        return await self.client.sunion(*args, **kwargs)

    asunion = sunion

    @omit_exception
    async def sunionstore(self, *args, **kwargs):
        return await self.client.sunionstore(*args, **kwargs)

    asunionstore = sunionstore

    @omit_exception
    async def hset(self, *args, **kwargs):
        return await self.client.hset(*args, **kwargs)

    ahset = hset

    @omit_exception
    async def hdel(self, *args, **kwargs):
        return await self.client.hdel(*args, **kwargs)

    ahdel = hdel

    @omit_exception
    async def hlen(self, *args, **kwargs):
        return await self.client.hlen(*args, **kwargs)

    ahlen = hlen

    @omit_exception
    async def hkeys(self, *args, **kwargs):
        return await self.client.hkeys(*args, **kwargs)

    ahkeys = hkeys

    @omit_exception
    async def hexists(self, *args, **kwargs):
        return await self.client.hexists(*args, **kwargs)

    ahexists = hexists
