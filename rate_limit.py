"""Общий лимитер запросов к MAX API.

Токен-бакет в Redis, один на все сервисы (бот, рассыльщик, перепроверка из
админки): суммарно не больше MAX_API_RATE запросов/с к API, независимо от
того, из какого контейнера они идут. При пустом бакете вызов ждёт на нашей
стороне вместо того, чтобы ловить 429 от MAX.

Если Redis недоступен, лимитер пропускает вызов без ожидания (fail-open):
сам бот без Redis всё равно неработоспособен, а рассылку лучше не вешать.
"""
import asyncio
import logging
import os

import redis_storage

# суммарный потолок обращений к MAX API, запросов/с (бакет общий на все сервисы)
MAX_API_RATE = float(os.getenv("MAX_API_RATE", "30"))

_BUCKET_KEY = "maxapi:bucket"

# Классический токен-бакет: емкость = rate (допускаем секундный всплеск).
# Возвращает "0" (токен взят) либо время ожидания в секундах (токен НЕ взят).
# Часы берём у Redis (TIME) — общие для всех клиентов бакета.
_LUA = """
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local t = redis.call('TIME')
local now = t[1] + t[2] / 1000000
local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil or ts == nil then
  tokens = capacity
  ts = now
end
tokens = math.min(capacity, tokens + (now - ts) * rate)
local wait = 0
if tokens >= 1 then
  tokens = tokens - 1
else
  wait = (1 - tokens) / rate
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('EXPIRE', KEYS[1], 60)
return tostring(wait)
"""

_script = redis_storage.redis_client.register_script(_LUA)


async def acquire() -> None:
    """Ждёт, пока в общем бакете появится токен на один запрос к MAX API."""
    while True:
        try:
            # таймаут — чтобы при недоступном Redis не висеть на его ретраях
            wait = float(await asyncio.wait_for(
                _script(keys=[_BUCKET_KEY], args=[MAX_API_RATE, MAX_API_RATE]),
                timeout=1.0,
            ))
        except Exception as e:
            logging.warning(f"Лимитер MAX API недоступен, пропускаю без ожидания: {e!r}")
            return
        if wait <= 0:
            return
        await asyncio.sleep(min(wait, 1.0))


def throttle_bot(bot):
    """Оборачивает методы инстанса Bot, ходящие в MAX API, ожиданием бакета.

    Инстансные атрибуты перекрывают методы класса, поэтому под лимитер
    попадают и шорткаты maxapi (chat.send и т.п.) — они зовут те же методы
    этого же инстанса.
    """
    for name in ("send_message", "edit_message", "get_chat_member"):
        orig = getattr(bot, name)

        async def wrapped(*args, _orig=orig, **kwargs):
            await acquire()
            return await _orig(*args, **kwargs)

        setattr(bot, name, wrapped)
    return bot
