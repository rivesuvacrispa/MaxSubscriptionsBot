import datetime
import json
import os
import redis.asyncio as redis

# BlockingConnectionPool: при исчерпании пула запрос ждёт свободное соединение,
# а не падает с MaxConnectionsError (важно при шторме одновременных регистраций)
redis_client = redis.Redis(
    connection_pool=redis.BlockingConnectionPool(
        host=os.getenv("REDIS_HOST"),
        port=int(os.getenv("REDIS_PORT")),
        decode_responses=True,
        max_connections=int(os.getenv("REDIS_MAX_CONNECTIONS", "100")),
        timeout=None,
    )
)


async def get_channel_checklist() -> list[dict]:
    """ Получить список всех чатов по которым нужно проверить подписку

    Формат чата:
    --------
    id: int
        Айди чата/канала
    link: string
        Ссылка на вступление
    title: string
        Название
    enabled: bool
        Требуется ли для участия в розыгрыше
    """
    data = await redis_client.get("chats")

    if data is None:
        return []

    data = json.loads(data)
    return [i for i in data if i.get('enabled', False)]


async def get_all_chats() -> list[dict]:
    """ Получить список всех чатов по которым нужно проверить подписку

    Формат чата:
    --------
    id: int
        Айди чата/канала
    link: string
        Ссылка на вступление
    title: string
        Название
    enabled: bool
        Требуется ли для участия в розыгрыше
    """
    data = await redis_client.get("chats")

    if data is None:
        return []

    return json.loads(data)


async def set_chat_checklist(chats: list[dict]) -> None:
    """ Сохранить/перезаписать список всех чатов для проверки подписок """
    await redis_client.set("chats", json.dumps(chats))


async def get_user_count() -> int:
    """Получить счетчик пользователей, нажавших на кнопку "я подписался"."""
    return await redis_client.zcard("users:index")


async def get_participant_count() -> int:
    """Получить счетчик подтверждённых участников розыгрыша (status=True).

    К реальному числу прибавляется смещение из ключа participants:offset
    (только отображение; в рассылки/снепшоты фейковые участники не попадают).
    """
    real = await redis_client.scard("users:verified")
    offset = await redis_client.get("participants:offset")
    return real + int(offset or 0)


async def get_users(
    page: int = 1,
    per_page: int = 50,
) -> tuple[list[dict], int]:
    """Получить данные пользователей.

    Формат пользователя:
    --------
    chat_id: int
        Айди чата пользователя
    user_id: int
        Айди пользователя
    username: str
        Юзернейм пользователя
    date_updated: string
        Дата обновления статуса
    status: bool
        Статус участия
    """

    start = (page - 1) * per_page
    end = start + per_page - 1

    user_ids = await redis_client.zrevrange(
        "users:index",
        start,
        end,
    )

    async with redis_client.pipeline() as pipe:
        for user_id in user_ids:
            await pipe.hgetall(f"user:{user_id}")

        results = await pipe.execute()

    users = [
        {
            "chat_id": int(user.get("chat_id", 0)),
            "user_id": int(user.get("user_id", 0)),
            "username": user.get("username", ""),
            "date_updated": user.get("date_updated", None),
            "status": bool(int(user.get("status", 0))),
        }
        for user in results
    ]

    total = await get_user_count()
    return users, total


async def iter_all_users(batch_size: int = 500):
    """Итерирует всех пользователей батчами (для выгрузки).

    Формат пользователя такой же, как в get_users().
    """
    start = 0
    while True:
        user_ids = await redis_client.zrevrange(
            "users:index",
            start,
            start + batch_size - 1,
        )

        if not user_ids:
            return

        async with redis_client.pipeline() as pipe:
            for user_id in user_ids:
                await pipe.hgetall(f"user:{user_id}")

            results = await pipe.execute()

        for user in results:
            if not user:
                continue

            yield {
                "chat_id": int(user.get("chat_id", 0)),
                "user_id": int(user.get("user_id", 0)),
                "username": user.get("username", ""),
                "date_updated": user.get("date_updated", None),
                "status": bool(int(user.get("status", 0))),
            }

        start += batch_size


async def iter_chat_ids(verified_only: bool = False, batch_size: int = 1000):
    """Итерирует chat_id всех пользователей (или только подтверждённых).

    Используется для снапшота получателей рассылки. SSCAN может отдавать
    дубликаты — потребитель обязан быть к ним готов (вставка с ON CONFLICT).
    """
    if verified_only:
        cursor = 0
        while True:
            cursor, members = await redis_client.sscan(
                "users:verified", cursor=cursor, count=batch_size
            )
            for member in members:
                yield int(member)
            if cursor == 0:
                return
    else:
        start = 0
        while True:
            chat_ids = await redis_client.zrange(
                "users:index", start, start + batch_size - 1
            )
            if not chat_ids:
                return
            for chat_id in chat_ids:
                yield int(chat_id)
            start += batch_size


async def get_user_status(chat_id: int) -> bool:
    """Получить статус участия пользователя в розыгрыше."""
    status = await redis_client.hget(
        f"user:{chat_id}",
        "status",
    )

    if status is None:
        return False

    return bool(int(status))


async def save_user(chat_id: int, user_id: int, username: str | None, status: bool) -> None:
    """Сохраняет чат-айди пользователя, дату участия и статус проверки пользователя."""

    date_updated = datetime.datetime.now(datetime.timezone.utc)

    await redis_client.hset(
        f"user:{chat_id}",
        mapping={
            "chat_id": chat_id,
            "user_id": user_id,
            "username": str(username),
            "date_updated": date_updated.isoformat(),
            "status": int(status),
        },
    )

    await redis_client.zadd(
        "users:index",
        {
            str(chat_id): date_updated.timestamp(),
        },
    )

    if status:
        await redis_client.sadd("users:verified", str(chat_id))

    # обратный индекс для переноса записи при смене бота (см. adopt_user_by_user_id)
    await redis_client.set(f"uid:{user_id}", str(chat_id))


async def ensure_uid_index() -> None:
    """Разовый бэкфилл обратного индекса uid:{user_id} -> chat_id.

    Для новых записей индекс ведёт save_user; исторические записи досоздаёт
    этот бэкфилл (флаг uid:backfilled). Идём по users:index от старых к новым:
    при дублях user_id побеждает более свежая запись.
    """
    if await redis_client.get("uid:backfilled"):
        return

    start = 0
    batch = 500
    while True:
        chat_ids = await redis_client.zrange("users:index", start, start + batch - 1)
        if not chat_ids:
            break

        async with redis_client.pipeline() as pipe:
            for chat_id in chat_ids:
                await pipe.hget(f"user:{chat_id}", "user_id")
            user_ids = await pipe.execute()

        async with redis_client.pipeline() as pipe:
            for chat_id, user_id in zip(chat_ids, user_ids):
                if user_id:
                    await pipe.set(f"uid:{user_id}", str(chat_id))
            await pipe.execute()

        start += batch

    await redis_client.set("uid:backfilled", "1")


async def adopt_user_by_user_id(user_id: int, new_chat_id: int) -> bool:
    """Переносит запись пользователя на новый chat_id по совпадению user_id.

    Диалоговые chat_id в MAX привязаны к паре «пользователь-бот»: при смене
    бота у того же человека будет другой chat_id, а user_id глобальный.
    Если новый chat_id боту ещё не известен, а по uid-индексу находится
    старая запись — переносим её целиком (статус участия, дату, место
    в индексе) на новый chat_id и удаляем старую. Возвращает True при переносе.
    """
    if await redis_client.exists(f"user:{new_chat_id}"):
        return False

    old_chat_id = await redis_client.get(f"uid:{user_id}")
    if old_chat_id is None or int(old_chat_id) == new_chat_id:
        return False

    old = await redis_client.hgetall(f"user:{old_chat_id}")
    if not old or int(old.get("user_id", 0)) != user_id:
        return False

    score = await redis_client.zscore("users:index", str(old_chat_id))
    if score is None:
        score = datetime.datetime.now(datetime.timezone.utc).timestamp()
    status = bool(int(old.get("status", 0)))

    async with redis_client.pipeline() as pipe:
        await pipe.hset(
            f"user:{new_chat_id}",
            mapping={
                "chat_id": new_chat_id,
                "user_id": user_id,
                "username": old.get("username", ""),
                "date_updated": old.get("date_updated", ""),
                "status": int(status),
            },
        )
        await pipe.zadd("users:index", {str(new_chat_id): score})
        if status:
            await pipe.sadd("users:verified", str(new_chat_id))
        await pipe.delete(f"user:{old_chat_id}")
        await pipe.zrem("users:index", str(old_chat_id))
        await pipe.srem("users:verified", str(old_chat_id))
        await pipe.set(f"uid:{user_id}", str(new_chat_id))
        await pipe.execute()

    return True


async def get_user(chat_id: int) -> dict | None:
    """Получить одного пользователя (формат как в get_users) или None."""
    user = await redis_client.hgetall(f"user:{chat_id}")

    if not user:
        return None

    return {
        "chat_id": int(user.get("chat_id", 0)),
        "user_id": int(user.get("user_id", 0)),
        "username": user.get("username", ""),
        "date_updated": user.get("date_updated", None),
        "status": bool(int(user.get("status", 0))),
    }


async def get_all_user_chat_ids() -> list[int]:
    """Снапшот chat_id всех пользователей.

    Перепроверка идёт по зафиксированному списку: во время неё бот и сама
    проверка двигают score в users:index, и постраничная итерация по нему
    пропускала бы/дублировала пользователей.
    """
    chat_ids = await redis_client.zrevrange("users:index", 0, -1)
    return [int(chat_id) for chat_id in chat_ids]


async def set_user_status(chat_id: int, status: bool) -> None:
    """Обновляет статус участия существующего пользователя (в обе стороны).

    В отличие от save_user умеет и снимать статус: убирает из users:verified.
    """
    date_updated = datetime.datetime.now(datetime.timezone.utc)

    async with redis_client.pipeline() as pipe:
        await pipe.hset(
            f"user:{chat_id}",
            mapping={
                "date_updated": date_updated.isoformat(),
                "status": int(status),
            },
        )
        await pipe.zadd("users:index", {str(chat_id): date_updated.timestamp()})
        if status:
            await pipe.sadd("users:verified", str(chat_id))
        else:
            await pipe.srem("users:verified", str(chat_id))
        await pipe.execute()


async def is_bot_stopped() -> bool:
    """Глобальный рубильник из админки: бот молчит, рассылки стоят."""
    return bool(await redis_client.exists("bot:stopped"))


async def set_bot_stopped(stopped: bool) -> None:
    if stopped:
        await redis_client.set("bot:stopped", "1")
    else:
        await redis_client.delete("bot:stopped")


async def try_acquire_check_cooldown(chat_id: int, ttl: int) -> bool:
    """Кулдаун кнопки проверки подписки: True — нажатие можно обрабатывать,
    False — с прошлого нажатия ещё не прошло ttl секунд."""
    return bool(
        await redis_client.set(f"check:cooldown:{chat_id}", "1", nx=True, ex=ttl)
    )


# Лок массовой перепроверки: TTL страхует от вечного лока при смерти процесса,
# работающая проверка обязана периодически продлевать его refresh_recheck_lock.
# Запас большой: один чанк при шторме ретраев API может идти несколько минут
RECHECK_LOCK_TTL = 900


async def try_acquire_recheck_lock() -> bool:
    """Взять лок перепроверки. False — проверка уже идёт."""
    return bool(await redis_client.set("recheck:lock", "1", nx=True, ex=RECHECK_LOCK_TTL))


async def refresh_recheck_lock() -> None:
    await redis_client.expire("recheck:lock", RECHECK_LOCK_TTL)


async def release_recheck_lock() -> None:
    await redis_client.delete("recheck:lock")


async def is_recheck_running() -> bool:
    return bool(await redis_client.exists("recheck:lock"))


async def set_recheck_progress(progress: dict) -> None:
    """Сохранить прогресс перепроверки (см. формат в admin.py)."""
    await redis_client.set("recheck:progress", json.dumps(progress))


async def get_recheck_progress() -> dict | None:
    data = await redis_client.get("recheck:progress")
    return json.loads(data) if data else None


async def delete_user(chat_id: int) -> None:
    """Полностью удаляет пользователя из базы (хеш, индекс, счётчик участников)."""
    async with redis_client.pipeline() as pipe:
        await pipe.delete(f"user:{chat_id}")
        await pipe.zrem("users:index", str(chat_id))
        await pipe.srem("users:verified", str(chat_id))
        await pipe.execute()


async def set_welcome_message(message: str) -> None:
    await redis_client.set("message:welcome", message)


async def get_welcome_message() -> str:
    message = await redis_client.get("message:welcome")
    return message or "Привет! Отправь мне /start"


async def set_start_message(message: str) -> None:
    await redis_client.set("message:start", message)


async def get_start_message() -> str:
    message = await redis_client.get("message:start")
    return message or "Для участия в розыгрыше вы должны подписаться на следующие каналы:"


async def set_success_message(message: str) -> None:
    await redis_client.set("message:success", message)


async def get_success_message() -> str:
    message = await redis_client.get("message:success")
    return message or "Вы успешно участвуете в розыгрыше!"


async def set_fail_message(message: str) -> None:
    await redis_client.set("message:fail", message)


async def get_fail_message() -> str:
    message = await redis_client.get("message:fail")
    return message or "Вы не подписаны на все каналы!"
