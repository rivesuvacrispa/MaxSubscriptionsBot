"""Фоновые снепшоты данных из Redis в PostgreSQL.

Раз в SNAPSHOT_INTERVAL секунд инкрементально копирует участников
(по score в users:index — это timestamp последнего обновления) и настройки
(каналы, тексты сообщений) в PG. Водяной знак хранится в самой PG,
так что перезапуск сервиса не теряет прогресс и не дублирует работу.
"""
import asyncio
import datetime
import json
import logging
import os

import asyncpg

import redis_storage
from redis_storage import redis_client

logging.basicConfig(level=logging.INFO)

INTERVAL = int(os.getenv("SNAPSHOT_INTERVAL", "300"))
# перекрытие по времени, чтобы не потерять обновления на границе прошлого прохода
OVERLAP = 60
BATCH = 500

DDL = """
CREATE TABLE IF NOT EXISTS users_snapshot (
    chat_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    username TEXT,
    date_updated TIMESTAMPTZ,
    status BOOLEAN NOT NULL DEFAULT FALSE,
    snapshotted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS kv_snapshot (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS snapshot_state (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""

UPSERT_USER = """
INSERT INTO users_snapshot (chat_id, user_id, username, date_updated, status, snapshotted_at)
VALUES ($1, $2, $3, $4, $5, now())
ON CONFLICT (chat_id) DO UPDATE SET
    user_id = EXCLUDED.user_id,
    username = EXCLUDED.username,
    date_updated = EXCLUDED.date_updated,
    status = EXCLUDED.status,
    snapshotted_at = now()
"""

UPSERT_KV = """
INSERT INTO kv_snapshot (key, value, updated_at) VALUES ($1, $2, now())
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()
WHERE kv_snapshot.value IS DISTINCT FROM EXCLUDED.value
"""


def connect():
    return asyncpg.connect(
        host=os.getenv("PG_HOST"),
        port=int(os.getenv("PG_PORT", "5432")),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        database=os.getenv("PG_DB"),
        ssl="require",
        timeout=30,
    )


def parse_date(value: str | None) -> datetime.datetime | None:
    if not value:
        return None
    try:
        return datetime.datetime.fromisoformat(value)
    except ValueError:
        return None


async def snapshot_users(conn: asyncpg.Connection) -> int:
    raw = await conn.fetchval(
        "SELECT value FROM snapshot_state WHERE key = 'users_watermark'"
    )
    watermark = float(raw) if raw else 0.0
    since = max(watermark - OVERLAP, 0.0)

    chat_ids = await redis_client.zrangebyscore(
        "users:index", since, "+inf", withscores=True
    )
    if not chat_ids:
        return 0

    new_watermark = max(score for _, score in chat_ids)

    copied = 0
    for start in range(0, len(chat_ids), BATCH):
        chunk = chat_ids[start:start + BATCH]

        async with redis_client.pipeline() as pipe:
            for chat_id, _ in chunk:
                await pipe.hgetall(f"user:{chat_id}")
            results = await pipe.execute()

        rows = [
            (
                int(user.get("chat_id", 0)),
                int(user.get("user_id", 0)),
                user.get("username", ""),
                parse_date(user.get("date_updated")),
                bool(int(user.get("status", 0))),
            )
            for user in results
            if user
        ]
        if rows:
            await conn.executemany(UPSERT_USER, rows)
            copied += len(rows)

    await conn.execute(
        """
        INSERT INTO snapshot_state (key, value) VALUES ('users_watermark', $1)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """,
        str(new_watermark),
    )
    return copied


async def snapshot_meta(conn: asyncpg.Connection) -> None:
    chats = await redis_storage.get_all_chats()
    messages = {
        "welcome": await redis_storage.get_welcome_message(),
        "start": await redis_storage.get_start_message(),
        "success": await redis_storage.get_success_message(),
        "fail": await redis_storage.get_fail_message(),
    }
    await conn.execute(UPSERT_KV, "chats", json.dumps(chats, ensure_ascii=False))
    await conn.execute(UPSERT_KV, "messages", json.dumps(messages, ensure_ascii=False))


async def run_once() -> None:
    conn = await connect()
    try:
        await conn.execute(DDL)
        copied = await snapshot_users(conn)
        await snapshot_meta(conn)

        pg_total = await conn.fetchval("SELECT count(*) FROM users_snapshot")
        redis_total = await redis_storage.get_user_count()
        logging.info(
            "Снепшот готов: скопировано %s, в PG %s, в Redis %s",
            copied, pg_total, redis_total,
        )
        if pg_total > redis_total:
            logging.warning(
                "В PG больше записей, чем в Redis (%s > %s) — вероятно, "
                "часть пользователей удалена из админки; снепшот их сохраняет",
                pg_total, redis_total,
            )
    finally:
        await conn.close()


async def main() -> None:
    logging.info("Сервис снепшотов запущен, интервал %s сек", INTERVAL)
    while True:
        try:
            await run_once()
        except Exception:
            logging.exception("Ошибка снепшота, повтор через %s сек", INTERVAL)
        await asyncio.sleep(INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())
