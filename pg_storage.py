"""Хранилище массовых рассылок в PostgreSQL.

Схема и семантика перенесены из go-бота gifts (internal/storage/broadcasts.go),
инварианты сохранены:

- дедуп доставок = PK (broadcast_id, chat_id);
- снапшот получателей снимается ТОЛЬКО при первом запуске из draft и больше
  не пересобирается: участник, пришедший после старта, в рассылку не попадает;
- захват доставок через FOR UPDATE SKIP LOCKED + повторный гард
  status='pending' во внешнем UPDATE (CTE и UPDATE — разные снапшоты MVCC);
- mark_delivery_* меняют строку только из 'sending' — повторный (запоздавший)
  вызов при at-least-once доставке не задваивает счётчики;
- смена статуса гардится условием "текущий статус ∈ from" в самом UPDATE,
  поэтому гонка админки с воркером (running -> done) не затирает статус.

Получатели снапшотятся из Redis (источник истины), а не из users_snapshot:
у снепшота лаг до SNAPSHOT_INTERVAL и он хранит удалённых пользователей.
"""
import asyncio
import os

import asyncpg

import redis_storage

MAX_DELIVERY_ATTEMPTS = 5
SNAPSHOT_BATCH = 1000

# произвольная константа-неймспейс для advisory-лока, чтобы конкурентный
# старт admin и broadcaster не выполнял DDL одновременно
_DDL_LOCK_KEY = 0x6D617862

DDL = """
CREATE TABLE IF NOT EXISTS broadcasts (
    id BIGSERIAL PRIMARY KEY,
    audience TEXT NOT NULL CHECK (audience IN ('all', 'verified')),
    body TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft'
        CHECK (status IN ('draft', 'running', 'paused', 'cancelled', 'done')),
    total BIGINT NOT NULL DEFAULT 0,
    sent BIGINT NOT NULL DEFAULT 0,
    failed BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS broadcast_deliveries (
    broadcast_id BIGINT NOT NULL REFERENCES broadcasts (id) ON DELETE CASCADE,
    chat_id BIGINT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'sending', 'sent', 'failed')),
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (broadcast_id, chat_id)
);
CREATE INDEX IF NOT EXISTS broadcast_deliveries_active_idx
    ON broadcast_deliveries (broadcast_id, chat_id)
    WHERE status IN ('pending', 'sending');
"""


class BroadcastNotFound(Exception):
    """Рассылка с таким id не существует."""


class BroadcastNotStartable(Exception):
    """Запуск невозможен: рассылка в терминальном статусе (done/cancelled)."""


class BroadcastStatusConflict(Exception):
    """Статус рассылки изменился конкурентно, переход не применён."""


_pool: asyncpg.Pool | None = None
_pool_lock: asyncio.Lock | None = None


async def get_pool() -> asyncpg.Pool:
    global _pool, _pool_lock
    if _pool is not None:
        return _pool

    # лениво, чтобы Lock создался в работающем event loop процесса
    if _pool_lock is None:
        _pool_lock = asyncio.Lock()

    async with _pool_lock:
        if _pool is not None:
            return _pool
        pool = await asyncpg.create_pool(
            host=os.getenv("PG_HOST"),
            port=int(os.getenv("PG_PORT", "5432")),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            database=os.getenv("PG_DB"),
            ssl=os.getenv("PG_SSLMODE", "require"),
            min_size=1,
            max_size=int(os.getenv("PG_POOL_SIZE", "5")),
            timeout=30,
        )
        async with pool.acquire() as conn:
            await conn.execute("SELECT pg_advisory_lock($1)", _DDL_LOCK_KEY)
            try:
                await conn.execute(DDL)
            finally:
                await conn.execute("SELECT pg_advisory_unlock($1)", _DDL_LOCK_KEY)
        _pool = pool
    return _pool


def _affected(tag: str) -> int:
    # asyncpg возвращает командный тег вида "UPDATE 3"
    return int(tag.rsplit(" ", 1)[-1])


def _row_to_dict(row: asyncpg.Record) -> dict:
    return dict(row)


BROADCAST_FIELDS = (
    "id, audience, body, status, total, sent, failed, "
    "created_at, started_at, finished_at"
)


async def create_broadcast(audience: str, body: str) -> int:
    """Создаёт рассылку в статусе draft, возвращает её id."""
    pool = await get_pool()
    return await pool.fetchval(
        "INSERT INTO broadcasts (audience, body) VALUES ($1, $2) RETURNING id",
        audience,
        body,
    )


async def list_broadcasts() -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch(
        f"SELECT {BROADCAST_FIELDS} FROM broadcasts ORDER BY created_at DESC"
    )
    return [_row_to_dict(r) for r in rows]


async def get_broadcast(broadcast_id: int) -> dict | None:
    pool = await get_pool()
    row = await pool.fetchrow(
        f"SELECT {BROADCAST_FIELDS} FROM broadcasts WHERE id = $1", broadcast_id
    )
    return _row_to_dict(row) if row else None


async def start_broadcast(broadcast_id: int) -> None:
    """Запускает рассылку.

    draft -> снапшот получателей из Redis + total + running (started_at
    выставляется один раз). paused -> running без пересборки снапшота.
    running -> чистый no-op (идемпотентность). done/cancelled ->
    BroadcastNotStartable.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                "SELECT audience, status FROM broadcasts WHERE id = $1 FOR UPDATE",
                broadcast_id,
            )
            if row is None:
                raise BroadcastNotFound(broadcast_id)

            status = row["status"]
            if status == "running":
                return
            if status == "paused":
                await conn.execute(
                    "UPDATE broadcasts SET status = 'running' WHERE id = $1",
                    broadcast_id,
                )
                return
            if status != "draft":
                raise BroadcastNotStartable(status)

            verified_only = row["audience"] == "verified"
            batch: list[tuple[int, int]] = []
            async for chat_id in redis_storage.iter_chat_ids(verified_only):
                batch.append((broadcast_id, chat_id))
                if len(batch) >= SNAPSHOT_BATCH:
                    await _insert_deliveries(conn, batch)
                    batch = []
            if batch:
                await _insert_deliveries(conn, batch)

            await conn.execute(
                """
                UPDATE broadcasts SET
                    total = (SELECT count(*) FROM broadcast_deliveries
                             WHERE broadcast_id = $1),
                    status = 'running',
                    started_at = coalesce(started_at, now())
                WHERE id = $1
                """,
                broadcast_id,
            )


async def _insert_deliveries(conn: asyncpg.Connection, rows: list[tuple[int, int]]) -> None:
    await conn.executemany(
        """
        INSERT INTO broadcast_deliveries (broadcast_id, chat_id)
        VALUES ($1, $2)
        ON CONFLICT (broadcast_id, chat_id) DO NOTHING
        """,
        rows,
    )


async def set_broadcast_status(
    broadcast_id: int, status: str, from_statuses: list[str]
) -> None:
    """Переводит рассылку в status, только если текущий статус ∈ from_statuses.

    Гард живёт в самом UPDATE, поэтому check-then-act атомарен: конкурентный
    воркер (running -> done) и админ-ручка не могут применить несовместимые
    переходы. 0 затронутых строк -> BroadcastStatusConflict (вызывающий код
    сам различает 404/409 по предварительному get_broadcast).
    """
    pool = await get_pool()
    tag = await pool.execute(
        "UPDATE broadcasts SET status = $2 WHERE id = $1 AND status = ANY($3)",
        broadcast_id,
        status,
        from_statuses,
    )
    if _affected(tag) == 0:
        raise BroadcastStatusConflict(broadcast_id)


async def claim_deliveries(batch: int) -> list[dict]:
    """Атомарно переводит до batch доставок pending -> sending (только у
    рассылок в статусе running) и возвращает их вместе с текстом.

    FOR UPDATE SKIP LOCKED защищает от двойного захвата параллельными
    воркерами; повторный гард d.status='pending' во внешнем UPDATE — от
    возврата в sending уже финализированной строки (CTE и внешний UPDATE
    видят разные снапшоты MVCC).
    """
    pool = await get_pool()
    rows = await pool.fetch(
        """
        WITH picked AS (
            SELECT bd.broadcast_id, bd.chat_id
            FROM broadcast_deliveries bd
            JOIN broadcasts b ON b.id = bd.broadcast_id AND b.status = 'running'
            WHERE bd.status = 'pending'
            ORDER BY bd.broadcast_id, bd.chat_id
            LIMIT $1 FOR UPDATE OF bd SKIP LOCKED
        )
        UPDATE broadcast_deliveries d
        SET status = 'sending', updated_at = now()
        FROM picked p JOIN broadcasts b ON b.id = p.broadcast_id
        WHERE d.broadcast_id = p.broadcast_id
          AND d.chat_id = p.chat_id
          AND d.status = 'pending'
        RETURNING d.broadcast_id, d.chat_id, b.body, d.attempts
        """,
        batch,
    )
    return [_row_to_dict(r) for r in rows]


async def mark_delivery_sent(broadcast_id: int, chat_id: int) -> None:
    """Помечает доставку sent и инкрементирует счётчик sent рассылки.

    Гард status='sending': повторный вызов на уже финализированной строке —
    no-op, счётчик не задваивается.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            tag = await conn.execute(
                """
                UPDATE broadcast_deliveries SET status = 'sent', updated_at = now()
                WHERE broadcast_id = $1 AND chat_id = $2 AND status = 'sending'
                """,
                broadcast_id,
                chat_id,
            )
            if _affected(tag) > 0:
                await conn.execute(
                    "UPDATE broadcasts SET sent = sent + 1 WHERE id = $1",
                    broadcast_id,
                )


async def mark_delivery_failed(
    broadcast_id: int, chat_id: int, error: str, retriable: bool
) -> None:
    """Фиксирует ошибку доставки.

    Ретраибельная ошибка при attempts < MAX_DELIVERY_ATTEMPTS возвращает
    доставку в pending, иначе доставка уходит в failed и инкрементируется
    счётчик failed рассылки. Гард status='sending' — повторный вызов no-op.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            attempts = await conn.fetchval(
                """
                UPDATE broadcast_deliveries
                SET attempts = attempts + 1, last_error = $3, updated_at = now()
                WHERE broadcast_id = $1 AND chat_id = $2 AND status = 'sending'
                RETURNING attempts
                """,
                broadcast_id,
                chat_id,
                error,
            )
            if attempts is None:
                return

            if retriable and attempts < MAX_DELIVERY_ATTEMPTS:
                await conn.execute(
                    """
                    UPDATE broadcast_deliveries SET status = 'pending'
                    WHERE broadcast_id = $1 AND chat_id = $2
                    """,
                    broadcast_id,
                    chat_id,
                )
            else:
                await conn.execute(
                    """
                    UPDATE broadcast_deliveries SET status = 'failed'
                    WHERE broadcast_id = $1 AND chat_id = $2
                    """,
                    broadcast_id,
                    chat_id,
                )
                await conn.execute(
                    "UPDATE broadcasts SET failed = failed + 1 WHERE id = $1",
                    broadcast_id,
                )


async def requeue_stale_sending(older_than_seconds: float) -> int:
    """Возвращает в pending доставки, зависшие в sending дольше порога
    (воркер упал, не успев отметить результат). Только для running-рассылок —
    иначе paused/cancelled вечно оживляли бы свои sending-строки.
    """
    pool = await get_pool()
    tag = await pool.execute(
        """
        UPDATE broadcast_deliveries d SET status = 'pending', updated_at = now()
        FROM broadcasts b
        WHERE d.broadcast_id = b.id AND b.status = 'running'
          AND d.status = 'sending'
          AND d.updated_at < now() - make_interval(secs => $1)
        """,
        older_than_seconds,
    )
    return _affected(tag)


async def finish_broadcast_if_done(broadcast_id: int) -> bool:
    """Переводит рассылку в done, когда не осталось pending/sending доставок.
    Возвращает True, если рассылка завершена именно этим вызовом."""
    pool = await get_pool()
    tag = await pool.execute(
        """
        UPDATE broadcasts SET status = 'done', finished_at = now()
        WHERE id = $1 AND status = 'running'
          AND NOT EXISTS (
              SELECT 1 FROM broadcast_deliveries
              WHERE broadcast_id = $1 AND status IN ('pending', 'sending')
          )
        """,
        broadcast_id,
    )
    return _affected(tag) > 0


async def running_broadcast_ids() -> list[int]:
    """id всех running-рассылок — для реконсиляции осиротевших рассылок."""
    pool = await get_pool()
    rows = await pool.fetch("SELECT id FROM broadcasts WHERE status = 'running' ORDER BY id")
    return [r["id"] for r in rows]
