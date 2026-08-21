"""Фоновый воркер массовых рассылок.

Цикл (перенесён из go-бота gifts, internal/broadcast/worker.go):
requeue зависших sending -> claim батча -> отправка с троттлингом ->
mark sent/failed -> finish_broadcast_if_done по затронутым рассылкам;
на пустом батче — реконсиляция осиротевших running-рассылок + пауза.

Доставка at-least-once: если воркер упал между отправкой и отметкой,
stale-таймер вернёт доставку в pending и сообщение может уйти повторно.
"""
import asyncio
import logging
import os

from maxapi import Bot
from maxapi.enums import ParseMode
from maxapi.exceptions import MaxApiError

import pg_storage

logging.basicConfig(level=logging.INFO)

CLAIM_BATCH = 100
IDLE_SLEEP = 1.0
# порог "зависания" доставки в sending: после него requeue вернёт её в pending
STALE_SECONDS = 60.0
# троттлинг отправки, сообщений в секунду
RATE = float(os.getenv("BROADCAST_RATE", "20"))

bot = Bot(os.getenv("BOT_TOKEN"))


def is_terminal_error(e: MaxApiError) -> bool:
    """Постоянная ошибка отправки — повторять бессмысленно (пользователь
    заблокировал бота, чат недоступен, некорректный запрос). Временными
    считаем только 429 и 5xx."""
    return e.code != 429 and e.code < 500


async def send_one(delivery: dict) -> None:
    broadcast_id = delivery["broadcast_id"]
    chat_id = delivery["chat_id"]

    try:
        await bot.send_message(
            chat_id=chat_id,
            text=delivery["body"],
            parse_mode=ParseMode.HTML,
        )
    except MaxApiError as e:
        retriable = not is_terminal_error(e)
        logging.warning(
            "Ошибка отправки %s/%s (retriable=%s): %s",
            broadcast_id, chat_id, retriable, e,
        )
        await pg_storage.mark_delivery_failed(broadcast_id, chat_id, str(e), retriable)
        return
    except Exception as e:
        # сеть/таймаут — временный сбой, доставка вернётся на повтор
        logging.warning("Сбой отправки %s/%s: %r", broadcast_id, chat_id, e)
        await pg_storage.mark_delivery_failed(broadcast_id, chat_id, repr(e), True)
        return

    await pg_storage.mark_delivery_sent(broadcast_id, chat_id)


async def reconcile_running() -> None:
    """Закрывает осиротевшие рассылки: у running-рассылки не осталось
    pending/sending доставок, но finish не был вызван (рестарт воркера)."""
    for broadcast_id in await pg_storage.running_broadcast_ids():
        if await pg_storage.finish_broadcast_if_done(broadcast_id):
            logging.info("Рассылка %s завершена (реконсиляция)", broadcast_id)


async def run() -> None:
    logging.info("Воркер рассылок запущен: батч %s, %s сообщ/с", CLAIM_BATCH, RATE)
    while True:
        try:
            requeued = await pg_storage.requeue_stale_sending(STALE_SECONDS)
            if requeued:
                logging.info("Возвращено зависших доставок: %s", requeued)

            deliveries = await pg_storage.claim_deliveries(CLAIM_BATCH)

            if not deliveries:
                await reconcile_running()
                await asyncio.sleep(IDLE_SLEEP)
                continue

            touched = set()
            for delivery in deliveries:
                await send_one(delivery)
                touched.add(delivery["broadcast_id"])
                await asyncio.sleep(1 / RATE)

            for broadcast_id in touched:
                if await pg_storage.finish_broadcast_if_done(broadcast_id):
                    logging.info("Рассылка %s завершена", broadcast_id)
        except Exception:
            logging.exception("Сбой цикла рассылки, повтор через %s сек", IDLE_SLEEP)
            await asyncio.sleep(IDLE_SLEEP)


if __name__ == "__main__":
    asyncio.run(run())
