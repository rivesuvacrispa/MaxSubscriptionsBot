import asyncio
import logging
import os

from maxapi.exceptions import MaxApiError

# число попыток и стартовая пауза ретрая при временных сбоях MAX API (429/5xx/сеть)
CHECK_RETRIES = int(os.getenv("CHECK_RETRIES", "4"))
CHECK_RETRY_DELAY = float(os.getenv("CHECK_RETRY_DELAY", "0.5"))

# маркер «канал не удалось проверить» — отличаем от None (не подписан)
UNAVAILABLE = object()


async def get_member_with_retry(bot, channel_id, user_id, on_transient_error=None):
    """Проверка подписки с ретраями.

    Временные сбои API (429/5xx/сеть) ретраятся с экспоненциальной паузой;
    если попытки исчерпаны — канал считается недоступным (в пользу пользователя),
    чтобы юзер никогда не упирался в «попробуйте ещё раз».
    403/404 — бот не админ канала, тоже недоступен.
    """
    delay = CHECK_RETRY_DELAY
    for attempt in range(1, CHECK_RETRIES + 1):
        try:
            return await bot.get_chat_member(channel_id, user_id)
        except MaxApiError as e:
            if e.code in (403, 404):
                logging.error(f"Нет доступа к каналу [{channel_id}]! Бот точно администратор? Ошибка:")
                logging.error(e)
                return UNAVAILABLE
            if on_transient_error:
                on_transient_error()
            logging.warning(f"Временная ошибка API при проверке канала [{channel_id}], попытка {attempt}/{CHECK_RETRIES}: {e}")
        except Exception as e:
            if on_transient_error:
                on_transient_error()
            logging.warning(f"Сбой при проверке канала [{channel_id}], попытка {attempt}/{CHECK_RETRIES}: {e!r}")
        if attempt < CHECK_RETRIES:
            await asyncio.sleep(delay)
            delay *= 2
    logging.error(f"Канал [{channel_id}] не удалось проверить за {CHECK_RETRIES} попыток — исключаем из требований")
    return UNAVAILABLE
