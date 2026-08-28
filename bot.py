import asyncio
import functools
import logging
import os
import time
from urllib.parse import urlparse

from maxapi import Bot, Dispatcher, F
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from maxapi.enums import ParseMode
from maxapi.types import BotStarted, CallbackButton, MessageCallback
from maxapi.utils.inline_keyboard import InlineKeyboardBuilder
import rate_limit
import redis_storage
import subscription_check

logging.basicConfig(level=logging.INFO)

bot = rate_limit.throttle_bot(Bot(os.getenv("BOT_TOKEN"), disable_link_preview=True))
# проверки подписки можно вести отдельным ботом (CHECK_BOT_TOKEN): старый бот
# остаётся админом каналов и проверяет, даже когда диалоги ведёт новый.
# Без CHECK_BOT_TOKEN проверяет основной бот. Лимитер MAX API общий на обоих.
check_bot = (
    rate_limit.throttle_bot(Bot(os.getenv("CHECK_BOT_TOKEN")))
    if os.getenv("CHECK_BOT_TOKEN")
    else bot
)
# use_create_task=True: события обрабатываются параллельно; дефолтный
# последовательный режим давал ~6 нажатий/с — очередь встаёт при наплыве
dp = Dispatcher(use_create_task=True)

# ограничивает число одновременных проверок подписки, чтобы при наплыве
# не выйти за лимиты MAX API; сверх лимита нажатия ждут своей очереди
check_semaphore = asyncio.Semaphore(int(os.getenv("MAX_CONCURRENT_CHECKS", "64")))

# кулдаун кнопки «Я подписался» на пользователя: повторные нажатия в течение
# TTL молча игнорируются — защита от спама кнопкой и лишних вызовов MAX API
CHECK_COOLDOWN = int(os.getenv("CHECK_COOLDOWN", "2"))

# условия розыгрыша и политика конфиденциальности — ссылка в конце приветствия
TERMS_URL = os.getenv(
    "TERMS_URL",
    "https://telegra.ph/USLOVIYA-PROVEDENIYA-STIMULIRUYUSHCHEGO-MEROPRIYATIYA-ROZYGRYSH-3-SAMSUNG-GALAXY-S26-ULTRA-08-28",
)

# --- Prometheus-метрики (HTTP на METRICS_PORT внутри контейнера) ---
EVENTS = Counter("bot_events_total", "Обработанные события бота", ["handler"])
DURATION = Histogram(
    "bot_handler_seconds", "Время обработки события ботом", ["handler"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30),
)
CHECK_RESULTS = Counter("bot_check_results_total", "Итоги проверки подписки", ["result"])
API_TRANSIENT_ERRORS = Counter("bot_api_transient_errors_total", "Временные ошибки MAX API (429/5xx/сеть)")
HANDLER_ERRORS = Counter("bot_handler_errors_total", "Исключения в хендлерах (фейлы отправки и пр.)", ["handler"])
PARTICIPANTS = Gauge("bot_participants", "Подтверждённые участники (реальные, без накрутки)")
USERS_KNOWN = Gauge("bot_users_known", "Все юзеры, известные боту")
# монотонный счётчик из Redis (общий лимитер всех сервисов) — в Grafana берётся rate()
MAX_API_REQUESTS = Gauge("max_api_requests_total", "Запросы к MAX API всех сервисов (из общего лимитера)")


def instrumented(handler_name):
    """Счётчик событий + гистограмма времени обработки для хендлера."""
    def wrap(func):
        HANDLER_ERRORS.labels(handler_name)  # серия видна с 0, не с первой ошибки
        @functools.wraps(func)
        async def inner(event):
            EVENTS.labels(handler_name).inc()
            start = time.monotonic()
            try:
                return await func(event)
            except Exception:
                HANDLER_ERRORS.labels(handler_name).inc()
                raise
            finally:
                DURATION.labels(handler_name).observe(time.monotonic() - start)
        return inner
    return wrap


async def update_gauges():
    while True:
        try:
            PARTICIPANTS.set(await redis_storage.redis_client.scard("users:verified"))
            USERS_KNOWN.set(await redis_storage.redis_client.zcard("users:index"))
            MAX_API_REQUESTS.set(int(await redis_storage.redis_client.get("maxapi:requests") or 0))
        except Exception as e:
            logging.warning(f"Не удалось обновить gauge-метрики: {e!r}")
        await asyncio.sleep(30)


# маркер «канал не удалось проверить» — отличаем от None (не подписан)
_UNAVAILABLE = subscription_check.UNAVAILABLE


async def _get_member_with_retry(channel_id, user_id):
    return await subscription_check.get_member_with_retry(
        check_bot, channel_id, user_id,
        on_transient_error=API_TRANSIENT_ERRORS.inc,
    )


async def _build_checklist_message(verified: bool, chat_id: int) -> str:
    """Стартовое сообщение со списком каналов.

    Для подтверждённого участника каналы помечаются ✅ и подсказка про кнопку
    заменяется на подтверждение участия — кнопка ему больше не нужна.
    """
    message = await redis_storage.get_start_message_for(chat_id)
    channels = await redis_storage.get_channel_checklist()

    mark = "✅" if verified else "❌"
    for channel in channels:
        message += f"""\n{mark} - <a href="{channel.get('link')}">{channel.get('title')}</a>"""

    if verified:
        message += "\n\nВсе условия выполнены — вы участвуете в розыгрыше!"
    else:
        message += "\n\nПосле подписки нажмите «✅ Я подписался», и мы проверим выполнение условий."

    participants = await redis_storage.get_participant_count()
    if "{count}" in message:
        message = message.replace("{count}", str(participants))
    else:
        message += f"\n\nУже участвуют: {participants}"

    message += (
        f"""\n\n<a href="{TERMS_URL}">Условия розыгрыша и политика конфиденциальности</a>"""
    )

    return message


async def _hide_check_button(callback: MessageCallback):
    """Перерисовывает стартовое сообщение подтверждённого участника: ✅ вместо ❌ и без кнопки.

    edit_message без attachments шлёт пустой список вложений — клавиатура снимается.
    """
    original = callback.message
    if original is None:
        return
    try:
        text = await _build_checklist_message(verified=True, chat_id=callback.chat.chat_id)
        await bot.edit_message(
            message_id=original.body.mid,
            text=text,
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        logging.warning(f"Не удалось убрать кнопку проверки: {e!r}")


# Ответ бота при нажатии на кнопку "Начать": сразу список каналов и кнопка проверки
@dp.bot_started()
@instrumented("bot_started")
async def bot_started(event: BotStarted):
    if await redis_storage.is_bot_stopped():
        return

    username = " ".join(filter(None, [event.user.first_name, event.user.last_name]))

    # при смене бота диалоговый chat_id другой — подхватываем старую запись
    # пользователя (статус участия) по глобальному user_id
    if await redis_storage.adopt_user_by_user_id(event.user.user_id, event.chat_id):
        logging.info(
            f"Запись пользователя {event.user.user_id} перенесена на chat_id {event.chat_id}"
        )

    # повторный «Старт» не должен сбрасывать уже подтверждённое участие
    verified = await redis_storage.get_user_status(event.chat_id)

    await redis_storage.save_user(
        chat_id=event.chat_id,
        user_id=event.user.user_id,
        username=username,
        status=verified
    )

    message = await _build_checklist_message(verified=verified, chat_id=event.chat_id)

    attachments = None
    if not verified:
        builder = InlineKeyboardBuilder()
        builder.row(
            CallbackButton(text="✅ Я подписался", payload="check-user"),
        )
        attachments = [builder.as_markup()]

    await event.bot.send_message(
        chat_id=event.chat_id,
        text=message,
        attachments=attachments,
        parse_mode=ParseMode.HTML
    )


# Обработчик нажатия на кнопку "Я подписался"
@dp.message_callback(F.callback.payload == 'check-user')
@instrumented("check_user")
async def check_user(callback: MessageCallback):
    if callback.chat.dialog_with_user is None:
        return

    user_id = callback.chat.dialog_with_user.user_id
    chat_id = callback.chat.chat_id

    if await redis_storage.is_bot_stopped():
        CHECK_RESULTS.labels("bot_stopped").inc()
        return

    if not await redis_storage.try_acquire_check_cooldown(chat_id, CHECK_COOLDOWN):
        CHECK_RESULTS.labels("cooldown").inc()
        return

    user_status = await redis_storage.get_user_status(chat_id)

    if user_status:
        CHECK_RESULTS.labels("already_verified").inc()
        await _hide_check_button(callback)
        message = await redis_storage.get_success_message_for(chat_id)
        await callback.chat.send(message, parse_mode=ParseMode.HTML)
        return

    channels = await redis_storage.get_channel_checklist()
    missing = []
    unavailable = 0
    async with check_semaphore:
        for channel in channels:
            member = await _get_member_with_retry(channel.get('id'), user_id)

            if member is _UNAVAILABLE:
                # канал недоступен для проверки — исключаем его из требований
                unavailable += 1
                continue

            if member is None:
                missing.append(channel)

    if channels and unavailable == len(channels):
        # не удалось проверить ни один канал — не засчитываем участие,
        # иначе при неподключённом боте проверку пройдут все подряд
        CHECK_RESULTS.labels("all_unavailable").inc()
        logging.error("Ни один канал недоступен для проверки, участие не засчитано. Добавьте бота администратором в каналы.")
        message = await redis_storage.get_fail_message()
        await callback.chat.send(message, parse_mode=ParseMode.HTML)
        return

    if missing:
        CHECK_RESULTS.labels("missing").inc()
        message = await redis_storage.get_fail_message()
        for channel in missing:
            message += f"""\n❌ - <a href="{channel.get('link')}">{channel.get('title')}</a>"""
        await callback.chat.send(message, parse_mode=ParseMode.HTML)
        return

    user = callback.chat.dialog_with_user
    username = " ".join(filter(None, [user.first_name, user.last_name]))

    await redis_storage.save_user(
        chat_id=chat_id,
        user_id=user_id,
        username=username,
        status=True
    )
    CHECK_RESULTS.labels("success").inc()
    await _hide_check_button(callback)
    message = await redis_storage.get_success_message_for(chat_id)
    await callback.chat.send(message, parse_mode=ParseMode.HTML)


async def _ensure_webhook_subscription(url: str, secret: str | None) -> None:
    """Приводит подписки MAX к единственной — нашему URL.

    Пауза — чтобы aiohttp-сервер вебхука успел подняться до того, как MAX
    начнёт слать события. Переподписываемся всегда: так подхватывается и
    смена секрета (у существующей подписки его не проверить).
    """
    await asyncio.sleep(2)
    try:
        subs = (await bot.get_subscriptions()).subscriptions or []
        for sub in subs:
            await bot.unsubscribe_webhook(sub.url)
            logging.info(f"Снята подписка вебхука: {sub.url}")
        await bot.subscribe_webhook(url, secret=secret)
        logging.info(f"Вебхук подписан: {url}")
    except Exception:
        logging.exception("Не удалось настроить подписку вебхука")


async def main():
    start_http_server(int(os.getenv("METRICS_PORT", "9114")))
    # бэкфилл uid-индекса до приёма событий: adopt_user_by_user_id при смене
    # бота должен видеть всех исторических пользователей
    await redis_storage.ensure_uid_index()
    asyncio.create_task(update_gauges())

    webhook_url = os.getenv("WEBHOOK_URL")
    if webhook_url:
        # вебхук-режим: MAX сам шлёт события на nginx -> наш aiohttp-сервер.
        # Секрет проверяется библиотекой по заголовку X-Max-Bot-Api-Secret.
        secret = os.getenv("WEBHOOK_SECRET")
        asyncio.create_task(_ensure_webhook_subscription(webhook_url, secret))
        await dp.handle_webhook(
            bot,
            host="0.0.0.0",
            port=int(os.getenv("WEBHOOK_PORT", "8082")),
            path=urlparse(webhook_url).path,
            secret=secret,
        )
    else:
        await dp.start_polling(bot)


if __name__ == '__main__':
    asyncio.run(main())
