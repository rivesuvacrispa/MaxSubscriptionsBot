import asyncio
import logging
import os

from maxapi import Bot, Dispatcher, F
from maxapi.enums import ParseMode
from maxapi.exceptions import MaxApiError
from maxapi.types import BotStarted, CallbackButton, MessageCallback
from maxapi.utils.inline_keyboard import InlineKeyboardBuilder
import redis_storage

logging.basicConfig(level=logging.INFO)

bot = Bot(os.getenv("BOT_TOKEN"))
# use_create_task=True: события обрабатываются параллельно; дефолтный
# последовательный режим давал ~6 нажатий/с — очередь встаёт при наплыве
dp = Dispatcher(use_create_task=True)

# ограничивает число одновременных проверок подписки, чтобы при наплыве
# не выйти за лимиты MAX API; сверх лимита нажатия ждут своей очереди
check_semaphore = asyncio.Semaphore(int(os.getenv("MAX_CONCURRENT_CHECKS", "64")))

# число попыток и стартовая пауза ретрая при временных сбоях MAX API (429/5xx/сеть)
CHECK_RETRIES = int(os.getenv("CHECK_RETRIES", "4"))
CHECK_RETRY_DELAY = float(os.getenv("CHECK_RETRY_DELAY", "0.5"))

# маркер «канал не удалось проверить» — отличаем от None (не подписан)
_UNAVAILABLE = object()


async def _get_member_with_retry(channel_id, user_id):
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
                return _UNAVAILABLE
            logging.warning(f"Временная ошибка API при проверке канала [{channel_id}], попытка {attempt}/{CHECK_RETRIES}: {e}")
        except Exception as e:
            logging.warning(f"Сбой при проверке канала [{channel_id}], попытка {attempt}/{CHECK_RETRIES}: {e!r}")
        if attempt < CHECK_RETRIES:
            await asyncio.sleep(delay)
            delay *= 2
    logging.error(f"Канал [{channel_id}] не удалось проверить за {CHECK_RETRIES} попыток — исключаем из требований")
    return _UNAVAILABLE


# Ответ бота при нажатии на кнопку "Начать": сразу список каналов и кнопка проверки
@dp.bot_started()
async def bot_started(event: BotStarted):
    message = await redis_storage.get_start_message()
    channels = await redis_storage.get_channel_checklist()
    username = " ".join(filter(None, [event.user.first_name, event.user.last_name]))

    await redis_storage.save_user(
        chat_id=event.chat_id,
        user_id=event.user.user_id,
        username=username,
        status=False
    )

    for channel in channels:
        message += f"""\n❌ - <a href="{channel.get('link')}">{channel.get('title')}</a>"""

    message += "\n\nПосле подписки нажмите «✅ Я подписался», и мы проверим выполнение условий."

    participants = await redis_storage.get_participant_count()
    if "{count}" in message:
        message = message.replace("{count}", str(participants))
    else:
        message += f"\n\nУже участвуют: {participants}"

    builder = InlineKeyboardBuilder()
    builder.row(
        CallbackButton(text="✅ Я подписался", payload="check-user"),
    )
    await event.bot.send_message(
        chat_id=event.chat_id,
        text=message,
        attachments=[builder.as_markup()],
        parse_mode=ParseMode.HTML
    )


# Обработчик нажатия на кнопку "Я подписался"
@dp.message_callback(F.callback.payload == 'check-user')
async def check_user(callback: MessageCallback):
    if callback.chat.dialog_with_user is None:
        return

    user_id = callback.chat.dialog_with_user.user_id
    chat_id = callback.chat.chat_id
    user_status = await redis_storage.get_user_status(chat_id)

    if user_status:
        message = await redis_storage.get_success_message()
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
        logging.error("Ни один канал недоступен для проверки, участие не засчитано. Добавьте бота администратором в каналы.")
        message = await redis_storage.get_fail_message()
        await callback.chat.send(message, parse_mode=ParseMode.HTML)
        return

    if missing:
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
    message = await redis_storage.get_success_message()
    await callback.chat.send(message, parse_mode=ParseMode.HTML)


async def main():
    await dp.start_polling(bot)


if __name__ == '__main__':
    asyncio.run(main())
