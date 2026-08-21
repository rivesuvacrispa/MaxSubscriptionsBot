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

TRANSIENT_CHECK_ERROR = "Не получилось проверить подписку, попробуйте ещё раз через пару минут."


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

    for counter, channel in enumerate(channels, start=1):
        message += f"""\n№{counter} - <a href="{channel.get('link')}">{channel.get('title')}</a>"""

    participants = await redis_storage.get_participant_count()
    if "{count}" in message:
        message = message.replace("{count}", str(participants))
    else:
        message += f"\n\nУже участвуют: {participants}"

    builder = InlineKeyboardBuilder()
    builder.row(
        CallbackButton(text="Я подписался", payload="check-user"),
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
            channel_id = channel.get('id')
            try:
                member = await bot.get_chat_member(channel_id, user_id)
            except MaxApiError as e:
                if e.code in (403, 404):
                    logging.error(f"Нет доступа к каналу [{channel_id}]! Бот точно администратор? Ошибка:")
                    logging.error(e)
                    # канал недоступен для проверки — исключаем его из требований
                    unavailable += 1
                    continue
                # 429/5xx — временный сбой API: не искажаем результат, просим повторить
                logging.warning(f"Временная ошибка API при проверке канала [{channel_id}]: {e}")
                await callback.chat.send(TRANSIENT_CHECK_ERROR, parse_mode=ParseMode.HTML)
                return
            except Exception as e:
                logging.warning(f"Сбой при проверке канала [{channel_id}]: {e!r}")
                await callback.chat.send(TRANSIENT_CHECK_ERROR, parse_mode=ParseMode.HTML)
                return

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
        for counter, channel in enumerate(missing, start=1):
            message += f"""\n№{counter} - <a href="{channel.get('link')}">{channel.get('title')}</a>"""
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
