import asyncio
import logging
import os

from maxapi import Bot, Dispatcher
from maxapi.enums import ParseMode
from maxapi.types import BotStarted, Command, MessageCreated, CallbackButton, MessageCallback
from maxapi.utils.inline_keyboard import InlineKeyboardBuilder
import redis_storage

logging.basicConfig(level=logging.INFO)

bot = Bot(os.getenv("BOT_TOKEN"))
dp = Dispatcher()


# Ответ бота при нажатии на кнопку "Начать"
@dp.bot_started()
async def bot_started(event: BotStarted):
    message = await redis_storage.get_welcome_message()
    await event.bot.send_message(
        chat_id=event.chat_id,
        text=message
    )


# Ответ бота на команду /start
@dp.message_created(Command('start'))
async def hello(event: MessageCreated):
    if event.chat.dialog_with_user is None:
        return

    message = await redis_storage.get_start_message()
    channels = await redis_storage.get_channel_checklist()
    username = f"{event.from_user.first_name} {event.from_user.last_name}"

    await redis_storage.save_user(
        chat_id=event.chat.chat_id,
        user_id=event.chat.dialog_with_user.user_id,
        username=username,
        status=False
    )

    counter = 1
    for channel in channels:
        message += f"""\n#№{counter} - <a href="{channel.get('link')}">{channel.get('title')}</a>"""

    builder = InlineKeyboardBuilder()
    builder.row(
        CallbackButton(text="Я подписался", payload="check-user"),
    )
    await event.message.answer(
        message,
        attachments=[builder.as_markup()],
        parse_mode=ParseMode.HTML
    )


# Обработчик нажатия на кнопку "Я подписался"
@dp.message_callback('check-user')
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
    for channel in channels:
        channel_id = channel.get('id')
        is_member = False
        try:
            member = await bot.get_chat_member(channel_id, user_id)
            is_member = member is not None
        except Exception as e:
            logging.error(f"Не удалось проверить пользователя в канале [{channel_id}]! Бот точно администратор? Ошибка:")
            logging.error(e)

        if not is_member:
            message = await redis_storage.get_fail_message()
            await callback.chat.send(message, parse_mode=ParseMode.HTML)
            return

    await redis_storage.save_user(
        chat_id=chat_id,
        user_id=user_id,
        username=callback.chat.dialog_with_user.username,
        status=True
    )
    message = await redis_storage.get_success_message()
    await callback.chat.send(message, parse_mode=ParseMode.HTML)


async def main():
    await dp.start_polling(bot)


if __name__ == '__main__':
    asyncio.run(main())
