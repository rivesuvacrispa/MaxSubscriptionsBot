import asyncio
import csv
import datetime
import io
import logging
import secrets
import os
import pg_storage
import redis_storage
import subscription_check
from typing import Annotated
from fastapi import Depends, FastAPI, HTTPException, status, Request, Body
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.templating import Jinja2Templates
from maxapi import Bot

app = FastAPI()
security = HTTPBasic()
templates = Jinja2Templates(directory="admin-templates")


def basic_auth(
    credentials: Annotated[HTTPBasicCredentials, Depends(security)],
):
    current_username_bytes = credentials.username.encode("utf8")
    correct_username_bytes = os.getenv("ADMIN_USER").encode()
    is_correct_username = secrets.compare_digest(
        current_username_bytes, correct_username_bytes
    )
    current_password_bytes = credentials.password.encode("utf8")
    correct_password_bytes = os.getenv("ADMIN_PASSWORD").encode()
    is_correct_password = secrets.compare_digest(
        current_password_bytes, correct_password_bytes
    )
    if not (is_correct_username and is_correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return True


@app.get("/admin/chats", name="chats")
async def index(request: Request, _: Annotated[str, Depends(basic_auth)]):
    channels = await redis_storage.get_channel_checklist()
    return templates.TemplateResponse(
        request,
        name="chats.html",
        context={"channels": channels}
    )


@app.put("/admin/chats")
async def update_chats(
    chats: list[dict] = Body(...),
    _: Annotated[str, Depends(basic_auth)] = None,
):
    await redis_storage.set_chat_checklist(chats)
    return {"status": "ok"}



@app.get("/admin/users", name="users")
async def index(
    request: Request,
    page: int = 1,
    per_page: int = 50,
    _: Annotated[str, Depends(basic_auth)] = None,
):
    page = max(page, 1)
    per_page = min(max(per_page, 1), 100)

    users, total = await redis_storage.get_users(
        page=page,
        per_page=per_page,
    )

    total_pages = max((total + per_page - 1) // per_page, 1)

    if page > total_pages:
        page = total_pages

        users, total = await redis_storage.get_users(
            page=page,
            per_page=per_page,
        )

    return templates.TemplateResponse(
        request,
        name="users.html",
        context={
            "page_title": "Список участников",
            "users": users,
            "page": page,
            "per_page": per_page,
            "total": total,
            "total_pages": total_pages,
        },
    )


@app.delete("/admin/users/{chat_id}")
async def delete_user(
    chat_id: int,
    _: Annotated[str, Depends(basic_auth)] = None,
):
    await redis_storage.delete_user(chat_id)
    return {"status": "ok"}


@app.get("/admin/users/export", name="users-export")
async def export_users(_: Annotated[str, Depends(basic_auth)]):
    async def generate():
        # BOM, чтобы Excel распознал UTF-8; разделитель ";" под русскую локаль
        buffer = io.StringIO()
        writer = csv.writer(buffer, delimiter=";")
        buffer.write("\ufeff")
        writer.writerow([
            "ID чата",
            "ID пользователя",
            "Имя пользователя",
            "Дата обновления",
        ])

        async for user in redis_storage.iter_all_users():
            # в выгрузку попадают только проверенные участники розыгрыша
            if not user["status"]:
                continue

            writer.writerow([
                user["chat_id"],
                user["user_id"],
                user["username"],
                user["date_updated"],
            ])

            if buffer.tell() > 64 * 1024:
                yield buffer.getvalue()
                buffer.seek(0)
                buffer.truncate()

        yield buffer.getvalue()

    filename = f"participants_verified_{datetime.date.today().isoformat()}.csv"
    return StreamingResponse(
        generate(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# --- Массовая перепроверка подписок ---

# сколько пользователей проверяется одновременно; держим ниже лимита бота,
# чтобы перепроверка не мешала живым нажатиям «Я подписался»
RECHECK_CONCURRENCY = int(os.getenv("RECHECK_CONCURRENCY", "8"))

_recheck_bot: Bot | None = None
# ссылка на текущую задачу, иначе create_task может быть собран GC
_recheck_task: asyncio.Task | None = None


def _get_recheck_bot() -> Bot:
    global _recheck_bot
    if _recheck_bot is None:
        _recheck_bot = Bot(os.getenv("BOT_TOKEN"))
    return _recheck_bot


class _RecheckAborted(Exception):
    pass


async def _recheck_one(bot: Bot, chat_id: int, channels: list[dict]) -> str:
    """Перепроверяет одного пользователя, возвращает итог для счётчиков."""
    user = await redis_storage.get_user(chat_id)
    if user is None:
        return "skipped"  # удалён, пока шла проверка

    unavailable = 0
    missing = False
    for channel in channels:
        member = await subscription_check.get_member_with_retry(
            bot, channel.get("id"), user["user_id"]
        )

        if member is subscription_check.UNAVAILABLE:
            unavailable += 1
            continue

        if member is None:
            missing = True
            break

    if unavailable == len(channels):
        # API не ответил ни по одному каналу — проблема глобальная (бот не
        # админ / токен), менять статусы по такой проверке нельзя
        raise _RecheckAborted(
            "Ни один канал недоступен для проверки. Добавьте бота администратором в каналы."
        )

    new_status = not missing
    if new_status == user["status"]:
        return "unchanged"

    await redis_storage.set_user_status(chat_id, new_status)
    return "promoted" if new_status else "demoted"


async def _run_recheck() -> None:
    progress = {
        "status": "running",
        "total": 0,
        "done": 0,
        "promoted": 0,
        "demoted": 0,
        "error": None,
        "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "finished_at": None,
    }

    try:
        channels = await redis_storage.get_channel_checklist()
        if not channels:
            raise _RecheckAborted("Нет включённых каналов — проверять нечего")

        bot = _get_recheck_bot()
        chat_ids = await redis_storage.get_all_user_chat_ids()
        progress["total"] = len(chat_ids)
        await redis_storage.set_recheck_progress(progress)

        for i in range(0, len(chat_ids), RECHECK_CONCURRENCY):
            chunk = chat_ids[i:i + RECHECK_CONCURRENCY]
            results = await asyncio.gather(
                *[_recheck_one(bot, chat_id, channels) for chat_id in chunk],
                return_exceptions=True,
            )

            for result in results:
                if isinstance(result, _RecheckAborted):
                    raise result
                if isinstance(result, BaseException):
                    raise RuntimeError(f"Сбой проверки пользователя: {result!r}")
                progress["done"] += 1
                if result == "promoted":
                    progress["promoted"] += 1
                elif result == "demoted":
                    progress["demoted"] += 1

            await redis_storage.set_recheck_progress(progress)
            await redis_storage.refresh_recheck_lock()

        progress["status"] = "done"
    except _RecheckAborted as e:
        progress["status"] = "error"
        progress["error"] = str(e)
    except Exception as e:
        logging.exception("Массовая перепроверка упала")
        progress["status"] = "error"
        progress["error"] = f"Внутренняя ошибка: {e!r}"
    finally:
        # порядок важен: сперва финальный прогресс, потом отпустить лок,
        # иначе ручка статуса успеет увидеть running без лока и решить,
        # что проверка прервана
        progress["finished_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        await redis_storage.set_recheck_progress(progress)
        await redis_storage.release_recheck_lock()


@app.post("/admin/users/recheck")
async def start_recheck(_: Annotated[str, Depends(basic_auth)] = None):
    global _recheck_task
    if not await redis_storage.try_acquire_recheck_lock():
        raise HTTPException(status_code=409, detail="Проверка уже идёт")

    _recheck_task = asyncio.create_task(_run_recheck())
    return {"status": "ok"}


@app.get("/admin/users/recheck/status")
async def recheck_status(_: Annotated[str, Depends(basic_auth)] = None):
    progress = await redis_storage.get_recheck_progress()
    if progress is None:
        return {"status": "idle"}

    if progress["status"] == "running" and not await redis_storage.is_recheck_running():
        # лок истёк, а прогресс завис в running — процесс админки умер посреди проверки
        progress["status"] = "error"
        progress["error"] = "Проверка прервана (админка перезапустилась). Запустите заново."

    return progress


# Допустимые исходные статусы для каждого целевого статуса ручек
# pause/resume/cancel. Запуск (draft/paused -> running) валидируется отдельно
# самим pg_storage.start_broadcast. Гард "текущий статус ∈ from" живёт в
# UPDATE на стороне БД, поэтому гонка с воркером даёт 409, а не затирание.
BROADCAST_TRANSITIONS = {
    "paused": ["running"],
    "running": ["paused"],
    "cancelled": ["draft", "running", "paused"],
}

BROADCAST_AUDIENCES = ("all", "verified")


def _serialize_broadcast(b: dict) -> dict:
    def fmt(dt):
        return dt.astimezone(datetime.timezone.utc).isoformat() if dt else None

    return {
        "id": b["id"],
        "audience": b["audience"],
        "body": b["body"],
        "status": b["status"],
        "total": b["total"],
        "sent": b["sent"],
        "failed": b["failed"],
        "created_at": fmt(b["created_at"]),
        "started_at": fmt(b["started_at"]),
        "finished_at": fmt(b["finished_at"]),
    }


@app.get("/admin/broadcasts", name="broadcasts")
async def broadcasts_page(request: Request, _: Annotated[str, Depends(basic_auth)]):
    broadcasts = await pg_storage.list_broadcasts()
    return templates.TemplateResponse(
        request,
        name="broadcasts.html",
        context={
            "page_title": "Рассылки",
            "broadcasts": [_serialize_broadcast(b) for b in broadcasts],
        },
    )


@app.get("/admin/broadcasts/data")
async def broadcasts_data(_: Annotated[str, Depends(basic_auth)]):
    broadcasts = await pg_storage.list_broadcasts()
    return {"broadcasts": [_serialize_broadcast(b) for b in broadcasts]}


@app.post("/admin/broadcasts")
async def create_broadcast(
    payload: dict = Body(...),
    _: Annotated[str, Depends(basic_auth)] = None,
):
    audience = payload.get("audience")
    body = (payload.get("body") or "").strip()

    if audience not in BROADCAST_AUDIENCES:
        raise HTTPException(status_code=400, detail="Некорректная аудитория")
    if not body:
        raise HTTPException(status_code=400, detail="Текст рассылки не может быть пустым")

    broadcast_id = await pg_storage.create_broadcast(audience, body)
    return {"status": "ok", "id": broadcast_id}


@app.post("/admin/broadcasts/{broadcast_id}/start")
async def start_broadcast(
    broadcast_id: int,
    _: Annotated[str, Depends(basic_auth)] = None,
):
    try:
        await pg_storage.start_broadcast(broadcast_id)
    except pg_storage.BroadcastNotFound:
        raise HTTPException(status_code=404, detail="Рассылка не найдена")
    except pg_storage.BroadcastNotStartable:
        raise HTTPException(
            status_code=409,
            detail="Рассылку нельзя запустить из текущего статуса",
        )
    return {"status": "ok"}


@app.post("/admin/broadcasts/{broadcast_id}/pause")
async def pause_broadcast(
    broadcast_id: int,
    _: Annotated[str, Depends(basic_auth)] = None,
):
    return await _set_broadcast_status(broadcast_id, "paused")


@app.post("/admin/broadcasts/{broadcast_id}/resume")
async def resume_broadcast(
    broadcast_id: int,
    _: Annotated[str, Depends(basic_auth)] = None,
):
    return await _set_broadcast_status(broadcast_id, "running")


@app.post("/admin/broadcasts/{broadcast_id}/cancel")
async def cancel_broadcast(
    broadcast_id: int,
    _: Annotated[str, Depends(basic_auth)] = None,
):
    return await _set_broadcast_status(broadcast_id, "cancelled")


async def _set_broadcast_status(broadcast_id: int, status: str) -> dict:
    broadcast = await pg_storage.get_broadcast(broadcast_id)
    if broadcast is None:
        raise HTTPException(status_code=404, detail="Рассылка не найдена")

    try:
        await pg_storage.set_broadcast_status(
            broadcast_id, status, BROADCAST_TRANSITIONS[status]
        )
    except pg_storage.BroadcastStatusConflict:
        raise HTTPException(
            status_code=409,
            detail="Состояние рассылки изменилось, обновите страницу",
        )
    return {"status": "ok"}


@app.get("/admin/bot-messages", name="bot-messages")
async def index(
    request: Request,
    _: Annotated[str, Depends(basic_auth)],
):
    messages = {
        "welcome": await redis_storage.get_welcome_message(),
        "start": await redis_storage.get_start_message(),
        "success": await redis_storage.get_success_message(),
        "fail": await redis_storage.get_fail_message(),
    }

    return templates.TemplateResponse(
        request,
        name="bot-messages.html",
        context={
            "page_title": "Настройка сообщений",
            "messages": messages,
        },
    )


@app.put("/admin/bot-messages")
async def update_bot_messages(
    messages: dict = Body(...),
    _: Annotated[str, Depends(basic_auth)] = None,
):
    await asyncio.gather(
        redis_storage.set_welcome_message(messages["welcome"]),
        redis_storage.set_start_message(messages["start"]),
        redis_storage.set_success_message(messages["success"]),
        redis_storage.set_fail_message(messages["fail"]),
    )

    return {"status": "ok"}