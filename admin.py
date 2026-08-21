import asyncio
import csv
import datetime
import io
import secrets
import os
import pg_storage
import redis_storage
from typing import Annotated
from fastapi import Depends, FastAPI, HTTPException, status, Request, Body
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.templating import Jinja2Templates

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
            "Статус",
        ])

        async for user in redis_storage.iter_all_users():
            writer.writerow([
                user["chat_id"],
                user["user_id"],
                user["username"],
                user["date_updated"],
                "Проверен" if user["status"] else "Не подписан / новый",
            ])

            if buffer.tell() > 64 * 1024:
                yield buffer.getvalue()
                buffer.seek(0)
                buffer.truncate()

        yield buffer.getvalue()

    filename = f"participants_{datetime.date.today().isoformat()}.csv"
    return StreamingResponse(
        generate(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


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