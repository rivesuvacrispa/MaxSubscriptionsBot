import asyncio
import secrets
import os
import redis_storage
from typing import Annotated
from fastapi import Depends, FastAPI, HTTPException, status, Request, Body
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