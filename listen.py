# listen.py — сбор контента из заданных групп/каналов:
# - слушаем только MONITORED_CHATS
# - скачиваем фото/видео и альбомы
# - загружаем в Supabase Storage (публичный бакет)
# - пишем одну запись в incoming_posts с JSON-массивом photo_list

import os
import asyncio
from datetime import datetime
from typing import List, Dict, Optional

from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.tl.types import User
from supabase import create_client, Client

load_dotenv()

# Telegram API
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
SESSION_NAME = os.getenv("SESSION_NAME", "collector")

# Что мониторим (через запятую): @chan1,@chan2,-1001234567890
MONITORED_CHATS_ENV = os.getenv("MONITORED_CHATS", "-1003047673137")


def _parse_monitored(env: str) -> List[object]:
    out: List[object] = []
    for raw in env.split(","):
        t = raw.strip()
        if not t:
            continue
        # поддержка числовых id
        try:
            if t.startswith("-") and t[1:].isdigit():
                out.append(int(t))
            elif t.isdigit():
                out.append(int(t))
            else:
                out.append(t)  # @username или ссылка без пробелов
        except Exception:
            out.append(t)
    return out


MONITORED_CHATS = _parse_monitored(MONITORED_CHATS_ENV)

# Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET", "tg_media")  # публичный бакет

if not API_ID or not API_HASH:
    raise RuntimeError("API_ID/API_HASH не заданы в .env")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL/SUPABASE_KEY не заданы в .env")

if not MONITORED_CHATS:
    raise RuntimeError("MONITORED_CHATS пуст — укажите каналы/группы для мониторинга")

supa: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ---------- Хелперы ----------


def _best_caption(messages) -> str:
    caps = [(m.raw_text or "").strip() for m in messages]
    caps = [c for c in caps if c]
    return max(caps, key=len) if caps else ""


def _get_public_url(path: str) -> Optional[str]:
    res = supa.storage.from_(SUPABASE_BUCKET).get_public_url(path)
    if isinstance(res, dict):
        return (res.get("data") or {}).get("publicUrl") or res.get("publicUrl")
    try:
        return getattr(res, "data", {}).get("publicUrl")
    except Exception:
        return None


def _upload_file(local_path: str, dest_path: str) -> Dict[str, Optional[str]]:
    # supabase-py: upload(path, file_obj) для Python SDK
    with open(local_path, "rb") as f:
        supa.storage.from_(SUPABASE_BUCKET).upload(dest_path, f)
    return {"path": dest_path, "public_url": _get_public_url(dest_path)}


def _upload_many(local_paths: List[str], base_dest: str) -> List[Dict[str, Optional[str]]]:
    out = []
    for i, p in enumerate(local_paths, start=1):
        # расширение берём из скачанного файла (.jpg, .png, .mp4 и т.д.)
        ext = os.path.splitext(p)[1] or ".bin"
        dest = f"{base_dest}/{i}{ext}"

        item = _upload_file(p, dest)
        item["index"] = i
        out.append(item)
    return out


def _insert_post_row(row: dict) -> None:
    supa.table("incoming_posts").insert(row).execute()


def _has_media(msg) -> bool:
    # Фото
    if getattr(msg, "photo", None):
        return True

    # Видео (shortcut-поле Telethon)
    if getattr(msg, "video", None):
        return True

    # Видео как документ с MIME-типом video/*
    doc = getattr(msg, "document", None)
    mime = getattr(doc, "mime_type", None) if doc else None
    if mime and mime.startswith("video/"):
        return True

    return False


async def _chat_title(event) -> str:
    try:
        chat = await event.get_chat()
        return getattr(chat, "title", None) or getattr(chat, "username", None) or str(event.chat_id)
    except Exception:
        return str(event.chat_id)


async def _sender_meta(event):
    username, full_name = None, ""
    try:
        s = await event.get_sender()
        if isinstance(s, User):
            first = getattr(s, "first_name", "") or ""
            last = getattr(s, "last_name", "") or ""
            full_name = f"{first} {last}".strip()
            username = getattr(s, "username", None)
    except Exception:
        pass
    return username, full_name


# ---------- Основной запуск ----------


async def run():
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.start()

    print("🚀 Collector запущен, слушаем:", MONITORED_CHATS)

    # ======== ОБРАБОТЧИК АЛЬБОМОВ (фото+видео) ========

    @client.on(events.Album(chats=MONITORED_CHATS))
    async def handle_album(event):
        chat_name = await _chat_title(event)
        username, full_name = await _sender_meta(event)
        text = _best_caption(event.messages) or ""

        media_dir = "./downloaded_media"
        os.makedirs(media_dir, exist_ok=True)

        local_paths: List[str] = []

        for i, msg in enumerate(event.messages, start=1):
            if not _has_media(msg):
                continue

            fn = f"{event.chat_id}_{event.messages[0].id}_{i}"
            p = await msg.download_media(file=os.path.join(media_dir, fn))
            if p:
                local_paths.append(p)

        if not local_paths:
            return

        date_part = (event.date or datetime.utcnow()).strftime("%Y/%m/%d")
        base_dest = f"{event.chat_id}/{date_part}/{event.messages[0].id}"
        uploaded = _upload_many(local_paths, base_dest)

        row = {
            "chat": chat_name,
            "chat_id": int(event.chat_id),
            "msg_id": int(event.messages[0].id),
            "text": text,
            "timestamp": (event.date or datetime.utcnow()).isoformat(),
            "username": username,
            "full_name": full_name,
            "matched": True,
            "images_count": len(uploaded),
            "photo_list": uploaded,
        }

        try:
            _insert_post_row(row)
            print(f"[ALBUM] saved id={row['msg_id']} media={row['images_count']}")
        except Exception as e:
            print(f"[ERROR] Supabase insert (album): {e}")

    # ======== ОБРАБОТЧИК ОДИНОЧНЫХ МЕДИА (фото+видео) ========

    @client.on(events.NewMessage(chats=MONITORED_CHATS, incoming=True))
    async def handle_single(event):
        # Если часть альбома — обработает handle_album
        if getattr(event.message, "grouped_id", None):
            return

        if not _has_media(event.message):
            # для чисто текстовых постов ничего не делаем
            return

        chat_name = await _chat_title(event)
        username, full_name = await _sender_meta(event)
        text = event.raw_text or ""

        media_dir = "./downloaded_media"
        os.makedirs(media_dir, exist_ok=True)

        fn = f"{event.chat_id}_{event.id}"
        local_path = await event.message.download_media(file=os.path.join(media_dir, fn))
        if not local_path:
            return

        date_part = (event.date or datetime.utcnow()).strftime("%Y/%m/%d")
        base_dest = f"{event.chat_id}/{date_part}/{event.id}"
        uploaded = _upload_many([local_path], base_dest)

        row = {
            "chat": chat_name,
            "chat_id": int(event.chat_id),
            "msg_id": int(event.id),
            "text": text,
            "timestamp": (event.date or datetime.utcnow()).isoformat(),
            "username": username,
            "full_name": full_name,
            "matched": True,
            "images_count": len(uploaded),
            "photo_list": uploaded,
        }

        try:
            _insert_post_row(row)
            print(f"[PHOTO/VIDEO] saved id={row['msg_id']} media={row['images_count']}")
        except Exception as e:
            print(f"[ERROR] Supabase insert (single): {e}")

    await client.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("👋 Завершение")
