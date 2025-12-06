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
from telethon.sessions import StringSession
from telethon.tl.types import User
from supabase import create_client, Client

load_dotenv()

# Telegram API
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
SESSION_NAME = os.getenv("SESSION_NAME", "collector")

# Что мониторим (через запятую): @chan1,@chan2,-1001234567890
MONITORED_CHATS_ENV = os.getenv("MONITORED_CHATS", "https://t.me/replicadesignerbags")


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
        # Берём расширение из локального файла (.jpg, .png, .mp4 и т.д.)
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

    # Видео (shortcut-поле)
    if getattr(msg, "video", None):
        return True

    # Видео как документ с MIME-типом video/*
