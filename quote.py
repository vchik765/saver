import asyncio
import base64
import logging
from io import BytesIO
from typing import Callable, Optional

import aiohttp
from aiogram import Bot
from aiogram.types import Message, BufferedInputFile

from fun import delete_command

QUOTLY_API = "https://bot.lyo.su/quote/generate"
MAX_QUOTE_MESSAGES = 10
MAX_TEXT_LEN = 1500
HTTP_TIMEOUT = 30
MAX_MEDIA_BYTES = 5 * 1024 * 1024


def parse_q_args(text: str) -> tuple[bool, int]:
    """Возвращает (with_reply_context, count).

    Поддерживаются ОБА формата:
        /q            — один стикер
        /q r          — с reply-контекстом
        /q 5          — 5 сообщений
        /q r 5        — 5 сообщений + reply-контекст
        /q 5 r        — то же
        /qr           — без пробела
        /q5           — без пробела
        /qr5 /q5r     — без пробелов в любом порядке
    """
    if not text:
        return False, 1

    tokens: list[str] = []
    parts = text.split()
    if not parts:
        return False, 1

    # Первый токен — сама команда, может содержать слитные модификаторы:
    # "/qr5", "/q5r", "/qr", "/q5", "/q@bot 3" и т. п.
    first = parts[0].lstrip("/").split("@")[0].lower()
    # Отрезаем ведущее "q"
    if first.startswith("q"):
        tail = first[1:]
        if tail:
            tokens.append(tail)
    # Остальные части как есть
    tokens.extend(p.lower() for p in parts[1:])

    with_reply = False
    count = 1

    import re
    for tok in tokens:
        # В одном токене может быть и "r", и число: "r5", "5r", "5"
        # Извлекаем цифры и наличие буквы "r" отдельно.
        if "r" in tok:
            with_reply = True
        digits = re.findall(r"\d+", tok)
        for d in digits:
            try:
                n = int(d)
                if n >= 1:
                    count = min(n, MAX_QUOTE_MESSAGES)
            except ValueError:
                pass

    return with_reply, count


async def _download_to_b64(bot: Bot, file_id: str, max_bytes: int = MAX_MEDIA_BYTES) -> Optional[str]:
    try:
        file = await bot.get_file(file_id)
        if file.file_size and file.file_size > max_bytes:
            return None
        buf = BytesIO()
        await bot.download_file(file.file_path, destination=buf)
        data = buf.getvalue()
        if len(data) > max_bytes:
            return None
        return base64.b64encode(data).decode()
    except Exception as e:
        logging.warning(f"Не удалось скачать файл {file_id}: {e}")
        return None


async def _get_avatar_data_url(bot: Bot, user_id: int) -> Optional[str]:
    try:
        photos = await bot.get_user_profile_photos(user_id, limit=1)
        if not photos.total_count or not photos.photos:
            return None
        ph = photos.photos[0][-1]
        b64 = await _download_to_b64(bot, ph.file_id, max_bytes=2 * 1024 * 1024)
        if not b64:
            return None
        return f"data:image/jpeg;base64,{b64}"
    except Exception as e:
        logging.warning(f"Не удалось получить аватар {user_id}: {e}")
        return None


def _user_payload(user) -> dict:
    if not user:
        return {"id": 0, "first_name": "Unknown", "name": "Unknown"}
    full = (user.full_name or "").strip()
    if full:
        display = full
        first = user.first_name or full
        last = user.last_name or ""
    elif user.username:
        display = f"@{user.username}"
        first = display
        last = ""
    else:
        display = "Unknown"
        first = "Unknown"
        last = ""
    return {
        "id": user.id,
        "first_name": first,
        "last_name": last,
        "username": user.username or "",
        "name": display,
    }


def _entities_payload(entities) -> list:
    if not entities:
        return []
    out = []
    for e in entities:
        item = {"type": e.type, "offset": e.offset, "length": e.length}
        for k in ("url", "language", "custom_emoji_id"):
            v = getattr(e, k, None)
            if v:
                item[k] = v
        u = getattr(e, "user", None)
        if u:
            item["user"] = {"id": u.id}
        out.append(item)
    return out


def _media_label(msg: Message) -> str:
    if msg.photo: return "📷 Фото"
    if msg.video: return "📹 Видео"
    if msg.voice: return "🎤 Голосовое"
    if msg.audio: return "🎵 Аудио"
    if msg.video_note: return "⭕ Кружок"
    if msg.animation: return "🎞 GIF"
    if msg.sticker: return "🎭 Стикер"
    if msg.document: return "📎 Документ"
    if msg.contact: return "📞 Контакт"
    if msg.location: return "📍 Локация"
    return ""


async def _build_message_payload(bot: Bot, msg: Message, with_reply: bool = False) -> dict:
    text = msg.text or msg.caption or ""
    if len(text) > MAX_TEXT_LEN:
        text = text[: MAX_TEXT_LEN - 1] + "…"

    user = msg.from_user
    from_payload = _user_payload(user)

    payload: dict = {
        "from": from_payload,
        "text": text,
        "entities": _entities_payload(msg.entities or msg.caption_entities),
        "avatar": True,
    }

    if with_reply:
        rep = msg.reply_to_message
        logging.info(
            f"[/q DEBUG reply] with_reply=True, base_mid={msg.message_id}, "
            f"has_reply_to_message={rep is not None}"
        )
        if rep:
            rep_text = rep.text or rep.caption or _media_label(rep) or ""
            if len(rep_text) > 200:
                rep_text = rep_text[:199] + "…"
            rep_user = _user_payload(rep.from_user)
            payload["replyMessage"] = {
                "name": rep_user.get("name", ""),
                "text": rep_text,
                "chatId": rep.from_user.id if rep.from_user else 1,
                "userId": rep.from_user.id if rep.from_user else 1,
                "entities": _entities_payload(rep.entities or rep.caption_entities),
            }
            logging.info(f"[/q DEBUG reply] sending replyMessage: {payload['replyMessage']}")

    media_file_id = None
    media_mime = "image/jpeg"
    if msg.photo:
        media_file_id = msg.photo[-1].file_id
    elif msg.sticker:
        if msg.sticker.thumbnail:
            media_file_id = msg.sticker.thumbnail.file_id
        else:
            media_file_id = msg.sticker.file_id
            media_mime = "image/webp"
    elif msg.video and msg.video.thumbnail:
        media_file_id = msg.video.thumbnail.file_id
    elif msg.video_note and msg.video_note.thumbnail:
        media_file_id = msg.video_note.thumbnail.file_id
    elif msg.animation and msg.animation.thumbnail:
        media_file_id = msg.animation.thumbnail.file_id
    elif msg.document and msg.document.thumbnail:
        media_file_id = msg.document.thumbnail.file_id

    if media_file_id:
        b64 = await _download_to_b64(bot, media_file_id)
        if b64:
            payload["media"] = [{"url": f"data:{media_mime};base64,{b64}"}]
            payload["mediaType"] = "sticker" if msg.sticker else "photo"

    if not text:
        label = _media_label(msg)
        if label:
            payload["text"] = label

    return payload


async def _render_quote(bot: Bot, messages: list[Message], with_reply: bool) -> Optional[bytes]:
    msg_payloads = []
    for idx, m in enumerate(messages):
        try:
            # reply-контекст показываем только у первого сообщения цитаты
            msg_payloads.append(await _build_message_payload(bot, m, with_reply=(with_reply and idx == 0)))
        except Exception as e:
            logging.warning(f"Ошибка построения payload цитаты: {e}")

    if not msg_payloads:
        return None

    # Чат-стиль: сообщения по левому краю, аватарки рядом с каждым.
    # Не задаём width/height — Quotly сам подберёт под содержимое и
    # верстает в формате группового чата (как в референс-боте).
    body = {
        "type": "quote",
        "format": "webp",
        "backgroundColor": "//#292232",
        "width": 384,
        "scale": 2,
        "messages": msg_payloads,
    }

    try:
        timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.post(QUOTLY_API, json=body) as r:
                if r.status != 200:
                    body_text = await r.text()
                    logging.error(f"Quotly статус {r.status}: {body_text[:300]}")
                    return None
                data = await r.json()
                if not data.get("ok"):
                    logging.error(f"Quotly не ok: {str(data)[:300]}")
                    return None
                img_b64 = data["result"]["image"]
                return base64.b64decode(img_b64)
    except Exception as e:
        logging.error(f"Ошибка вызова Quotly API: {e}")
        return None


async def _send_temp_error(bot: Bot, chat_id: int, text: str, bc_id: str, delay: float = 4.0):
    try:
        sent = await bot.send_message(chat_id, text, business_connection_id=bc_id)
        await asyncio.sleep(delay)
        try:
            await bot.delete_messages(
                chat_id=chat_id,
                message_ids=[sent.message_id],
                business_connection_id=bc_id,
            )
        except Exception:
            pass
    except Exception as e:
        logging.warning(f"Ошибка временного сообщения /q: {e}")


async def cmd_quote(message: Message, bot: Bot, cache_lookup: Callable[[int, int], Optional[Message]]):
    bc_id = message.business_connection_id
    chat_id = message.chat.id

    if not message.reply_to_message:
        await delete_command(message, bot)
        await _send_temp_error(bot, chat_id, "❌ Ответьте командой /q на сообщение.", bc_id)
        return

    with_reply, count = parse_q_args(message.text or "")
    logging.info(f"[/q DEBUG] raw_text={message.text!r}  parsed: r={with_reply} count={count}")

    base_msg = message.reply_to_message

    # В Business-апдейтах reply_to_message часто приходит "обрезанным" —
    # без собственного reply_to_message. Если у нас в кэше есть это же
    # сообщение полностью — берём оттуда (там вся reply-цепочка).
    try:
        import sys
        _bot_mod = sys.modules.get("__main__")
        if _bot_mod is None or not hasattr(_bot_mod, "cache"):
            import bot as _bot_mod
        cached_pair = _bot_mod.cache.get(chat_id, {}).get(base_msg.message_id)
        if cached_pair:
            cached_base = cached_pair[0]
            if cached_base.reply_to_message and not base_msg.reply_to_message:
                logging.info("[/q] base_msg обогащён из кэша (есть reply_to_message)")
                base_msg = cached_base
    except Exception as e:
        logging.warning(f"Не удалось обогатить base_msg из кэша: {e}")

    messages_to_quote: list[Message] = [base_msg]

    # /q N: основной режим — base_msg + следующие (N-1) из кэша.
    # Если после base_msg в кэше меньше чем нужно — добираем недостающие
    # из сообщений ДО base_msg. Итог всегда в хронологическом порядке.
    if count > 1:
        try:
            # ВАЖНО: бот запускается как `python bot.py` — модуль грузится как
            # __main__. Если делать `import bot`, Python загружает ВТОРУЮ копию
            # модуля с отдельными глобалами, и кэш будет пустой. Берём именно
            # тот модуль, где живут хендлеры.
            import sys
            _bot_mod = sys.modules.get("__main__")
            if _bot_mod is None or not hasattr(_bot_mod, "cache"):
                import bot as _bot_mod  # fallback на случай иного запуска
            chat_cache = _bot_mod.cache.get(chat_id, {})
            logging.info(
                f"[/q DEBUG] cmd_chat_id={chat_id} base_chat_id={base_msg.chat.id} "
                f"base_mid={base_msg.message_id} all_cache_chats={list(_bot_mod.cache.keys())} "
                f"this_chat_cache_size={len(chat_cache)} "
                f"sample_ids={sorted(chat_cache.keys())[:10]}"
            )
            need = count - 1

            # Берём ТОЛЬКО следующие сообщения (вперёд от base_msg).
            # Если в кэше после base_msg меньше чем need — собираем сколько есть,
            # вверх не лезем.
            next_ids = sorted(mid for mid in chat_cache.keys() if mid > base_msg.message_id)
            forward: list[Message] = []
            for mid in next_ids[:need]:
                pair = chat_cache.get(mid)
                if pair:
                    forward.append(pair[0])

            messages_to_quote = [base_msg] + forward
            logging.info(
                f"/q {count}: собрано {len(messages_to_quote)} "
                f"(вперёд {len(forward)}, кэш чата {chat_id}: {len(chat_cache)} сообщений)"
            )
        except Exception as e:
            logging.warning(f"Не удалось добрать сообщения для /q {count}: {e}")

    await delete_command(message, bot)

    img_bytes = await _render_quote(bot, messages_to_quote, with_reply)

    if not img_bytes:
        await _send_temp_error(bot, chat_id, "❌ Не удалось создать цитату. Попробуйте позже.", bc_id)
        return

    reply_to = base_msg.message_id
    sticker_file = BufferedInputFile(img_bytes, filename="quote.webp")
    try:
        await bot.send_sticker(
            chat_id,
            sticker_file,
            business_connection_id=bc_id,
            reply_to_message_id=reply_to,
        )
    except Exception as e:
        logging.warning(f"send_sticker не прошёл, fallback на send_document: {e}")
        try:
            doc_file = BufferedInputFile(img_bytes, filename="quote.webp")
            await bot.send_document(
                chat_id,
                doc_file,
                business_connection_id=bc_id,
                reply_to_message_id=reply_to,
            )
        except Exception as e2:
            logging.error(f"Ошибка отправки цитаты: {e2}")
            await _send_temp_error(bot, chat_id, "❌ Не удалось отправить цитату.", bc_id)
