"""Команда /secret — одноразовое сообщение через бизнес-аккаунт.

Поток:
1. Владелец бизнес-подключения пишет в ЛС с ботом любое сообщение
   (текст / фото / видео / голосовое / документ — что угодно).
2. Реплаит на это сообщение командой:
       /secret <user|id> <время>
   Примеры:
       /secret @vasya 30сек
       /secret 12345 1мин
       /secret @vasya 5мин

3. Бот через бизнес-подключение отправителя посылает получателю
   «конверт» — короткое сообщение с inline-кнопкой «📩 Открыть».
   Конверт виден как сообщение от владельца (бизнес-аккаунт).
4. Если получатель не открыл конверт за SECRET_UNOPENED_TTL секунд —
   конверт автоматически удаляется.
5. Когда получатель нажимает «Открыть»:
       • конверт удаляется,
       • в этот же чат приходит реальное содержимое
         (с protect_content=True — нельзя переслать/сохранить/скопировать),
       • запускается таймер на указанное в команде время,
         после которого содержимое тоже удаляется.
   Кнопку можно нажать только один раз — повторный тап получит
   ответ «уже прочитано».
6. Админу (ADMIN_ID) одновременно прилетает копия в обычном виде,
   без таймеров и защиты — для архива/контроля.

⚠ Состояние конвертов хранится в памяти. Если Railway перезапустит
бота — все «незакрытые» секреты пропадут (получатель уже не сможет
их открыть, но и удалить их тогда некому).
"""

import asyncio
import logging
import re
from secrets import token_urlsafe

from aiogram import Bot
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from fun import delete_command, get_display_name


# === Настройки =======================================================

# Сколько секунд конверт ждёт открытия. После — авто-удаление.
SECRET_UNOPENED_TTL: int = 60

# Допустимый диапазон таймера на чтение после открытия (секунды).
SECRET_READ_MIN: int = 1
SECRET_READ_MAX: int = 3600  # 1 час

# Длина случайного id для callback_data (Telegram лимит — 64 байта).
SECRET_ID_LEN: int = 12


# === Состояние =======================================================

# secret_id -> запись с метаданными и сохранённым содержимым.
_secrets: dict[str, dict] = {}


def _new_secret_id() -> str:
    return token_urlsafe(SECRET_ID_LEN)[:SECRET_ID_LEN]


# === Парсинг времени ================================================

_TIME_RE = re.compile(
    r"^(\d+)\s*(sec|сек|s|min|мин|m|h|ч|hour|час)?$",
    re.IGNORECASE,
)


def _parse_time(s: str) -> float | None:
    """«30сек» / «1мин» / «2ч» / «10» (по умолчанию секунды) → float секунд.
    Возвращает None при некорректном формате или выходе за лимиты.
    """
    s = s.strip().lower()
    m = _TIME_RE.match(s)
    if not m:
        return None
    n = int(m.group(1))
    unit = (m.group(2) or "сек").lower()
    if unit in ("s", "sec", "сек"):
        secs = n
    elif unit in ("m", "min", "мин"):
        secs = n * 60
    elif unit in ("h", "hour", "ч", "час"):
        secs = n * 3600
    else:
        return None
    if secs < SECRET_READ_MIN or secs > SECRET_READ_MAX:
        return None
    return float(secs)


def _format_secs(secs: float) -> str:
    s = int(secs)
    if s < 60:
        return f"{s} сек"
    if s < 3600:
        m, rem = divmod(s, 60)
        return f"{m} мин" + (f" {rem} сек" if rem else "")
    h, rem_s = divmod(s, 3600)
    rem_m = rem_s // 60
    return f"{h} ч" + (f" {rem_m} мин" if rem_m else "")


# === Извлечение содержимого =========================================

def _extract_content(msg: Message) -> dict | None:
    """Достаёт из сообщения тип + file_id/текст в плоский dict.
    Возвращает None если в сообщении нет ничего полезного."""
    cap = msg.caption or None
    if msg.text:
        return {"kind": "text", "text": msg.text}
    if msg.photo:
        return {"kind": "photo", "file_id": msg.photo[-1].file_id, "caption": cap}
    if msg.video:
        return {"kind": "video", "file_id": msg.video.file_id, "caption": cap}
    if msg.animation:
        return {"kind": "animation", "file_id": msg.animation.file_id, "caption": cap}
    if msg.voice:
        return {"kind": "voice", "file_id": msg.voice.file_id, "caption": cap}
    if msg.audio:
        return {"kind": "audio", "file_id": msg.audio.file_id, "caption": cap}
    if msg.video_note:
        return {"kind": "video_note", "file_id": msg.video_note.file_id}
    if msg.sticker:
        return {"kind": "sticker", "file_id": msg.sticker.file_id}
    if msg.document:
        return {"kind": "document", "file_id": msg.document.file_id, "caption": cap}
    return None


# === Резолв получателя ===============================================

async def _resolve_target(bot: Bot, raw: str) -> tuple[int | None, str]:
    """raw — это «@username», «username» или числовой id.
    Возвращает (user_id, display_name) или (None, текст_ошибки)."""
    raw = raw.strip()
    if not raw:
        return None, "не указан получатель"

    if raw.lstrip("-").isdigit():
        try:
            chat = await bot.get_chat(int(raw))
            name = get_display_name(chat) or str(chat.id)
            return chat.id, name
        except Exception as e:
            logging.warning(f"/secret: get_chat по id={raw}: {e}")
            return None, f"не нашёл пользователя по id {raw}"

    username = raw if raw.startswith("@") else "@" + raw
    try:
        chat = await bot.get_chat(username)
        if chat.type != "private":
            return None, f"{username} — это не личный чат"
        name = get_display_name(chat) or username
        return chat.id, name
    except Exception as e:
        logging.warning(f"/secret: get_chat по {username}: {e}")
        return None, (
            f"не нашёл пользователя {username}. "
            f"Проверь юзернейм или попробуй указать числовой id."
        )


# === Отправка содержимого через бизнес-подключение ==================

async def _send_content_via_bc(
    bot: Bot,
    chat_id: int,
    bc_id: str,
    content: dict,
    protect: bool = True,
) -> Message | None:
    """Отправляет сохранённое содержимое в чат от имени владельца
    бизнес-подключения. Возвращает Message или None при неизвестном типе."""
    kind = content["kind"]
    cap = content.get("caption")
    kw = dict(chat_id=chat_id, business_connection_id=bc_id, protect_content=protect)
    if kind == "text":
        return await bot.send_message(text=content["text"], **kw)
    if kind == "photo":
        return await bot.send_photo(photo=content["file_id"], caption=cap, **kw)
    if kind == "video":
        return await bot.send_video(video=content["file_id"], caption=cap, **kw)
    if kind == "animation":
        return await bot.send_animation(animation=content["file_id"], caption=cap, **kw)
    if kind == "voice":
        return await bot.send_voice(voice=content["file_id"], caption=cap, **kw)
    if kind == "audio":
        return await bot.send_audio(audio=content["file_id"], caption=cap, **kw)
    if kind == "document":
        return await bot.send_document(document=content["file_id"], caption=cap, **kw)
    if kind == "video_note":
        return await bot.send_video_note(video_note=content["file_id"], **kw)
    if kind == "sticker":
        return await bot.send_sticker(sticker=content["file_id"], **kw)
    return None


# === Копия для админа =================================================

async def _send_admin_copy(bot: Bot, admin_id: int, content: dict, header: str) -> None:
    """Отсылает админу копию секретного сообщения с шапкой,
    БЕЗ protect_content и без таймера."""
    kind = content["kind"]
    cap = content.get("caption")
    full_cap = header + (f"\n\n{cap}" if cap else "")
    try:
        if kind == "text":
            await bot.send_message(admin_id, f"{header}\n\n{content['text']}")
        elif kind == "photo":
            await bot.send_photo(admin_id, content["file_id"], caption=full_cap)
        elif kind == "video":
            await bot.send_video(admin_id, content["file_id"], caption=full_cap)
        elif kind == "animation":
            await bot.send_animation(admin_id, content["file_id"], caption=full_cap)
        elif kind == "voice":
            await bot.send_voice(admin_id, content["file_id"], caption=full_cap)
        elif kind == "audio":
            await bot.send_audio(admin_id, content["file_id"], caption=full_cap)
        elif kind == "document":
            await bot.send_document(admin_id, content["file_id"], caption=full_cap)
        elif kind == "video_note":
            await bot.send_message(admin_id, header)
            await bot.send_video_note(admin_id, content["file_id"])
        elif kind == "sticker":
            await bot.send_message(admin_id, header)
            await bot.send_sticker(admin_id, content["file_id"])
    except Exception as e:
        logging.error(f"/secret: не удалось отправить копию админу: {e}")


# === Удаление сообщения через бизнес-подключение ====================

async def _delete_business_msg(
    bot: Bot, bc_id: str, chat_id: int, message_id: int
) -> bool:
    """Тихо удаляет сообщение в бизнес-чате. С fallback на raw-метод
    для старых aiogram-3 (логика взята из existing /mute)."""
    try:
        await bot.delete_business_messages(
            business_connection_id=bc_id,
            message_ids=[message_id],
        )
        return True
    except AttributeError:
        try:
            from aiogram.methods import DeleteBusinessMessages
            await bot(DeleteBusinessMessages(
                business_connection_id=bc_id,
                message_ids=[message_id],
            ))
            return True
        except Exception as e:
            logging.warning(f"/secret: raw DeleteBusinessMessages: {e}")
            return False
    except Exception as e:
        logging.warning(f"/secret: delete_business_messages mid={message_id}: {e}")
        return False


# === Команда /secret =================================================

USAGE_TEXT = (
    "Использование: <code>/secret @username 30сек</code>\n\n"
    "<b>Как это работает:</b>\n"
    "1) Сначала отправляешь мне сюда сообщение, которое нужно «зашифровать» "
    "(текст, фото, видео, голосовое — что угодно).\n"
    "2) Затем <b>отвечаешь</b> на это сообщение командой:\n"
    "<code>/secret @user 30сек</code> — секретное сообщение для @user, "
    "после открытия будет видно 30 секунд.\n\n"
    "Время: <code>30сек</code>, <code>1мин</code>, <code>2ч</code> "
    f"(от {SECRET_READ_MIN} сек до {SECRET_READ_MAX // 60} мин).\n\n"
    "Если получатель не откроет конверт за минуту — он удалится сам."
)


async def cmd_secret(
    message: Message,
    bot: Bot,
    admin_id: int,
    connection_owners: dict[str, int],
    connected_users: dict[int, dict],
) -> None:
    """Обработчик /secret в ЛС с ботом. Зарегистрирован в bot.py."""
    if not message.from_user:
        return
    sender_id = message.from_user.id

    text = (message.text or "").strip()
    parts = text.split(maxsplit=2)
    if len(parts) < 3:
        await message.reply(USAGE_TEXT, parse_mode="HTML")
        return

    if not message.reply_to_message:
        await message.reply(
            "❗ Команду <code>/secret</code> нужно отправить как <b>ответ</b> "
            "на сообщение, которое надо «зашифровать».\n\n" + USAGE_TEXT,
            parse_mode="HTML",
        )
        return

    target_raw = parts[1]
    time_raw = parts[2]

    read_time = _parse_time(time_raw)
    if read_time is None:
        await message.reply(
            f"❗ Не понимаю время «{time_raw}». Примеры: "
            "<code>30сек</code>, <code>1мин</code>, <code>2ч</code>. "
            f"Допустимо: от {SECRET_READ_MIN} сек до "
            f"{SECRET_READ_MAX // 60} мин.",
            parse_mode="HTML",
        )
        return

    # Проверяем, что отправитель — владелец бизнес-подключения.
    bc_id: str | None = None
    for bid, oid in connection_owners.items():
        if oid == sender_id:
            bc_id = bid
            break
    if not bc_id:
        await message.reply(
            "❗ Чтобы отправить секретное сообщение, у тебя должен быть "
            "подключён этот бот в Telegram → Настройки → Бизнес → Чат-боты. "
            "Без бизнес-подключения я не могу написать от твоего имени."
        )
        return

    # Извлекаем содержимое из реплая.
    content = _extract_content(message.reply_to_message)
    if not content:
        await message.reply(
            "❗ В сообщении, на которое ты ответил, нечего шифровать. "
            "Поддерживаются: текст, фото, видео, гифки, голосовые, "
            "аудио, документы, кружки, стикеры."
        )
        return

    # Резолвим получателя.
    target_id, target_name = await _resolve_target(bot, target_raw)
    if target_id is None:
        await message.reply(f"❗ {target_name}", parse_mode="HTML")
        return

    if target_id == sender_id:
        await message.reply("❗ Нельзя отправить секретное сообщение самому себе.")
        return

    # Имя отправителя для шапки конверта.
    info = connected_users.get(sender_id) or {}
    sender_name = info.get("name") or get_display_name(message.from_user)

    # Создаём id и заводим запись.
    sid = _new_secret_id()
    while sid in _secrets:
        sid = _new_secret_id()

    wrapper_text = (
        f"🔒 <b>Секретное сообщение от {sender_name}</b>\n\n"
        f"Нажми «Открыть», чтобы прочитать. После открытия "
        f"будет видно <b>{_format_secs(read_time)}</b>, потом удалится. "
        f"Открыть можно только один раз."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="📩 Открыть", callback_data=f"secret:{sid}"),
    ]])

    try:
        wrapper = await bot.send_message(
            chat_id=target_id,
            text=wrapper_text,
            parse_mode="HTML",
            reply_markup=kb,
            business_connection_id=bc_id,
        )
    except Exception as e:
        logging.error(f"/secret: не удалось отправить конверт: {e}")
        await message.reply(
            f"❗ Не удалось отправить сообщение получателю: <code>{e}</code>\n\n"
            "Возможно, ты не можешь ему писать (заблокирован, "
            "приватность, или в подключении бота отключено право отвечать).",
            parse_mode="HTML",
        )
        return

    entry = {
        "owner_id": sender_id,
        "bc_id": bc_id,
        "target_id": target_id,
        "target_name": target_name,
        "sender_name": sender_name,
        "wrapper_msg_id": wrapper.message_id,
        "content": content,
        "read_time": read_time,
        "opened": False,
        "expire_task": None,
        "delete_task": None,
        "admin_id": admin_id,
    }
    _secrets[sid] = entry

    # Авто-удаление неоткрытого конверта через TTL.
    entry["expire_task"] = asyncio.create_task(_expire_unopened(sid, bot))

    # Копия админу (полная, без таймера и защиты).
    admin_header = (
        f"🔒 <b>Секрет (копия для архива)</b>\n"
        f"👤 От: {sender_name} [<code>{sender_id}</code>]\n"
        f"🎯 Кому: {target_name} [<code>{target_id}</code>]\n"
        f"⏱ Таймер на чтение: {_format_secs(read_time)}"
    )
    asyncio.create_task(_send_admin_copy(bot, admin_id, content, admin_header))

    # Удаляем команду из ЛС, чтобы не оставалась.
    try:
        await delete_command(message, bot)
    except Exception:
        pass

    # Подтверждение отправителю.
    try:
        await bot.send_message(
            sender_id,
            f"✅ Секретное сообщение отправлено <b>{target_name}</b>.\n"
            f"⏱ На чтение после открытия: <b>{_format_secs(read_time)}</b>.\n"
            f"Если не откроет за {SECRET_UNOPENED_TTL} сек — удалю сам.",
            parse_mode="HTML",
        )
    except Exception:
        pass


async def _expire_unopened(sid: str, bot: Bot) -> None:
    """Через SECRET_UNOPENED_TTL сек, если конверт ещё не открыт — удаляем его."""
    try:
        await asyncio.sleep(SECRET_UNOPENED_TTL)
    except asyncio.CancelledError:
        return
    entry = _secrets.get(sid)
    if not entry or entry.get("opened"):
        return
    await _delete_business_msg(
        bot, entry["bc_id"], entry["target_id"], entry["wrapper_msg_id"],
    )
    _secrets.pop(sid, None)
    try:
        await bot.send_message(
            entry["owner_id"],
            f"⏱ Секрет для <b>{entry['target_name']}</b> не был открыт за "
            f"{SECRET_UNOPENED_TTL} сек — я его удалил.",
            parse_mode="HTML",
        )
    except Exception:
        pass


# === Callback на кнопку «Открыть» ====================================

async def handle_secret_callback(callback: CallbackQuery, bot: Bot) -> None:
    data = callback.data or ""
    if not data.startswith("secret:"):
        return
    sid = data.split(":", 1)[1]
    entry = _secrets.get(sid)

    if not entry:
        await callback.answer(
            "🚫 Это сообщение больше недоступно.", show_alert=True,
        )
        return
    if entry.get("opened"):
        await callback.answer(
            "🚫 Это сообщение уже было прочитано.", show_alert=True,
        )
        return
    # Открыть может только сам получатель.
    if not callback.from_user or callback.from_user.id != entry["target_id"]:
        await callback.answer(
            "🚫 Это сообщение не для тебя.", show_alert=True,
        )
        return

    # Помечаем открытым ДО любых awaitов — чтобы повторный быстрый клик
    # не успел провалиться сюда же.
    entry["opened"] = True
    if entry.get("expire_task"):
        entry["expire_task"].cancel()

    # Удаляем конверт.
    await _delete_business_msg(
        bot, entry["bc_id"], entry["target_id"], entry["wrapper_msg_id"],
    )

    # Отправляем настоящее содержимое с защитой.
    sent: Message | None = None
    try:
        sent = await _send_content_via_bc(
            bot, entry["target_id"], entry["bc_id"], entry["content"],
            protect=True,
        )
    except Exception as e:
        logging.error(f"/secret: не удалось показать содержимое: {e}")

    if not sent:
        await callback.answer(
            "Не удалось показать сообщение.", show_alert=True,
        )
        _secrets.pop(sid, None)
        # Сообщить отправителю.
        try:
            await bot.send_message(
                entry["owner_id"],
                f"⚠ Не удалось показать секрет получателю "
                f"<b>{entry['target_name']}</b> при открытии.",
                parse_mode="HTML",
            )
        except Exception:
            pass
        return

    sent_id = sent.message_id
    read_time = entry["read_time"]

    await callback.answer(
        f"Сообщение исчезнет через {_format_secs(read_time)}.",
    )

    async def _delete_after() -> None:
        try:
            await asyncio.sleep(read_time)
            await _delete_business_msg(
                bot, entry["bc_id"], entry["target_id"], sent_id,
            )
        except asyncio.CancelledError:
            pass
        finally:
            _secrets.pop(sid, None)

    entry["delete_task"] = asyncio.create_task(_delete_after())

    # Уведомление отправителю.
    try:
        await bot.send_message(
            entry["owner_id"],
            f"👁 <b>{entry['target_name']}</b> открыл твой секрет. "
            f"Исчезнет у него через {_format_secs(read_time)}.",
            parse_mode="HTML",
        )
    except Exception:
        pass
