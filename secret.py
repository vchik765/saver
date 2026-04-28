"""Команда /secret — одноразовое секретное сообщение через бизнес-аккаунт.

Поток:
1. Владелец бизнес-подключения пишет в ЛС с ботом любой текст.
2. Реплаит на это сообщение командой:
       /secret <user|id>
   Примеры:
       /secret @vasya
       /secret 12345

3. Бот через бизнес-подключение отправителя посылает получателю
   «конверт» — короткое сообщение с inline-кнопкой «📩 Открыть».
   Конверт виден как сообщение от владельца (бизнес-аккаунт).
4. Если получатель не открыл конверт за SECRET_UNOPENED_TTL секунд —
   конверт автоматически удаляется.
5. Когда получатель нажимает «Открыть»:
       • Telegram показывает у него нативное всплывающее окно
         (alert с кнопкой «OK») с текстом секретного сообщения,
       • конверт сразу удаляется.
   После того как получатель нажмёт «OK» — окно закроется и
   текст исчезнет навсегда. Восстановить или открыть второй раз
   нельзя — кнопка одноразовая.

   ⚠ Из-за того что используется alert, поддерживается только
   текст (не медиа). Лимит Telegram на текст алерта — 200 символов.

6. Админу (ADMIN_ID) одновременно прилетает копия текста с шапкой
   (от кого, кому) — для архива/контроля.

⚠ Состояние конвертов хранится в памяти. Если Railway перезапустит
бота — все «незакрытые» секреты пропадут (получатель уже не сможет
их открыть, а сам конверт удалить тогда некому).
"""

import asyncio
import logging
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

# Лимит Telegram на текст в callback-alert.
SECRET_TEXT_MAX: int = 200

# Длина случайного id для callback_data (Telegram лимит — 64 байта).
SECRET_ID_LEN: int = 12


# === Состояние =======================================================

# secret_id -> запись с метаданными и сохранённым текстом.
_secrets: dict[str, dict] = {}


def _new_secret_id() -> str:
    return token_urlsafe(SECRET_ID_LEN)[:SECRET_ID_LEN]


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
    "Использование: <code>/secret @username</code>\n\n"
    "<b>Как это работает:</b>\n"
    "1) Сначала отправляешь мне сюда <b>текст</b>, который надо «зашифровать» "
    f"(до {SECRET_TEXT_MAX} символов).\n"
    "2) Затем <b>отвечаешь</b> на этот текст командой:\n"
    "<code>/secret @user</code> — секретка для @user.\n\n"
    "Получатель увидит у себя в чате с тобой «конверт» с кнопкой «Открыть». "
    "При нажатии у него появится всплывающее окно с твоим текстом и кнопкой "
    "«OK». Как только он нажмёт «OK» — окно закрывается, текст исчезает "
    "навсегда. Открыть второй раз нельзя.\n\n"
    "Если конверт не открыт за минуту — я удалю его сам.\n\n"
    "⚠ Поддерживается только текст. Медиа в этом формате не работает."
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
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(USAGE_TEXT, parse_mode="HTML")
        return

    if not message.reply_to_message:
        await message.reply(
            "❗ Команду <code>/secret</code> нужно отправить как <b>ответ</b> "
            "на сообщение, которое надо «зашифровать».\n\n" + USAGE_TEXT,
            parse_mode="HTML",
        )
        return

    target_raw = parts[1].strip()

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

    # Из реплая берём только текст.
    secret_text = (
        message.reply_to_message.text
        or message.reply_to_message.caption
        or ""
    ).strip()
    if not secret_text:
        await message.reply(
            "❗ В этом формате поддерживается только <b>текст</b>. "
            "Ответь командой на текстовое сообщение.",
            parse_mode="HTML",
        )
        return
    if len(secret_text) > SECRET_TEXT_MAX:
        await message.reply(
            f"❗ Слишком длинный текст: {len(secret_text)} символов. "
            f"Лимит Telegram на всплывающее окно — {SECRET_TEXT_MAX}. "
            f"Сократи и попробуй ещё раз.",
            parse_mode="HTML",
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
        f"Нажми «Открыть» — текст появится во всплывающем окне. "
        f"Как только закроешь окно — сообщение исчезнет навсегда. "
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
        "secret_text": secret_text,
        "opened": False,
        "expire_task": None,
    }
    _secrets[sid] = entry

    # Авто-удаление неоткрытого конверта через TTL.
    entry["expire_task"] = asyncio.create_task(_expire_unopened(sid, bot))

    # Копия админу (полная, без защиты).
    admin_copy = (
        f"🔒 <b>Секрет (копия для архива)</b>\n"
        f"👤 От: {sender_name} [<code>{sender_id}</code>]\n"
        f"🎯 Кому: {target_name} [<code>{target_id}</code>]\n\n"
        f"{secret_text}"
    )
    try:
        await bot.send_message(admin_id, admin_copy, parse_mode="HTML")
    except Exception as e:
        logging.error(f"/secret: не удалось отправить копию админу: {e}")

    # Удаляем команду из ЛС.
    try:
        await delete_command(message, bot)
    except Exception:
        pass

    # Подтверждение отправителю.
    try:
        await bot.send_message(
            sender_id,
            f"✅ Секретное сообщение отправлено <b>{target_name}</b>.\n"
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

    # Помечаем открытым ДО любых других awaitов — чтобы повторный
    # быстрый клик не успел провалиться сюда же.
    entry["opened"] = True
    if entry.get("expire_task"):
        entry["expire_task"].cancel()

    secret_text = entry["secret_text"]

    # Главное действие: показываем нативный alert у получателя.
    try:
        await callback.answer(text=secret_text, show_alert=True)
    except Exception as e:
        logging.error(f"/secret: callback.answer alert упал: {e}")
        # Откатываем флажок, чтобы получатель мог попробовать ещё раз.
        entry["opened"] = False
        entry["expire_task"] = asyncio.create_task(_expire_unopened(sid, bot))
        return

    # Удаляем конверт — его задача выполнена.
    await _delete_business_msg(
        bot, entry["bc_id"], entry["target_id"], entry["wrapper_msg_id"],
    )
    _secrets.pop(sid, None)

    # Уведомление отправителю.
    try:
        await bot.send_message(
            entry["owner_id"],
            f"👁 <b>{entry['target_name']}</b> открыл и прочитал твой секрет.",
            parse_mode="HTML",
        )
    except Exception:
        pass
