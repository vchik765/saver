"""
Telegram Userbot — Сборщик юзернеймов (24/7)

Режимы работы:
  • Автоматически — каждый день в RUN_HOUR:00 UTC собирает MAX_PER_DAY юзернеймов
  • Вручную — напиши .собрать в любой группе, соберёт 100 уников оттуда
  Дубликаты исключаются всегда — и авто, и ручной режим работают с одной базой.
"""

import asyncio
import json
import logging
import os
from datetime import date, datetime
from pathlib import Path

from pyrogram import Client, filters
from pyrogram.errors import FloodWait, ChatAdminRequired
from pyrogram.types import Message

logging.basicConfig(level=logging.INFO, format="%(asctime)s [collector] %(message)s")
log = logging.getLogger("collector")

# ══════════════════════════════════════════════════════════════
#  НАСТРОЙКИ
# ══════════════════════════════════════════════════════════════

API_ID         = 2040
API_HASH       = "b18441a1ff607e10a989891a5462e627"
SESSION_STRING = os.environ.get("SESSION_STRING", "")

RUN_HOUR             = 12    # час автозапуска (UTC). 12 = 15:00 мск
MAX_PER_DAY          = 200   # лимит автосбора в день
MANUAL_LIMIT         = 100   # сколько собирает .собрать за раз
BATCH_SIZE           = 50    # юзернеймов в одном сообщении в Избранное
DELAY_BETWEEN_GROUPS = 8     # сек между группами (автосбор)
DELAY_BETWEEN_CHUNKS = 2     # сек между запросами участников
MAX_PER_GROUP        = 500   # лимит участников из одной группы (автосбор)

STATE_FILE = "collector_state.json"

# Глобальный стейт и лок — чтобы авто и ручной режим не конфликтовали
_state_lock = asyncio.Lock()

# ══════════════════════════════════════════════════════════════
#  Хранение прогресса
# ══════════════════════════════════════════════════════════════

def load_state() -> dict:
    if Path(STATE_FILE).exists():
        try:
            return json.loads(Path(STATE_FILE).read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"collected": [], "daily": {}, "scanned_groups": []}

def save_state(state: dict):
    Path(STATE_FILE).write_text(
        json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8"
    )

def today_str() -> str:
    return date.today().isoformat()

def sent_today(state: dict) -> int:
    return state["daily"].get(today_str(), 0)

# ══════════════════════════════════════════════════════════════
#  Сборка юзернеймов из одной группы
# ══════════════════════════════════════════════════════════════

async def collect_from_group(
    app: Client,
    chat_id,
    existing: set,
    limit: int = MAX_PER_GROUP,
) -> list:
    """Собирает уникальные юзернеймы из группы. existing — множество уже известных (lowercase)."""
    found = []
    try:
        count = 0
        async for member in app.get_chat_members(chat_id):
            if len(found) >= limit:
                break
            user = member.user
            if user and user.username and not user.is_bot:
                uname = user.username.lower()
                if uname not in existing:
                    found.append(user.username)
                    existing.add(uname)
            count += 1
            if count % 100 == 0:
                await asyncio.sleep(DELAY_BETWEEN_CHUNKS)
    except FloodWait as e:
        log.warning(f"FloodWait {e.value}с...")
        await asyncio.sleep(e.value + 5)
    except ChatAdminRequired:
        log.info("Нет прав смотреть участников")
    except Exception as e:
        log.warning(f"Ошибка сбора: {e}")
    return found

# ══════════════════════════════════════════════════════════════
#  Отправка в Избранное
# ══════════════════════════════════════════════════════════════

async def send_to_saved(app: Client, usernames: list, label: str = ""):
    prefix = f" [{label}]" if label else ""
    for i in range(0, len(usernames), BATCH_SIZE):
        chunk = usernames[i:i + BATCH_SIZE]
        text  = (
            f"📋 Юзернеймы{prefix} [{i+1}–{i+len(chunk)}] ({today_str()}):\n\n"
            + "\n".join(f"@{u}" for u in chunk)
        )
        try:
            await app.send_message("me", text)
            log.info(f"Отправлено в Избранное: {len(chunk)} юзернеймов")
        except FloodWait as e:
            await asyncio.sleep(e.value + 3)
            await app.send_message("me", text)
        await asyncio.sleep(3)

# ══════════════════════════════════════════════════════════════
#  РУЧНОЙ РЕЖИМ: .собрать
# ══════════════════════════════════════════════════════════════

def setup_handlers(app: Client):
    """Регистрирует обработчик команды .собрать."""

    @app.on_message(
        filters.regex(r"^\.собрать$", flags=0) & filters.group & filters.me
    )
    async def handle_manual_collect(client: Client, message: Message):
        chat_id    = message.chat.id
        chat_title = message.chat.title or str(chat_id)

        log.info(f"Команда .собрать в группе: {chat_title}")

        # Удаляем своё сообщение с командой чтобы не светиться
        try:
            await message.delete()
        except Exception:
            pass

        # Статус в Избранное
        status_msg = await client.send_message(
            "me",
            f"⏳ Собираю юзернеймы из «{chat_title}»..."
        )

        async with _state_lock:
            state = load_state()
            # Берём ВСЕ уже известные юзернеймы — и авто, и ручные
            existing = set(u.lower() for u in state["collected"])

            found = await collect_from_group(
                client, chat_id, existing, limit=MANUAL_LIMIT
            )

            if found:
                # Сохраняем в общую базу
                state["collected"].extend(found)
                save_state(state)

                # Отправляем в Избранное
                await send_to_saved(client, found, label=chat_title[:20])

                await client.edit_message_text(
                    "me",
                    status_msg.id,
                    f"✅ Собрано из «{chat_title}»: {len(found)} новых юзернеймов\n"
                    f"Всего в базе: {len(state['collected'])}"
                )
            else:
                await client.edit_message_text(
                    "me",
                    status_msg.id,
                    f"😕 В «{chat_title}» новых юзернеймов нет\n"
                    f"(все уже есть в базе или нет прав)"
                )

        log.info(f".собрать завершено: {len(found)} новых из {chat_title}")

# ══════════════════════════════════════════════════════════════
#  АВТО РЕЖИМ: ежедневный сбор
# ══════════════════════════════════════════════════════════════

async def run_once(app: Client):
    async with _state_lock:
        state     = load_state()
        collected = set(u.lower() for u in state["collected"])
        scanned   = set(state.get("scanned_groups", []))
        new_today = []
        remaining = MAX_PER_DAY - sent_today(state)

        log.info(f"Автосбор: всего в базе {len(state['collected'])} | осталось сегодня: {remaining}")

        if remaining <= 0:
            log.info("Дневной лимит уже выполнен.")
            return

        groups = []
        async for dialog in app.get_dialogs():
            chat = dialog.chat
            if chat.type.name in ("GROUP", "SUPERGROUP"):
                groups.append(chat)

        log.info(f"Групп найдено: {len(groups)}")

        for chat in groups:
            if len(new_today) >= remaining:
                log.info(f"Дневной лимит {MAX_PER_DAY} достигнут.")
                break

            gid = str(chat.id)
            if gid in scanned:
                continue

            log.info(f"Обхожу: {chat.title[:40]} ...")
            found = await collect_from_group(app, chat.id, collected)

            space = remaining - len(new_today)
            found = found[:space]

            if found:
                new_today.extend(found)
                log.info(f"+{len(found)} (итого: {len(new_today)})")

            scanned.add(gid)
            await asyncio.sleep(DELAY_BETWEEN_GROUPS)

        if new_today:
            log.info(f"Отправляю {len(new_today)} в Избранное...")
            await send_to_saved(app, new_today, label="авто")
            state["collected"].extend(new_today)
            state["daily"][today_str()] = sent_today(state) + len(new_today)

        state["scanned_groups"] = list(scanned)
        save_state(state)
        log.info(f"Автосбор завершён. Сегодня: {len(new_today)} | всего: {len(state['collected'])}")

# ══════════════════════════════════════════════════════════════
#  Планировщик
# ══════════════════════════════════════════════════════════════

async def scheduler(app: Client):
    log.info(f"Планировщик: автосбор каждый день в {RUN_HOUR}:00 UTC ({RUN_HOUR+3}:00 мск)")
    last_run_date = None

    while True:
        now = datetime.utcnow()
        if now.hour == RUN_HOUR and last_run_date != now.date():
            log.info("Запускаю ежедневный автосбор...")
            try:
                await run_once(app)
            except Exception as e:
                log.exception(f"Ошибка автосбора: {e}")
            last_run_date = now.date()
        await asyncio.sleep(60)

# ══════════════════════════════════════════════════════════════
#  Точка входа
# ══════════════════════════════════════════════════════════════

async def main():
    if not SESSION_STRING:
        log.error("SESSION_STRING не задан в Railway Variables!")
        return

    log.info("Подключаюсь к Telegram...")
    async with Client("userbot", API_ID, API_HASH, session_string=SESSION_STRING) as app:
        setup_handlers(app)
        log.info("✅ Подключено! Слушаю .собрать и жду время автосбора...")
        await scheduler(app)

if __name__ == "__main__":
    asyncio.run(main())
