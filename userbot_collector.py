"""
Telegram Userbot — Сборщик юзернеймов (24/7)

Три способа сбора:
  1. АВТО      — каждый день в RUN_HOUR:00 UTC обходит список участников групп
  2. ПАССИВНЫЙ — постоянно следит за сообщениями во всех группах,
                 при каждом новом сообщении добавляет автора в базу
  3. РУЧНОЙ    — напиши .собрать в группе → соберёт 100 уников оттуда

Все три способа работают с одной базой — дублей не будет.
Пассивный сбор накапливает юзернеймы и раз в PASSIVE_FLUSH_EVERY штук
отправляет пачку в Избранное.
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

RUN_HOUR             = 12    # час автосбора (UTC). 12 = 15:00 мск
MAX_PER_DAY          = 200   # лимит автосбора в день
MANUAL_LIMIT         = 100   # сколько собирает .собрать за раз
BATCH_SIZE           = 50    # юзернеймов в одном сообщении в Избранное
DELAY_BETWEEN_GROUPS = 8     # сек между группами (автосбор)
DELAY_BETWEEN_CHUNKS = 2     # сек между запросами участников
MAX_PER_GROUP        = 500   # лимит участников из одной группы (автосбор)

# Пассивный сбор: накапливаем и сбрасываем в Избранное пачкой
PASSIVE_FLUSH_EVERY  = 50    # отправлять в Избранное каждые N новых юзернеймов

STATE_FILE = "collector_state.json"

# Глобальный стейт — общий для всех трёх режимов
_state_lock      = asyncio.Lock()
_passive_pending: list[str] = []   # накопленные из сообщений, ещё не отправленные


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
#  Отправка в Избранное
# ══════════════════════════════════════════════════════════════

async def send_to_saved(app: Client, usernames: list, label: str = ""):
    prefix = f" [{label}]" if label else ""
    for i in range(0, len(usernames), BATCH_SIZE):
        chunk = usernames[i:i + BATCH_SIZE]
        text = (
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
#  Сборка из списка участников (для авто и .собрать)
# ══════════════════════════════════════════════════════════════

async def collect_from_group(
    app: Client,
    chat_id,
    existing: set,
    limit: int = MAX_PER_GROUP,
) -> list:
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
        log.info("Нет прав смотреть список — используй пассивный сбор")
    except Exception as e:
        log.warning(f"Ошибка collect_from_group: {e}")
    return found


# ══════════════════════════════════════════════════════════════
#  РЕЖИМ 2: Пассивный сбор из сообщений
# ══════════════════════════════════════════════════════════════

def setup_passive_handler(app: Client):
    """Слушает все входящие сообщения в группах и собирает юзернеймы авторов."""

    @app.on_message(filters.group & ~filters.me)
    async def on_group_message(client: Client, message: Message):
        global _passive_pending

        user = message.from_user
        if not user or not user.username or user.is_bot:
            return

        uname = user.username.lower()

        async with _state_lock:
            state = load_state()
            existing = set(u.lower() for u in state["collected"])

            if uname in existing:
                return  # уже есть — пропускаем

            # Новый юзернейм — добавляем
            _passive_pending.append(user.username)
            state["collected"].append(user.username)
            save_state(state)
            log.info(f"[пассивный] +@{user.username} из {message.chat.title[:30]} (буфер: {len(_passive_pending)})")

            # Если накопилось достаточно — сбрасываем в Избранное
            if len(_passive_pending) >= PASSIVE_FLUSH_EVERY:
                to_send = _passive_pending[:]
                _passive_pending = []
                await send_to_saved(client, to_send, label="пассивный")


# ══════════════════════════════════════════════════════════════
#  РЕЖИМ 3: Ручная команда .собрать
# ══════════════════════════════════════════════════════════════

def setup_manual_handler(app: Client):

    @app.on_message(
        filters.regex(r"^\.собрать$") & filters.group & filters.me
    )
    async def handle_manual_collect(client: Client, message: Message):
        chat_id    = message.chat.id
        chat_title = message.chat.title or str(chat_id)

        log.info(f"[.собрать] в группе: {chat_title}")

        try:
            await message.delete()
        except Exception:
            pass

        status_msg = await client.send_message(
            "me",
            f"⏳ Собираю юзернеймы из «{chat_title}»..."
        )

        async with _state_lock:
            state    = load_state()
            existing = set(u.lower() for u in state["collected"])

            found = await collect_from_group(
                client, chat_id, existing, limit=MANUAL_LIMIT
            )

            if found:
                state["collected"].extend(found)
                save_state(state)
                await send_to_saved(client, found, label=chat_title[:20])
                await client.edit_message_text(
                    "me", status_msg.id,
                    f"✅ «{chat_title}»: {len(found)} новых юзернеймов\n"
                    f"Всего в базе: {len(state['collected'])}"
                )
            else:
                # Список участников недоступен — подсказываем
                await client.edit_message_text(
                    "me", status_msg.id,
                    f"ℹ️ «{chat_title}»: список участников скрыт.\n"
                    f"Пассивный сбор уже работает — юзернеймы добавляются\n"
                    f"автоматически по мере того как люди пишут в группе.\n"
                    f"Всего в базе: {len(state['collected'])}"
                )

        log.info(f"[.собрать] завершено: {len(found)} новых из {chat_title}")


# ══════════════════════════════════════════════════════════════
#  РЕЖИМ 1: Авто — ежедневный обход
# ══════════════════════════════════════════════════════════════

async def run_once(app: Client):
    async with _state_lock:
        state     = load_state()
        collected = set(u.lower() for u in state["collected"])
        scanned   = set(state.get("scanned_groups", []))
        new_today = []
        remaining = MAX_PER_DAY - sent_today(state)

        log.info(f"[авто] в базе: {len(state['collected'])} | осталось сегодня: {remaining}")

        if remaining <= 0:
            log.info("[авто] дневной лимит выполнен.")
            return

        groups = []
        async for dialog in app.get_dialogs():
            chat = dialog.chat
            if chat.type.name in ("GROUP", "SUPERGROUP"):
                groups.append(chat)

        log.info(f"[авто] групп найдено: {len(groups)}")

        for chat in groups:
            if len(new_today) >= remaining:
                break

            gid = str(chat.id)
            if gid in scanned:
                continue

            log.info(f"[авто] обхожу: {chat.title[:40]} ...")
            found = await collect_from_group(app, chat.id, collected)
            space = remaining - len(new_today)
            found = found[:space]

            if found:
                new_today.extend(found)
                log.info(f"[авто] +{len(found)} (итого: {len(new_today)})")

            scanned.add(gid)
            await asyncio.sleep(DELAY_BETWEEN_GROUPS)

        if new_today:
            await send_to_saved(app, new_today, label="авто")
            state["collected"].extend(new_today)
            state["daily"][today_str()] = sent_today(state) + len(new_today)

        state["scanned_groups"] = list(scanned)
        save_state(state)
        log.info(f"[авто] готово. сегодня: {len(new_today)} | всего: {len(state['collected'])}")


# ══════════════════════════════════════════════════════════════
#  Планировщик
# ══════════════════════════════════════════════════════════════

async def scheduler(app: Client):
    log.info(f"Планировщик: автосбор каждый день в {RUN_HOUR}:00 UTC ({RUN_HOUR+3}:00 мск)")
    last_run_date = None

    # Сброс накопленного пассивного буфера раз в час
    last_flush = datetime.utcnow()

    while True:
        now = datetime.utcnow()

        # Ежедневный автосбор
        if now.hour == RUN_HOUR and last_run_date != now.date():
            log.info("Запускаю ежедневный автосбор...")
            try:
                await run_once(app)
            except Exception as e:
                log.exception(f"Ошибка автосбора: {e}")
            last_run_date = now.date()

        # Принудительный сброс пассивного буфера раз в час
        if (now - last_flush).seconds >= 3600 and _passive_pending:
            async with _state_lock:
                global _passive_pending
                if _passive_pending:
                    to_send = _passive_pending[:]
                    _passive_pending = []
                    log.info(f"Плановый сброс пассивного буфера: {len(to_send)} юзернеймов")
                    await send_to_saved(app, to_send, label="пассивный")
            last_flush = now

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
        setup_passive_handler(app)
        setup_manual_handler(app)
        log.info("✅ Подключено! Слушаю сообщения + жду .собрать + планировщик активен")
        await scheduler(app)

if __name__ == "__main__":
    asyncio.run(main())
