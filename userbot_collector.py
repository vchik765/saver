"""
Telegram Userbot — Сборщик юзернеймов (автоматический, работает 24/7)
Каждый день в заданное время сам собирает 200 юзернеймов и шлёт в Избранное.
Запускается один раз — дальше работает сам.
"""

import asyncio
import json
import logging
import os
from datetime import date, datetime
from pathlib import Path

from pyrogram import Client
from pyrogram.errors import FloodWait, ChatAdminRequired

logging.basicConfig(level=logging.INFO, format="%(asctime)s [collector] %(message)s")
log = logging.getLogger("collector")

# ══════════════════════════════════════════════════════════════
#  НАСТРОЙКИ
# ══════════════════════════════════════════════════════════════

API_ID         = 2040
API_HASH       = "b18441a1ff607e10a989891a5462e627"
SESSION_STRING = os.environ.get("SESSION_STRING", "")

RUN_HOUR             = 12    # в каком часу запускать (12 = полдень, UTC)
MAX_PER_DAY          = 200
BATCH_SIZE           = 50
DELAY_BETWEEN_GROUPS = 8     # сек между группами
DELAY_BETWEEN_CHUNKS = 2     # сек между запросами участников
MAX_PER_GROUP        = 500

STATE_FILE = "collector_state.json"

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

async def collect_from_group(app: Client, chat_id, existing: set) -> list:
    found = []
    try:
        count = 0
        async for member in app.get_chat_members(chat_id):
            if count >= MAX_PER_GROUP:
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
        log.warning(f"Ошибка: {e}")
    return found

# ══════════════════════════════════════════════════════════════
#  Отправка в Избранное
# ══════════════════════════════════════════════════════════════

async def send_to_saved(app: Client, usernames: list):
    for i in range(0, len(usernames), BATCH_SIZE):
        chunk = usernames[i:i + BATCH_SIZE]
        text  = (
            f"📋 Юзернеймы [{i+1}–{i+len(chunk)}] ({today_str()}):\n\n"
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
#  Один рабочий цикл (запускается раз в день)
# ══════════════════════════════════════════════════════════════

async def run_once(app: Client):
    state     = load_state()
    collected = set(u.lower() for u in state["collected"])
    scanned   = set(state.get("scanned_groups", []))
    new_today = []
    remaining = MAX_PER_DAY - sent_today(state)

    log.info(f"Всего собрано: {len(state['collected'])} | осталось сегодня: {remaining}")

    if remaining <= 0:
        log.info("Дневной лимит уже выполнен.")
        return

    # Собираем список групп
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
            log.info(f"+{len(found)} юзернеймов (итого сегодня: {len(new_today)})")

        scanned.add(gid)
        await asyncio.sleep(DELAY_BETWEEN_GROUPS)

    if new_today:
        log.info(f"Отправляю {len(new_today)} в Избранное...")
        await send_to_saved(app, new_today)
        state["collected"].extend(new_today)
        state["daily"][today_str()] = sent_today(state) + len(new_today)

    state["scanned_groups"] = list(scanned)
    save_state(state)
    log.info(f"Готово. Сегодня: {len(new_today)} | всего: {len(state['collected'])}")

# ══════════════════════════════════════════════════════════════
#  Планировщик — запускается каждый день в RUN_HOUR (UTC)
# ══════════════════════════════════════════════════════════════

async def scheduler(app: Client):
    log.info(f"Планировщик запущен. Сбор каждый день в {RUN_HOUR}:00 UTC")
    last_run_date = None

    while True:
        now = datetime.utcnow()

        # Запускаем если наступил нужный час И сегодня ещё не запускали
        if now.hour == RUN_HOUR and last_run_date != now.date():
            log.info("Запускаю ежедневный сбор...")
            try:
                await run_once(app)
            except Exception as e:
                log.exception(f"Ошибка в run_once: {e}")
            last_run_date = now.date()

        # Ждём 60 секунд до следующей проверки
        await asyncio.sleep(60)

# ══════════════════════════════════════════════════════════════
#  Точка входа
# ══════════════════════════════════════════════════════════════

async def main():
    if not SESSION_STRING:
        log.error("SESSION_STRING не задан в переменных окружения Railway!")
        return

    log.info("Подключаюсь к Telegram...")
    async with Client("userbot", API_ID, API_HASH, session_string=SESSION_STRING) as app:
        log.info("✅ Подключено! Жду нужного времени...")
        await scheduler(app)

if __name__ == "__main__":
    asyncio.run(main())
