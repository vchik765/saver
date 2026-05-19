"""
Telegram Userbot — Сборщик юзернеймов из групп
Использует Pyrogram StringSession (без номера телефона при повторных запусках)
"""

import asyncio
import json
import os
from datetime import date
from pathlib import Path

from pyrogram import Client
from pyrogram.errors import FloodWait, ChatAdminRequired, UserNotParticipant

# ══════════════════════════════════════════════════════════════
#  НАСТРОЙКИ
# ══════════════════════════════════════════════════════════════

API_ID      = 2040
API_HASH    = "b18441a1ff607e10a989891a5462e627"

# Берётся из переменной окружения Railway
SESSION_STRING = os.environ.get("SESSION_STRING", "")

MAX_PER_DAY          = 200   # юзернеймов в день
BATCH_SIZE           = 50    # штук в одном сообщении в Избранном
DELAY_BETWEEN_GROUPS = 8     # сек между группами
DELAY_BETWEEN_CHUNKS = 2     # сек между запросами участников
MAX_PER_GROUP        = 500   # не больше N участников из одной группы

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
        print(f"    ⏳ Flood wait {e.value}с...")
        await asyncio.sleep(e.value + 5)
    except (ChatAdminRequired, UserNotParticipant):
        print(f"    ⚠ Нет прав смотреть участников")
    except Exception as e:
        print(f"    ⚠ Ошибка: {e}")
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
            print(f"  ✅ Отправлено: {len(chunk)} юзернеймов")
        except FloodWait as e:
            await asyncio.sleep(e.value + 3)
            await app.send_message("me", text)
        await asyncio.sleep(3)

# ══════════════════════════════════════════════════════════════
#  Главный цикл
# ══════════════════════════════════════════════════════════════

async def main():
    if not SESSION_STRING:
        print("❌ SESSION_STRING не задан в переменных Railway!")
        return

    state     = load_state()
    collected = set(u.lower() for u in state["collected"])
    scanned   = set(state.get("scanned_groups", []))
    new_today = []
    remaining = MAX_PER_DAY - sent_today(state)

    print(f"📊 Всего собрано: {len(state['collected'])} юзернеймов")
    print(f"📅 Осталось на сегодня: {remaining}/{MAX_PER_DAY}")

    if remaining <= 0:
        print("✅ Дневной лимит выполнен. Запусти завтра.")
        return

    async with Client("userbot", API_ID, API_HASH, session_string=SESSION_STRING) as app:
        print("✅ Подключено к Telegram!\n")

        # Получаем все группы
        groups = []
        async for dialog in app.get_dialogs():
            chat = dialog.chat
            if chat.type.name in ("GROUP", "SUPERGROUP"):
                groups.append(chat)

        print(f"📂 Групп найдено: {len(groups)}\n")

        for chat in groups:
            if len(new_today) >= remaining:
                print(f"\n⛔ Дневной лимит {MAX_PER_DAY} достигнут.")
                break

            gid = str(chat.id)
            if gid in scanned:
                print(f"⏭ Уже обходили: {chat.title[:40]}")
                continue

            print(f"🔍 Обхожу: {chat.title[:50]} ...")
            found = await collect_from_group(app, chat.id, collected)

            space = remaining - len(new_today)
            found = found[:space]

            if found:
                new_today.extend(found)
                print(f"   +{len(found)} юзернеймов (итого: {len(new_today)})")
            else:
                print(f"   Новых нет")

            scanned.add(gid)
            await asyncio.sleep(DELAY_BETWEEN_GROUPS)

        # Отправляем и сохраняем
        if new_today:
            print(f"\n📤 Отправляю {len(new_today)} в Избранное...")
            await send_to_saved(app, new_today)

            state["collected"].extend(new_today)
            state["daily"][today_str()] = sent_today(state) + len(new_today)
            state["scanned_groups"] = list(scanned)
            save_state(state)

            print(f"\n🎉 Готово! Сегодня: {len(new_today)}, всего: {len(state['collected'])}")
        else:
            print("\n😕 Новых юзернеймов не найдено.")
            state["scanned_groups"] = list(scanned)
            save_state(state)

if __name__ == "__main__":
    asyncio.run(main())
