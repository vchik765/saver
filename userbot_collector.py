"""
╔══════════════════════════════════════════════════════════════╗
║  Telegram Userbot — Сборщик юзернеймов из групп             ║
║  Без личного API Hash — используются ключи Telegram Desktop  ║
╚══════════════════════════════════════════════════════════════╝

УСТАНОВКА (один раз):
    pip install telethon

ЗАПУСК:
    python userbot_collector.py

При первом запуске введёшь номер телефона и код из Telegram.
Сессия сохранится — повторно вводить не нужно.
"""

import asyncio
import json
import os
import sys
from datetime import date
from pathlib import Path

from telethon import TelegramClient, errors
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import (
    ChannelParticipantsSearch,
    InputPeerChannel,
    Channel,
    Chat,
)

# ══════════════════════════════════════════════════════════════
#  НАСТРОЙКИ  ← меняй только здесь
# ══════════════════════════════════════════════════════════════

# Публичные ключи Telegram Desktop — работают без my.telegram.org
API_ID   = 2040
API_HASH = "b18441a1ff607e10a989891a5462e627"

# Твой номер телефона (с кодом страны)
PHONE_NUMBER = "+7XXXXXXXXXX"   # ← ЗАМЕНИ НА СВОЙ НОМЕР

# Лимиты безопасности (не трогай без нужды)
MAX_PER_DAY           = 200   # максимум юзернеймов в день
BATCH_SIZE            = 50    # сколько имён в одном сообщении в Избранном
DELAY_BETWEEN_GROUPS  = 7     # сек между обходом групп (против флуд-бана)
DELAY_BETWEEN_CHUNKS  = 2     # сек между запросами участников одной группы
MAX_PER_GROUP         = 500   # не берём больше N участников из одной группы

# Файл хранения прогресса
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
    return {
        "collected": [],        # все найденные юзернеймы
        "sent": [],             # уже отправленные в избранное
        "daily": {},            # {дата: кол-во} — лимит по дням
        "scanned_groups": [],   # группы которые уже обходили
    }

def save_state(state: dict):
    Path(STATE_FILE).write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

def today_str() -> str:
    return date.today().isoformat()

def sent_today(state: dict) -> int:
    return state["daily"].get(today_str(), 0)

def add_sent_today(state: dict, count: int):
    state["daily"][today_str()] = sent_today(state) + count

# ══════════════════════════════════════════════════════════════
#  Сборка юзернеймов
# ══════════════════════════════════════════════════════════════

async def collect_from_group(client, dialog, existing: set[str]) -> list[str]:
    """Собирает юзернеймы участников одной группы."""
    found = []
    offset = 0
    limit  = 100

    while True:
        try:
            result = await client(GetParticipantsRequest(
                channel=dialog.entity,
                filter=ChannelParticipantsSearch(""),
                offset=offset,
                limit=limit,
                hash=0,
            ))
        except errors.ChatAdminRequiredError:
            print(f"    ⚠ Нет прав смотреть участников: {dialog.name}")
            break
        except errors.FloodWaitError as e:
            print(f"    ⏳ FloodWait {e.seconds}с — жду...")
            await asyncio.sleep(e.seconds + 5)
            continue
        except Exception as e:
            print(f"    ⚠ Ошибка: {e}")
            break

        if not result.users:
            break

        for user in result.users:
            if user.username and user.username.lower() not in existing:
                found.append(user.username)
                existing.add(user.username.lower())

        offset += len(result.users)
        if offset >= MAX_PER_GROUP or len(result.users) < limit:
            break

        await asyncio.sleep(DELAY_BETWEEN_CHUNKS)

    return found

# ══════════════════════════════════════════════════════════════
#  Отправка в Избранное
# ══════════════════════════════════════════════════════════════

async def send_to_saved(client, usernames: list[str]):
    """Отправляет юзернеймы в Избранное батчами по BATCH_SIZE."""
    for i in range(0, len(usernames), BATCH_SIZE):
        chunk = usernames[i:i + BATCH_SIZE]
        text  = (
            f"📋 Собранные юзернеймы [{i+1}–{i+len(chunk)}] "
            f"({date.today().isoformat()}):\n\n"
            + "\n".join(f"@{u}" for u in chunk)
        )
        try:
            await client.send_message("me", text)
            print(f"  ✅ Отправлено в Избранное: {len(chunk)} юзернеймов")
        except errors.FloodWaitError as e:
            print(f"  ⏳ FloodWait {e.seconds}с...")
            await asyncio.sleep(e.seconds + 3)
            await client.send_message("me", text)
        await asyncio.sleep(3)

# ══════════════════════════════════════════════════════════════
#  Главный цикл
# ══════════════════════════════════════════════════════════════

async def main():
    if PHONE_NUMBER == "+7XXXXXXXXXX":
        print("❌ Сначала замени PHONE_NUMBER в скрипте на свой номер!")
        sys.exit(1)

    state      = load_state()
    collected  = set(u.lower() for u in state["collected"])
    new_today  = []
    scanned    = set(state.get("scanned_groups", []))
    remaining  = MAX_PER_DAY - sent_today(state)

    print(f"📊 Уже собрано всего: {len(state['collected'])} юзернеймов")
    print(f"📅 Лимит на сегодня: осталось собрать {remaining} из {MAX_PER_DAY}")

    if remaining <= 0:
        print("✅ Дневной лимит уже выполнен. Запусти завтра.")
        return

    client = TelegramClient("userbot_session", API_ID, API_HASH)
    await client.start(phone=PHONE_NUMBER)
    print("✅ Подключено к Telegram!\n")

    try:
        # Получаем список всех диалогов (групп и каналов)
        print("📂 Загружаю список групп...")
        dialogs = await client.get_dialogs()

        groups = [
            d for d in dialogs
            if isinstance(d.entity, (Channel, Chat))
            and not getattr(d.entity, "broadcast", False)  # исключаем каналы (только группы)
        ]

        print(f"   Найдено групп: {len(groups)}\n")

        for dialog in groups:
            if len(new_today) >= remaining:
                print(f"\n⛔ Дневной лимит {MAX_PER_DAY} достигнут — останавливаюсь.")
                break

            group_id = str(getattr(dialog.entity, "id", ""))
            if group_id in scanned:
                print(f"⏭ Уже обходили: {dialog.name[:40]}")
                continue

            print(f"🔍 Обхожу: {dialog.name[:50]} ...")

            try:
                found = await collect_from_group(client, dialog, collected)
            except Exception as e:
                print(f"   ⚠ Ошибка при обходе: {e}")
                found = []

            # Обрезаем до дневного лимита
            space = remaining - len(new_today)
            found = found[:space]

            if found:
                new_today.extend(found)
                print(f"   Новых юзернеймов: +{len(found)} (итого сегодня: {len(new_today)})")
            else:
                print(f"   Новых нет (или нет прав)")

            scanned.add(group_id)
            await asyncio.sleep(DELAY_BETWEEN_GROUPS)

        # Сохраняем и отправляем в Избранное
        if new_today:
            print(f"\n📤 Отправляю {len(new_today)} юзернеймов в Избранное...")
            await send_to_saved(client, new_today)

            state["collected"].extend(new_today)
            state["sent"].extend(new_today)
            add_sent_today(state, len(new_today))
            state["scanned_groups"] = list(scanned)
            save_state(state)

            print(f"\n🎉 Готово! Сегодня собрано: {len(new_today)}")
            print(f"   Всего за всё время:       {len(state['collected'])}")
            print(f"   Сохранено в {STATE_FILE}")
        else:
            print("\n😕 Новых юзернеймов не найдено.")
            state["scanned_groups"] = list(scanned)
            save_state(state)

    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
