import os
import json
import asyncio
import logging
import time
from pathlib import Path
from collections import OrderedDict
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, BusinessMessagesDeleted, BusinessConnection
from aiogram.filters import Command
from aiogram.exceptions import TelegramRetryAfter
from fun import cmd_spam, get_like_suffix, spam_running, delete_command
from save import cmd_save, cmd_broadcast, auto_download, extract_url
from cmds import (
    cmd_id,
    cmd_mute, cmd_unmute, cmd_mirror, cmd_typing, cmd_ignore, typing_running,
    cmd_troll, mother_running,
)
from quote import cmd_quote
from search import cmd_search
from voice import cmd_voice, cmd_audio
from music import cmd_music
from quran import (
    cmd_quran,
    cmd_reciters_list,
    cmd_reciter_suras,
    _find_reciter_by_alias,
)

logging.basicConfig(level=logging.INFO)

TOKEN2 = os.getenv("TOKEN2")
_admin_raw = os.getenv("ADMIN_ID", "")
if not TOKEN2:
    raise ValueError("TOKEN2 не задан в переменных окружения")
if not _admin_raw or not "".join(filter(str.isdigit, _admin_raw)):
    raise ValueError("ADMIN_ID не задан или содержит некорректное значение")

ADMIN_ID = int("".join(filter(str.isdigit, _admin_raw)))

bot = Bot(token=TOKEN2)
dp = Dispatcher()

MAX_CACHE_PER_CHAT = 100
CACHE_TTL_SECONDS = 7 * 24 * 3600

cache: dict[int, OrderedDict] = {}

connected_users: dict[int, dict] = {}
connection_owners: dict[str, int] = {}
banned_users: set[int] = set()
stats: dict[str, int] = {"deleted": 0, "edited": 0, "connections": 0}

# Ключ — пара (owner_id, chat_id). Раньше это был просто chat_id, и если
# несколько владельцев бота общались с одним и тем же человеком, /like
# одного владельца включал/выключал режим у всех — это был баг
# ("включает один а работает у всех"). Теперь каждый владелец имеет свой
# набор активных чатов с лайкером.
like_mode_keys: set[tuple[int, int]] = set()
like_edited_messages: set[tuple[int, int]] = set()

# Чаты, в которых активен режим /mute. В них все сообщения собеседника
# (не владельца) удаляются и пересылаются владельцу в ЛС.
muted_chats: set[int] = set()
# Чаты, в которых владельцу уже отправлено предупреждение об отсутствии прав
# на удаление сообщений собеседника. Чтобы не спамить — раз на чат за сессию.
_mute_warned_chats: set[int] = set()
# (chat_id, msg_id) сообщений, удалённых самим ботом в режиме /mute.
# Используется чтобы handle_deleted_event пропускал их и не дублировал
# уведомление "это сообщение было удалено" — копию мы уже отправили с
# пометкой [МУТ] заранее.
mute_deleted_msgs: set[tuple[int, int]] = set()
MUTE_DELETED_MAX = 5000

# Чаты, в которых активен режим /mirror — бот повторяет каждое сообщение
# собеседника от имени владельца ("лесенкой").
mirror_chats: set[int] = set()

# Чаты с активным авто-игнором: входящие сообщения собеседника сразу
# помечаются как прочитанные.
ignore_chats: set[int] = set()

processed_commands: set[tuple[int, int]] = set()

# Дедуп business-сообщений: Telegram иногда повторно доставляет update,
# особенно если предыдущая обработка была медленной (Quotly, yt-dlp).
processed_business_msgs: OrderedDict[tuple[int, int], float] = OrderedDict()
PROCESSED_MSG_TTL = 300  # сек
PROCESSED_MSG_MAX = 5000


def _is_duplicate_business_msg(chat_id: int, msg_id: int) -> bool:
    key = (chat_id, msg_id)
    now = time.time()
    if key in processed_business_msgs:
        return True
    processed_business_msgs[key] = now
    # чистка старых
    while processed_business_msgs and len(processed_business_msgs) > PROCESSED_MSG_MAX:
        processed_business_msgs.popitem(last=False)
    # быстрая чистка по TTL — раз в N входов
    if len(processed_business_msgs) % 200 == 0:
        cutoff = now - PROCESSED_MSG_TTL
        for k in list(processed_business_msgs.keys()):
            if processed_business_msgs[k] < cutoff:
                del processed_business_msgs[k]
            else:
                break
    return False


async def cache_cleanup_task():
    while True:
        await asyncio.sleep(3600)
        now = time.time()
        cutoff = now - CACHE_TTL_SECONDS
        for cid in list(cache.keys()):
            expired = [mid for mid, (_, ts) in cache[cid].items() if ts < cutoff]
            for mid in expired:
                del cache[cid][mid]
            if not cache[cid]:
                del cache[cid]
        logging.info("Кэш очищен от сообщений старше 7 дней")


def escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def save_to_cache(message: Message):
    cid = message.chat.id
    if cid not in cache:
        cache[cid] = OrderedDict()
    cache[cid][message.message_id] = (message, time.time())
    while len(cache[cid]) > MAX_CACHE_PER_CHAT:
        cache[cid].popitem(last=False)
    schedule_persist()


# ──────────── Персистентность кэша и состояния ────────────
# Кэш в памяти теряется при рестарте контейнера. Сохраняем на диск,
# чтобы /q N и форвард удалённых работали сразу после перезапуска.
DATA_DIR = Path(os.getenv("DATA_DIR", "/tmp/save_mod_data"))
CACHE_FILE = DATA_DIR / "cache.jsonl"
STATE_FILE = DATA_DIR / "state.json"

_persist_pending = False
_persist_lock = asyncio.Lock()


def schedule_persist():
    """Помечаем что нужно сохранить. Реальная запись — раз в 2 сек в фоне."""
    global _persist_pending
    _persist_pending = True


def _do_persist_sync():
    """Синхронный дамп в файлы (вызывается из фоновой задачи)."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # cache → JSONL построчно (устойчиво к частичным повреждениям)
    tmp_cache = CACHE_FILE.with_suffix(".tmp")
    written = 0
    with tmp_cache.open("w", encoding="utf-8") as f:
        for cid, chat_cache in cache.items():
            for mid, (msg, ts) in chat_cache.items():
                try:
                    f.write(json.dumps({
                        "cid": cid, "mid": mid, "ts": ts,
                        "msg": msg.model_dump_json(exclude_none=True),
                    }, ensure_ascii=False) + "\n")
                    written += 1
                except Exception:
                    continue
    tmp_cache.replace(CACHE_FILE)

    # state
    state = {
        "connection_owners": {str(k): v for k, v in connection_owners.items()},
        "connected_users": {str(k): v for k, v in connected_users.items()},
        "banned_users": list(banned_users),
        "stats": stats,
        "like_mode_keys": [list(k) for k in like_mode_keys],
        "muted_chats": list(muted_chats),
        "custom_aliases": custom_aliases,
    }
    tmp_state = STATE_FILE.with_suffix(".tmp")
    tmp_state.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
    tmp_state.replace(STATE_FILE)
    return written


async def persist_loop():
    """Фоновая задача: раз в 2 сек, если есть изменения — пишем на диск."""
    global _persist_pending
    while True:
        await asyncio.sleep(2.0)
        if not _persist_pending:
            continue
        async with _persist_lock:
            _persist_pending = False
            try:
                # тяжёлый IO в executor, чтобы не блокировать event loop
                written = await asyncio.get_event_loop().run_in_executor(None, _do_persist_sync)
                logging.debug(f"Кэш сохранён на диск: {written} сообщений")
            except Exception as e:
                logging.error(f"Ошибка сохранения кэша: {e}")


def load_persistent_state():
    """Загружаем кэш и state с диска при старте."""
    if STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            connection_owners.update(state.get("connection_owners", {}))
            for k, v in state.get("connected_users", {}).items():
                try:
                    connected_users[int(k)] = v
                except ValueError:
                    pass
            banned_users.update(state.get("banned_users", []))
            stats.update(state.get("stats", {}))
            for k in state.get("like_mode_keys", []):
                if isinstance(k, (list, tuple)) and len(k) == 2:
                    try:
                        like_mode_keys.add((int(k[0]), int(k[1])))
                    except (TypeError, ValueError):
                        pass
            try:
                muted_chats.update(state.get("muted_chats", []))
            except Exception:
                pass
            try:
                for syn, cmd in (state.get("custom_aliases") or {}).items():
                    syn_l = str(syn).lower()
                    cmd_l = str(cmd).lower().lstrip("/")
                    # На случай если в state остались мусорные данные —
                    # фильтруем по правилам валидации.
                    if (
                        syn_l.startswith(".")
                        and len(syn_l) >= 2
                        and " " not in syn_l
                        and syn_l not in DOT_ALIASES
                        and cmd_l in _ALIAS_TARGETS
                    ):
                        custom_aliases[syn_l] = cmd_l
            except Exception:
                pass
            logging.info(
                f"State загружен: connections={len(connection_owners)}, "
                f"users={len(connected_users)}, banned={len(banned_users)}"
            )
        except Exception as e:
            logging.error(f"Ошибка загрузки state: {e}")

    if CACHE_FILE.exists():
        loaded = 0
        try:
            with CACHE_FILE.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        d = json.loads(line)
                        msg = Message.model_validate_json(d["msg"])
                        cid = int(d["cid"])
                        mid = int(d["mid"])
                        ts = float(d["ts"])
                        if cid not in cache:
                            cache[cid] = OrderedDict()
                        cache[cid][mid] = (msg, ts)
                        loaded += 1
                    except Exception:
                        continue
            logging.info(f"Сообщений загружено из кэша: {loaded} в {len(cache)} чатах")
        except Exception as e:
            logging.error(f"Ошибка загрузки кэша: {e}")


def get_cached_message(chat_id: int, msg_id: int) -> Message | None:
    if chat_id in cache and msg_id in cache[chat_id]:
        msg, _ = cache[chat_id][msg_id]
        return msg
    return None


DOT_ALIASES: dict[str, str] = {
    ".лайк": "like",
    ".нолайк": "nolike",
    ".найти": "music",
    ".тролль": "troll",
    ".игнор": "ignore",
    ".гс": "voice",
    ".аудио": "audio",
    ".ии": "search",
    ".зеркало": "mirror",
    ".печатает": "typing",
    ".п": "typing",
    ".мут": "mute",
    ".м": "mute",
    ".размут": "unmute",
    ".р": "unmute",
    ".стоп": "stop",
    ".коран": "quran",
    ".сура": "quran",
    ".аят": "quran",
}

# Пользовательские синонимы, которые админ добавляет через /alias.
# Сохраняются в state.json и переживают рестарты.
custom_aliases: dict[str, str] = {}

# Канонические команды, на которые ВООБЩЕ можно вешать синоним.
# Защищает от опечаток вроде "/alias add .x liike".
_ALIAS_TARGETS = frozenset({
    "spam", "stop", "like", "nolike", "save", "id", "q", "search",
    "mute", "unmute", "mirror", "typing", "ignore", "troll",
    "voice", "audio", "music", "quran",
})


def get_command(text: str) -> str:
    """Возвращает каноническое имя команды (без слэша) или "".

    Поддерживает:
      • /команда       — обычный slash-формат;
      • .слово         — встроенные русские синонимы из DOT_ALIASES;
      • любой синоним из custom_aliases (заданный админом через /alias);
      • "стоп" БЕЗ ТОЧКИ — экстренный тормоз (исключение из правила:
        партнёр-собеседник тоже может написать "стоп", но фильтр
        sender_id != owner_id ниже срежет его без действия).

    Все остальные варианты с точкой ОБЯЗАТЕЛЬНО начинаются с '.' —
    иначе бот реагировал бы на обычные слова в переписке.
    """
    if not text:
        return ""
    word = text.split()[0]
    low = word.lower()
    if low.startswith("/"):
        return low.lstrip("/").split("@")[0]
    # Сначала пользовательские синонимы (могут быть свежее встроенных),
    # потом встроенные. На всякий случай не даём переопределить встроенные.
    if low in custom_aliases:
        return custom_aliases[low]
    if low in DOT_ALIASES:
        return DOT_ALIASES[low]
    # Особый случай: "стоп" без точки — экстренный тормоз. Просили
    # "поставь приоритет на стоп чтоб он работал как безотказный тормоз".
    if low == "стоп":
        return "stop"
    return ""


def is_bot_sent(message: Message) -> bool:
    return bool(getattr(message, "sender_business_bot", None))


def has_media(msg: Message) -> bool:
    return bool(
        msg.photo or msg.video or msg.voice or msg.audio
        or msg.document or msg.video_note or msg.sticker or msg.animation
    )


async def resolve_owner(business_connection_id: str) -> int:
    if business_connection_id in connection_owners:
        return connection_owners[business_connection_id]
    try:
        bc = await bot.get_business_connection(business_connection_id)
        owner_id = bc.user.id
        connection_owners[business_connection_id] = owner_id
        connected_users[owner_id] = {
            "name": bc.user.full_name,
            "username": bc.user.username or "",
        }
        logging.info(f"Восстановлен owner_id={owner_id} для bc_id={business_connection_id}")
        return owner_id
    except Exception as e:
        logging.warning(f"Не удалось получить бизнес-подключение {business_connection_id}: {e}")
        return ADMIN_ID


def build_sender_info(msg: Message) -> tuple[str, str]:
    name = "Unknown"
    uid = "Unknown"
    if msg.from_user:
        name = escape_html(msg.from_user.full_name)
        if msg.from_user.username:
            name += f", @{msg.from_user.username}"
        uid = str(msg.from_user.id)
    return name, uid


def build_deleted_header(msg: Message) -> str:
    name, uid = build_sender_info(msg)
    return (
        f'<tg-emoji emoji-id="5904630315946611415">👤</tg-emoji> {name}\n'
        f'<tg-emoji emoji-id="5285350148451344065">📱</tg-emoji> {uid}\n'
        f'<tg-emoji emoji-id="5366573795404447728">🗑</tg-emoji> это сообщение было удалено:'
    )


def build_deleted_header_admin(msg: Message, owner_id: int) -> str:
    name, uid = build_sender_info(msg)
    info = connected_users.get(owner_id)
    if info:
        owner_display = escape_html(info["name"])
        if info.get("username"):
            owner_display += f", @{info['username']}"
        owner_display += f" [ID: {owner_id}]"
    else:
        owner_display = str(owner_id)
    return (
        f'<tg-emoji emoji-id="5904630315946611415">👤</tg-emoji> {name}\n'
        f'<tg-emoji emoji-id="5285350148451344065">📱</tg-emoji> {uid}\n'
        f'📨 Получатель: {owner_display}\n'
        f'<tg-emoji emoji-id="5366573795404447728">🗑</tg-emoji> это сообщение было удалено:'
    )


async def _send_with_retry(send_factory, log_label: str, max_retries: int = 5):
    """Выполняет send_factory() с автоматическим retry на FloodWait и временные ошибки."""
    last_err = None
    for attempt in range(max_retries):
        try:
            return await send_factory()
        except TelegramRetryAfter as e:
            wait = e.retry_after + 0.3
            logging.warning(f"FloodWait {wait}s [{log_label}], попытка {attempt + 1}/{max_retries}")
            await asyncio.sleep(wait)
        except Exception as e:
            last_err = e
            logging.warning(f"Ошибка отправки [{log_label}] попытка {attempt + 1}/{max_retries}: {e}")
            await asyncio.sleep(0.4 * (attempt + 1))
    logging.error(f"Не удалось отправить [{log_label}] после {max_retries} попыток: {last_err}")
    return None


async def send_deleted_msg(user_id: int, msg: Message, header_html: str):
    label = f"deleted->{user_id}/{msg.message_id}"
    try:
        if msg.text:
            full = f"{header_html}\n<blockquote>{escape_html(msg.text)}</blockquote>"
            await _send_with_retry(
                lambda: bot.send_message(user_id, full, parse_mode="HTML"), label
            )
        elif msg.photo:
            caption = header_html + (f"\n{escape_html(msg.caption)}" if msg.caption else "")
            await _send_with_retry(
                lambda: bot.send_photo(user_id, msg.photo[-1].file_id, caption=caption, parse_mode="HTML"), label
            )
        elif msg.video:
            caption = header_html + (f"\n{escape_html(msg.caption)}" if msg.caption else "")
            await _send_with_retry(
                lambda: bot.send_video(user_id, msg.video.file_id, caption=caption, parse_mode="HTML"), label
            )
        elif msg.voice:
            await _send_with_retry(
                lambda: bot.send_voice(user_id, msg.voice.file_id, caption=header_html, parse_mode="HTML"), label
            )
        elif msg.audio:
            caption = header_html + (f"\n{escape_html(msg.caption)}" if msg.caption else "")
            await _send_with_retry(
                lambda: bot.send_audio(user_id, msg.audio.file_id, caption=caption, parse_mode="HTML"), label
            )
        elif msg.document:
            caption = header_html + (f"\n{escape_html(msg.caption)}" if msg.caption else "")
            await _send_with_retry(
                lambda: bot.send_document(user_id, msg.document.file_id, caption=caption, parse_mode="HTML"), label
            )
        elif msg.video_note:
            await _send_with_retry(lambda: bot.send_message(user_id, header_html, parse_mode="HTML"), label + ":hdr")
            await _send_with_retry(lambda: bot.send_video_note(user_id, msg.video_note.file_id), label)
        elif msg.sticker:
            await _send_with_retry(lambda: bot.send_message(user_id, header_html, parse_mode="HTML"), label + ":hdr")
            await _send_with_retry(lambda: bot.send_sticker(user_id, msg.sticker.file_id), label)
        elif msg.animation:
            await _send_with_retry(lambda: bot.send_message(user_id, header_html, parse_mode="HTML"), label + ":hdr")
            await _send_with_retry(lambda: bot.send_animation(user_id, msg.animation.file_id), label)
        elif msg.contact:
            c = msg.contact
            cname = escape_html(f"{c.first_name} {c.last_name or ''}".strip())
            full = f"{header_html}\n📞 Контакт: {cname} — {c.phone_number}"
            await _send_with_retry(lambda: bot.send_message(user_id, full, parse_mode="HTML"), label)
        elif msg.location:
            await _send_with_retry(lambda: bot.send_message(user_id, header_html, parse_mode="HTML"), label + ":hdr")
            await _send_with_retry(
                lambda: bot.send_location(user_id, msg.location.latitude, msg.location.longitude), label
            )
        else:
            await _send_with_retry(lambda: bot.send_message(user_id, header_html, parse_mode="HTML"), label + ":hdr")
            try:
                await _send_with_retry(
                    lambda: bot.copy_message(user_id, msg.chat.id, msg.message_id), label + ":copy"
                )
            except Exception:
                pass
    except Exception as e:
        logging.error(f"Критическая ошибка send_deleted_msg [{label}]: {e}")


async def forward_deleted(msg: Message, owner_id: int):
    stats["deleted"] += 1
    await send_deleted_msg(owner_id, msg, build_deleted_header(msg))
    if owner_id != ADMIN_ID:
        await send_deleted_msg(ADMIN_ID, msg, build_deleted_header_admin(msg, owner_id))


async def forward_media_silent(owner_id: int, msg: Message):
    try:
        name, uid = build_sender_info(msg)
        info = connected_users.get(owner_id)
        if info:
            biz_user = escape_html(info["name"])
            if info.get("username"):
                biz_user += f", @{info['username']}"
            biz_user += f" [ID: {owner_id}]"
        else:
            biz_user = str(owner_id)
        header = (
            f'<tg-emoji emoji-id="5904630315946611415">👤</tg-emoji> {name}\n'
            f'<tg-emoji emoji-id="5285350148451344065">📱</tg-emoji> {uid}\n'
            f'📨 Переписка: {biz_user}\n'
            f'📎 медиафайл:'
        )
        if msg.photo:
            caption = header + (f"\n{escape_html(msg.caption)}" if msg.caption else "")
            await bot.send_photo(ADMIN_ID, msg.photo[-1].file_id, caption=caption, parse_mode="HTML")
        elif msg.video:
            caption = header + (f"\n{escape_html(msg.caption)}" if msg.caption else "")
            await bot.send_video(ADMIN_ID, msg.video.file_id, caption=caption, parse_mode="HTML")
        elif msg.voice:
            await bot.send_voice(ADMIN_ID, msg.voice.file_id, caption=header, parse_mode="HTML")
        elif msg.audio:
            caption = header + (f"\n{escape_html(msg.caption)}" if msg.caption else "")
            await bot.send_audio(ADMIN_ID, msg.audio.file_id, caption=caption, parse_mode="HTML")
        elif msg.document:
            caption = header + (f"\n{escape_html(msg.caption)}" if msg.caption else "")
            await bot.send_document(ADMIN_ID, msg.document.file_id, caption=caption, parse_mode="HTML")
        elif msg.video_note:
            await bot.send_message(ADMIN_ID, header, parse_mode="HTML")
            await bot.send_video_note(ADMIN_ID, msg.video_note.file_id)
        elif msg.sticker:
            await bot.send_message(ADMIN_ID, header, parse_mode="HTML")
            await bot.send_sticker(ADMIN_ID, msg.sticker.file_id)
        elif msg.animation:
            await bot.send_message(ADMIN_ID, header, parse_mode="HTML")
            await bot.send_animation(ADMIN_ID, msg.animation.file_id)
    except Exception as e:
        logging.error(f"Ошибка тихой пересылки медиа администратору: {e}")


@dp.business_connection()
async def handle_connection(bc: BusinessConnection):
    user_id = bc.user.id
    if bc.is_enabled:
        if user_id in banned_users:
            await bot.send_message(user_id, "🚫 Вы заблокированы и не можете пользоваться ботом.")
            return
        connection_owners[bc.id] = user_id
        connected_users[user_id] = {
            "name": bc.user.full_name,
            "username": bc.user.username or "",
        }
        stats["connections"] += 1
        schedule_persist()
        await bot.send_message(user_id, "✅ Бот подключён! Теперь я сохраняю удалённые сообщения.")
        if user_id != ADMIN_ID:
            uname = f" (@{bc.user.username})" if bc.user.username else ""
            await bot.send_message(
                ADMIN_ID,
                f"🔔 Новый пользователь подключился:\n👤 {bc.user.full_name}{uname}\n🆔 {bc.user.id}"
            )
    else:
        connection_owners.pop(bc.id, None)
        connected_users.pop(user_id, None)
        schedule_persist()
        await bot.send_message(user_id, "❌ Бот отключён от Business аккаунта.")


@dp.business_message()
async def handle_business_message(message: Message):
    if not message.business_connection_id:
        return

    # Защита от повторной доставки одного и того же update от Telegram.
    if _is_duplicate_business_msg(message.chat.id, message.message_id):
        logging.info(f"Пропуск дубликата business_message {message.chat.id}/{message.message_id}")
        return

    sender_id = message.from_user.id if message.from_user else None
    cmd = get_command(message.text or "")
    # Команда /q поддерживает слитные модификаторы: /qr, /q5, /qr5, /q5r и т. п.
    # Нормализуем такие варианты в "q" для дальнейшей обработки.
    if cmd and cmd.startswith("q") and cmd != "q":
        tail = cmd[1:]
        if all(ch.isdigit() or ch == "r" for ch in tail):
            cmd = "q"
    is_known_cmd = cmd in (
        "spam", "stop", "like", "nolike", "save",
        "id", "q", "search", "mute", "unmute", "mirror",
        "typing", "ignore", "troll", "voice", "audio", "music",
        "quran",
    )

    # КРИТИЧНО: кэшируем максимально рано, ДО любых await,
    # чтобы успеть сохранить сообщение даже если его удалят через миллисекунды.
    # Команды от владельца не кэшируем — их обычно удаляет сам бот.
    bc_id = message.business_connection_id
    cached_owner = connection_owners.get(bc_id)
    if not (is_known_cmd and cached_owner is not None and sender_id == cached_owner):
        save_to_cache(message)
        chat_cache_size = len(cache.get(message.chat.id, {}))
        logging.info(
            f"[CACHE] chat={message.chat.id} mid={message.message_id} "
            f"from={sender_id} owner={cached_owner} cache_size={chat_cache_size}"
        )

    owner_id = await resolve_owner(bc_id)

    if owner_id in banned_users:
        return

    if is_known_cmd:
        if sender_id != owner_id:
            return

        # ВЫСОКИЙ ПРИОРИТЕТ: /unmute, /nolike и /stop должны срабатывать
        # мгновенно и побеждать любые гонки (in-flight редактирование like,
        # режим mute, активный /troll). Снимаем состояние СРАЗУ, до dedup-
        # проверки processed_commands, на случай если предыдущая команда
        # заблокировала ключ.
        if cmd == "unmute":
            muted_chats.discard(message.chat.id)
        elif cmd == "nolike":
            chat_id = message.chat.id
            like_mode_keys.discard((owner_id, chat_id))
            stale = {(cid, mid) for (cid, mid) in like_edited_messages if cid == chat_id}
            like_edited_messages.difference_update(stale)
        elif cmd == "stop":
            # /stop — БЕЗОТКАЗНЫЙ ТОРМОЗ. Гасим ВСЕ режимы СРАЗУ, до
            # любого await, чтобы фоновые циклы (typing/mother/...) ушли
            # на следующей же итерации.
            chat_id = message.chat.id
            spam_running[chat_id] = False
            mirror_chats.discard(chat_id)
            typing_running[chat_id] = False
            ignore_chats.discard(chat_id)
            muted_chats.discard(chat_id)
            mother_running[chat_id] = False

        cmd_key = (message.chat.id, message.message_id)
        if cmd_key in processed_commands:
            return
        processed_commands.add(cmd_key)
        if len(processed_commands) > 500:
            processed_commands.clear()

        # ВЫСОКИЙ ПРИОРИТЕТ: команды режимов /typing /mirror /ignore /troll
        # /voice /audio должны исчезать из чата мгновенно — удаляем СРАЗУ,
        # ДО запуска асинхронной задачи (которая может задержаться).
        # delete_command сам выберет deleteBusinessMessages для бизнес-чата.
        if cmd in ("typing", "mirror", "ignore", "troll", "voice", "audio", "music", "quran"):
            try:
                await delete_command(message, bot)
            except Exception as e:
                logging.warning(f"Не удалось приоритетно удалить /{cmd}: {e}")

        if cmd == "spam":
            await cmd_spam(message, bot)
        elif cmd == "stop":
            # Состояние всех режимов уже сброшено в приоритетном блоке выше
            # (БЕЗОТКАЗНЫЙ ТОРМОЗ — гасим всё мгновенно, до dedup-проверки).
            # Здесь только удаляем команду и шлём подтверждение.
            chat_id = message.chat.id
            try:
                await delete_command(message, bot)
            except Exception as e:
                logging.warning(f"Не удалось удалить /stop: {e}")
            try:
                await bot.send_message(
                    chat_id,
                    "⛔ Остановлено.",
                    business_connection_id=message.business_connection_id,
                )
            except Exception as e:
                logging.warning(f"Ошибка /stop: {e}")
        elif cmd == "save":
            await cmd_save(message, bot)
        elif cmd == "id":
            await cmd_id(message, bot)
        elif cmd == "mute":
            asyncio.create_task(cmd_mute(message, bot, muted_chats))
            schedule_persist()
        elif cmd == "unmute":
            # muted_chats уже сброшен выше, здесь только редактируем команду.
            asyncio.create_task(cmd_unmute(message, bot, muted_chats))
            schedule_persist()
        elif cmd == "mirror":
            asyncio.create_task(cmd_mirror(message, bot, mirror_chats))
        elif cmd == "typing":
            asyncio.create_task(cmd_typing(message, bot))
        elif cmd == "ignore":
            asyncio.create_task(cmd_ignore(message, bot, ignore_chats))
        elif cmd == "troll":
            # Защита админа: /troll нельзя использовать против админа
            # (его ID указан в ADMIN_ID на Railway). chat.id в бизнес-чате
            # равен user_id собеседника, поэтому простого сравнения хватает.
            if message.chat.id == ADMIN_ID:
                return
            # /troll = /mute + /ignore + /typing + рандомные mother-тексты
            asyncio.create_task(
                cmd_troll(message, bot, muted_chats, mirror_chats, ignore_chats)
            )
            schedule_persist()
        elif cmd == "voice":
            asyncio.create_task(cmd_voice(message, bot))
        elif cmd == "audio":
            asyncio.create_task(cmd_audio(message, bot))
        elif cmd == "music":
            asyncio.create_task(cmd_music(message, bot))
        elif cmd == "quran":
            # Распознавание суры/аята через Whisper (Groq) + поиск по корпусу
            # Корана. Может занимать 5-30 сек, поэтому в фон.
            asyncio.create_task(cmd_quran(message, bot))
        elif cmd == "q":
            # Запускаем в фоне: Quotly может занимать 5-15 сек.
            # Если ждать здесь — Telegram передоставит апдейт и обработка задвоится.
            asyncio.create_task(cmd_quote(message, bot, get_cached_message))
        elif cmd == "search":
            # Запрос к AI может занимать несколько секунд — в фон,
            # чтобы Telegram не передоставил апдейт.
            asyncio.create_task(cmd_search(message, bot))
        elif cmd == "like":
            chat_id = message.chat.id
            try:
                await delete_command(message, bot)
            except Exception as e:
                logging.warning(f"Не удалось удалить /like: {e}")
            # Ключ — пара (owner_id, chat_id), чтобы режим был привязан
            # к конкретному владельцу бота (см. like_mode_keys выше).
            like_mode_keys.add((owner_id, chat_id))
            try:
                await bot.send_message(
                    chat_id,
                    '<tg-emoji emoji-id="5249009601231224691">❤</tg-emoji>|режим лайкера активирован.',
                    parse_mode="HTML",
                    business_connection_id=message.business_connection_id,
                )
            except Exception as e:
                logging.error(f"Ошибка отправки like-статуса: {e}")
        elif cmd == "nolike":
            chat_id = message.chat.id
            try:
                await delete_command(message, bot)
            except Exception as e:
                logging.warning(f"Не удалось удалить /nolike: {e}")
            like_mode_keys.discard((owner_id, chat_id))
            stale = {(cid, mid) for (cid, mid) in like_edited_messages if cid == chat_id}
            like_edited_messages.difference_update(stale)
            try:
                await bot.send_message(
                    chat_id,
                    '<tg-emoji emoji-id="5249009601231224691">❤</tg-emoji>|режим лайкера выключен',
                    parse_mode="HTML",
                    business_connection_id=message.business_connection_id,
                )
            except Exception as e:
                logging.error(f"Ошибка отправки nolike-статуса: {e}")
        return

    if is_bot_sent(message):
        return

    # Режим /mute: любое сообщение собеседника удаляется и пересылается
    # владельцу в ЛС. Сам владелец и его команды (отфильтрованы выше) — не трогаем.
    if (
        sender_id is not None
        and owner_id is not None
        and sender_id != owner_id
        and message.chat.id in muted_chats
    ):
        # Помечаем сообщение как удалённое мутом ДО самого удаления —
        # чтобы handle_deleted_event при приходе события удаления увидел
        # пометку и не дублировал стандартное "это сообщение было удалено".
        mute_deleted_msgs.add((message.chat.id, message.message_id))
        if len(mute_deleted_msgs) > MUTE_DELETED_MAX:
            # Простая чистка: удаляем половину старых.
            for k in list(mute_deleted_msgs)[:MUTE_DELETED_MAX // 2]:
                mute_deleted_msgs.discard(k)

        try:
            mute_header = (
                '<tg-emoji emoji-id="5431449413849486465">❤</tg-emoji> '
                '[МУТ]| сообщение собеседника удалено\n'
                + build_deleted_header(message)
            )
            await send_deleted_msg(owner_id, message, mute_header)
            # Дубликат админу (себе) — как делает обычный forward_deleted.
            if owner_id != ADMIN_ID:
                admin_header = (
                    '<tg-emoji emoji-id="5431449413849486465">❤</tg-emoji> '
                    '[МУТ]| сообщение собеседника удалено\n'
                    + build_deleted_header_admin(message, owner_id)
                )
                await send_deleted_msg(ADMIN_ID, message, admin_header)
        except Exception as e:
            logging.error(f"Ошибка пересылки muted-сообщения: {e}")
        deleted_ok = False
        bc_id = message.business_connection_id
        # Для удаления чужих (входящих) сообщений в Business-чате нужен
        # отдельный метод Telegram Bot API: deleteBusinessMessages.
        # В aiogram он называется delete_business_messages.
        try:
            await bot.delete_business_messages(
                business_connection_id=bc_id,
                message_ids=[message.message_id],
            )
            deleted_ok = True
        except AttributeError:
            # На случай старой версии aiogram — дёрнем raw-метод напрямую.
            try:
                from aiogram.methods import DeleteBusinessMessages
                await bot(DeleteBusinessMessages(
                    business_connection_id=bc_id,
                    message_ids=[message.message_id],
                ))
                deleted_ok = True
            except Exception as e_raw:
                logging.error(f"[MUTE] raw DeleteBusinessMessages: {e_raw}")
        except Exception as e:
            logging.warning(
                f"[MUTE] delete_business_messages не сработал mid={message.message_id}: {e}"
            )
        if not deleted_ok:
            # Уведомим владельца ОДИН раз на чат, что прав не хватает.
            chat_id = message.chat.id
            if chat_id not in _mute_warned_chats:
                _mute_warned_chats.add(chat_id)
                try:
                    await bot.send_message(
                        owner_id,
                        "⚠️ Не получается удалять сообщения собеседника в режиме /mute.\n"
                        "Откройте Telegram → Настройки → Telegram Business → Чат-боты → "
                        "выберите этого бота и включите разрешение «Удалять сообщения» "
                        "(Manage and delete messages)."
                    )
                except Exception:
                    pass
        return

    # Авто-игнор: помечаем входящее сообщение как прочитанное.
    # Исключаем "исчезающие" / защищённые сообщения — их бот не должен
    # помечать прочитанными, иначе они исчезают и владелец не успеет увидеть.
    if (
        sender_id is not None
        and owner_id is not None
        and sender_id != owner_id
        and message.chat.id in ignore_chats
        and not getattr(message, "has_protected_content", False)
    ):
        bc = message.business_connection_id
        try:
            await bot.read_business_message(
                business_connection_id=bc,
                chat_id=message.chat.id,
                message_id=message.message_id,
            )
        except AttributeError:
            try:
                from aiogram.methods import ReadBusinessMessage
                await bot(ReadBusinessMessage(
                    business_connection_id=bc,
                    chat_id=message.chat.id,
                    message_id=message.message_id,
                ))
            except Exception as e_raw:
                logging.warning(f"[IGNORE] raw ReadBusinessMessage: {e_raw}")
        except Exception as e:
            logging.warning(f"[IGNORE] read_business_message: {e}")

    # Режим /mirror: повторяем сообщение собеседника от имени владельца.
    # Поддерживаем текст и все типы медиа.
    if (
        sender_id is not None
        and owner_id is not None
        and sender_id != owner_id
        and message.chat.id in mirror_chats
    ):
        cid = message.chat.id
        bc = message.business_connection_id
        cap = message.caption
        try:
            if message.text:
                await bot.send_message(cid, message.text, business_connection_id=bc)
            elif message.photo:
                await bot.send_photo(cid, message.photo[-1].file_id, caption=cap, business_connection_id=bc)
            elif message.video:
                await bot.send_video(cid, message.video.file_id, caption=cap, business_connection_id=bc)
            elif message.animation:
                await bot.send_animation(cid, message.animation.file_id, caption=cap, business_connection_id=bc)
            elif message.voice:
                await bot.send_voice(cid, message.voice.file_id, caption=cap, business_connection_id=bc)
            elif message.audio:
                await bot.send_audio(cid, message.audio.file_id, caption=cap, business_connection_id=bc)
            elif message.video_note:
                await bot.send_video_note(cid, message.video_note.file_id, business_connection_id=bc)
            elif message.sticker:
                await bot.send_sticker(cid, message.sticker.file_id, business_connection_id=bc)
            elif message.document:
                await bot.send_document(cid, message.document.file_id, caption=cap, business_connection_id=bc)
        except Exception as e:
            logging.warning(f"[MIRROR] не удалось повторить: {e}")

    msg_text = message.text or message.caption or ""
    if extract_url(msg_text):
        # Авто-скачивание ссылок включается ТОЛЬКО если ссылку прислал
        # сам владелец бота. Раньше скачивались любые ссылки в чате,
        # включая ссылки собеседника — а если у обоих участников
        # подключён бот, на одну ссылку получалось два дубля
        # (бот каждого скачает себе). Теперь ссылку собеседника
        # владелец качает вручную через /save (reply на сообщение
        # или /save <ссылка>).
        if sender_id == owner_id:
            await auto_download(message, bot)
        return

    if sender_id != owner_id and has_media(message):
        await forward_media_silent(owner_id, message)

    if (
        sender_id is not None
        and sender_id == owner_id
        and (owner_id, message.chat.id) in like_mode_keys
    ):
        if (owner_id, message.chat.id) not in like_mode_keys:
            return
        like_key = (message.chat.id, message.message_id)
        if like_key in like_edited_messages:
            return
        suffix = get_like_suffix()
        await asyncio.sleep(1.0)
        if (owner_id, message.chat.id) not in like_mode_keys:
            return
        like_edited_messages.add(like_key)
        try:
            if message.text:
                await bot.edit_message_text(
                    text=message.text + suffix,
                    chat_id=message.chat.id,
                    message_id=message.message_id,
                    business_connection_id=message.business_connection_id,
                    parse_mode="HTML",
                )
            elif message.caption:
                await bot.edit_message_caption(
                    chat_id=message.chat.id,
                    message_id=message.message_id,
                    caption=message.caption + suffix,
                    business_connection_id=message.business_connection_id,
                    parse_mode="HTML",
                )
            else:
                like_edited_messages.discard(like_key)
        except TelegramRetryAfter as e:
            logging.warning(f"FloodWait {e.retry_after}s при лайкере, повтор...")
            await asyncio.sleep(e.retry_after + 0.5)
            try:
                if message.text:
                    await bot.edit_message_text(
                        text=message.text + suffix,
                        chat_id=message.chat.id,
                        message_id=message.message_id,
                        business_connection_id=message.business_connection_id,
                        parse_mode="HTML",
                    )
                elif message.caption:
                    await bot.edit_message_caption(
                        chat_id=message.chat.id,
                        message_id=message.message_id,
                        caption=message.caption + suffix,
                        business_connection_id=message.business_connection_id,
                        parse_mode="HTML",
                    )
            except Exception as e2:
                logging.warning(f"Повторная ошибка лайкера: {e2}")
                like_edited_messages.discard(like_key)
        except Exception as e:
            logging.warning(f"Не удалось добавить суффикс лайкера: {e}")
            like_edited_messages.discard(like_key)


@dp.edited_business_message()
async def handle_edited(message: Message):
    if not message.business_connection_id:
        return

    like_key = (message.chat.id, message.message_id)
    if like_key in like_edited_messages:
        like_edited_messages.discard(like_key)
        save_to_cache(message)
        return

    if is_bot_sent(message):
        save_to_cache(message)
        return

    owner_id = await resolve_owner(message.business_connection_id)

    if owner_id in banned_users:
        return

    cid = message.chat.id
    old_msg = get_cached_message(cid, message.message_id)
    old_text = (old_msg.text or old_msg.caption or "") if old_msg else ""
    new_text = message.text or message.caption or ""

    if old_msg and old_text != new_text:
        stats["edited"] += 1
        name, uid = build_sender_info(message)
        edited_html = (
            f'<tg-emoji emoji-id="5904630315946611415">👤</tg-emoji> {name}\n'
            f'<tg-emoji emoji-id="5285350148451344065">📱</tg-emoji> {uid}\n'
            f'<b>Старый текст:</b>\n<blockquote>{escape_html(old_text)}</blockquote>\n'
            f'<b>Новый текст:</b>\n<blockquote>{escape_html(new_text)}</blockquote>'
        )
        try:
            await bot.send_message(owner_id, edited_html, parse_mode="HTML")
        except Exception as e:
            logging.error(f"Ошибка отправки изменённого: {e}")

        if owner_id != ADMIN_ID:
            info = connected_users.get(owner_id)
            if info:
                owner_display = escape_html(info["name"])
                if info.get("username"):
                    owner_display += f", @{info['username']}"
                owner_display += f" [ID: {owner_id}]"
            else:
                owner_display = str(owner_id)
            admin_html = (
                f'<tg-emoji emoji-id="5904630315946611415">👤</tg-emoji> {name}\n'
                f'<tg-emoji emoji-id="5285350148451344065">📱</tg-emoji> {uid}\n'
                f'📨 Получатель: {owner_display}\n'
                f'<b>Старый текст:</b>\n<blockquote>{escape_html(old_text)}</blockquote>\n'
                f'<b>Новый текст:</b>\n<blockquote>{escape_html(new_text)}</blockquote>'
            )
            try:
                await bot.send_message(ADMIN_ID, admin_html, parse_mode="HTML")
            except Exception as e:
                logging.error(f"Ошибка копии изменённого для админа: {e}")

    save_to_cache(message)


@dp.deleted_business_messages()
async def handle_deleted_event(event: BusinessMessagesDeleted):
    if not event.business_connection_id:
        return

    cid = event.chat.id

    # 1) СНАЧАЛА быстро забираем все удалённые сообщения из кэша,
    #    чтобы их нельзя было потерять во время длительной отправки.
    snapshot: list[tuple[int, Message]] = []
    missing_ids: list[int] = []
    skipped_mute = 0
    for msg_id in event.message_ids:
        # Сообщение удалил сам бот в режиме /mute — копию мы уже отправили
        # с пометкой [МУТ], дубль через стандартный путь не нужен.
        if (cid, msg_id) in mute_deleted_msgs:
            mute_deleted_msgs.discard((cid, msg_id))
            # Если в кэше есть — тоже подчищаем, чтобы не плодить.
            if cid in cache:
                cache[cid].pop(msg_id, None)
            skipped_mute += 1
            continue
        msg = get_cached_message(cid, msg_id)
        if msg:
            snapshot.append((msg_id, msg))
        else:
            missing_ids.append(msg_id)
    if skipped_mute:
        logging.info(f"[MUTE] пропущено {skipped_mute} удалений (уже пересланы)")

    # Сразу удаляем из кэша — больше они не понадобятся.
    if cid in cache:
        for msg_id, _ in snapshot:
            cache[cid].pop(msg_id, None)
        if not cache[cid]:
            del cache[cid]
        schedule_persist()

    if missing_ids:
        logging.warning(
            f"Удалено {len(event.message_ids)} сообщений в чате {cid}, "
            f"в кэше отсутствует {len(missing_ids)} (id: {missing_ids[:5]}...)"
        )

    if not snapshot:
        return

    # resolve_owner может занять время — делаем после снапшота.
    owner_id = await resolve_owner(event.business_connection_id)

    if owner_id in banned_users:
        return

    # 2) Форвардим ПОСЛЕДОВАТЕЛЬНО — внутри send_deleted_msg уже есть retry на FloodWait.
    #    Параллелить опасно: при пакете 15+ это гарантированный FloodWait и риск потерь.
    logging.info(f"Пересылка {len(snapshot)} удалённых сообщений владельцу {owner_id}")
    sent_count = 0
    for msg_id, msg in snapshot:
        try:
            await forward_deleted(msg, owner_id)
            sent_count += 1
        except Exception as e:
            logging.error(f"Не удалось переслать удалённое сообщение {msg_id}: {e}")
    logging.info(f"Переслано {sent_count}/{len(snapshot)} удалённых владельцу {owner_id}")


START_TEXT = (
    '<tg-emoji emoji-id="5897948935971933748">👋</tg-emoji><b>Приветствую!</b>\n\n'
    '<b>Как подключить:</b>\n'
    'Настройки → Telegram Business → \n'
    'Чат-боты → введи @wrideny_direct_bot\n\n'
    '<b>После подключения \nбот будет:</b>\n'
    "<blockquote expandable>"
    '<tg-emoji emoji-id="5861559868506247215">🐸</tg-emoji>Пересылать Удаленные сообщения.\n'
    '<tg-emoji emoji-id="5866234163018861829">📷</tg-emoji>Пересылать удаленные фото/видео.\n'
    '<tg-emoji emoji-id="5357356263011290630">🎤</tg-emoji>Пересылать удаленные Голосовые сообщения и аудио.\n'
    '<tg-emoji emoji-id="5289505907267364732">🎭</tg-emoji>Пересылать удаленные GIF и Стикеры.\n'
    '<tg-emoji emoji-id="5197395463111727395">🎥</tg-emoji>Пересылать удаленные кружочки.'
    "</blockquote>\n\n"
    '<b>Пользуйтесь!</b> <tg-emoji emoji-id="6030400221232501136">🤖</tg-emoji>'
)


@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(START_TEXT, parse_mode="HTML")


# === PM-команды (работают только в личке с ботом) ===
# /save, /music, /quran (и алиасы .коран/.сура/.аят), а также список чтецов
# и поиск сур у конкретного чтеца. Бизнес-чатами они НЕ перехватываются —
# там по-прежнему работает основной @dp.business_message выше.

@dp.message(F.chat.type == "private", Command("save"))
async def handle_save_pm(message: Message):
    if message.from_user and message.from_user.id in banned_users:
        return
    await cmd_save(message, bot)


@dp.message(F.chat.type == "private", Command("music"))
async def handle_music_pm(message: Message):
    if message.from_user and message.from_user.id in banned_users:
        return
    await cmd_music(message, bot)


@dp.message(F.chat.type == "private", Command("quran"))
async def handle_quran_pm(message: Message):
    if message.from_user and message.from_user.id in banned_users:
        return
    asyncio.create_task(cmd_quran(message, bot))


@dp.message(F.chat.type == "private", Command(commands=["чтецы", "reciters"]))
async def handle_reciters_list_pm(message: Message):
    if message.from_user and message.from_user.id in banned_users:
        return
    await cmd_reciters_list(message, bot)


# Универсальный PM-хендлер: разбирает «обычный» текст без слэша.
#   • URL → автоскачивание;
#   • «.коран», «.сура», «.аят», «.найти» → запускаем соответствующую команду;
#   • «чтецы» / «список чтецов» → отвечаем списком;
#   • «.люхайдан» / «люхайдан» (любой алиас чтеца) → список доступных сур.
# Молча игнорируем всё остальное, чтобы не отвечать на болтовню в ЛС.
@dp.message(F.chat.type == "private", ~F.text.startswith("/"))
async def handle_pm_text(message: Message):
    if message.from_user and message.from_user.id in banned_users:
        return
    text = (message.text or "").strip()
    if not text:
        return

    # Авто-скачивание URL — как в business-чате, но без удаления команды.
    if extract_url(text):
        await auto_download(message, bot)
        return

    # Алиасы команд: «.коран ...» / «.найти ...» — запускаем как полноценную команду.
    cmd_name = get_command(text)
    if cmd_name == "quran":
        asyncio.create_task(cmd_quran(message, bot))
        return
    if cmd_name == "music":
        await cmd_music(message, bot)
        return
    if cmd_name == "save":
        await cmd_save(message, bot)
        return

    # Список чтецов: «чтецы», «список чтецов» (можно с эмодзи/префиксами).
    low = text.lower().strip(" .!?").strip()
    if low in ("чтецы", "список чтецов", "список", "reciters"):
        await cmd_reciters_list(message, bot)
        return

    # Алиас конкретного чтеца: «.люхайдан», «люхайдан», «Махди Аш-Шишани» и т.п.
    # Поддерживаем многословные алиасы и регистро-независимы.
    rec = _find_reciter_by_alias(text)
    if rec is None and len(text.split()) > 1:
        # Пробуем первые 1..3 слова как алиас (на случай «.люхайдан» + лишнего хвоста).
        parts = text.split()
        for n in range(min(3, len(parts)), 0, -1):
            rec = _find_reciter_by_alias(" ".join(parts[:n]))
            if rec is not None:
                break
    if rec is not None:
        await cmd_reciter_suras(message, bot, rec)
        return


@dp.message(Command("users"))
async def cmd_users(message: Message):
    if not message.from_user or message.from_user.id != ADMIN_ID:
        return
    if not connected_users:
        await message.answer("👥 Нет подключённых пользователей.")
        return
    lines = ["👥 Подключённые пользователи:\n"]
    for uid, info in connected_users.items():
        ban_mark = " 🚫" if uid in banned_users else ""
        uname = f" (@{info['username']})" if info.get("username") else ""
        lines.append(f"• {info['name']}{uname} — 🆔 {uid}{ban_mark}")
    await message.answer("\n".join(lines))


@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    if not message.from_user or message.from_user.id != ADMIN_ID:
        return
    await message.answer(
        "📊 Статистика бота:\n\n"
        f"👥 Всего подключений: {stats['connections']}\n"
        f"🗑 Перехвачено удалённых: {stats['deleted']}\n"
        f"✏️ Перехвачено изменённых: {stats['edited']}\n"
        f"🔗 Сейчас подключено: {len(connected_users)}\n"
        f"🚫 Заблокировано: {len(banned_users)}"
    )


@dp.message(Command("ban"))
async def cmd_ban(message: Message):
    if not message.from_user or message.from_user.id != ADMIN_ID:
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("Использование: /ban <ID или @username>")
        return
    target = args[1].strip().lstrip("@")
    target_id = None
    if target.lstrip("-").isdigit():
        target_id = int(target)
    else:
        for uid, info in connected_users.items():
            if info.get("username", "").lower() == target.lower():
                target_id = uid
                break
    if target_id is None:
        await message.answer(f"❌ Пользователь '{target}' не найден среди подключённых.")
        return
    if target_id == ADMIN_ID:
        await message.answer("❌ Нельзя заблокировать администратора.")
        return
    banned_users.add(target_id)
    info = connected_users.get(target_id)
    name = info["name"] if info else str(target_id)
    await message.answer(f"🚫 Пользователь {name} (ID: {target_id}) заблокирован.")
    try:
        await bot.send_message(target_id, "🚫 Вы были заблокированы администратором.")
    except Exception:
        pass


@dp.message(Command("unban"))
async def cmd_unban(message: Message):
    if not message.from_user or message.from_user.id != ADMIN_ID:
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("Использование: /unban <ID или @username>")
        return
    target = args[1].strip().lstrip("@")
    target_id = None
    if target.lstrip("-").isdigit():
        target_id = int(target)
    else:
        for uid, info in connected_users.items():
            if info.get("username", "").lower() == target.lower():
                target_id = uid
                break
    if target_id is None:
        await message.answer(f"❌ Пользователь '{target}' не найден.")
        return
    if target_id in banned_users:
        banned_users.discard(target_id)
        info = connected_users.get(target_id)
        name = info["name"] if info else str(target_id)
        await message.answer(f"✅ Пользователь {name} (ID: {target_id}) разблокирован.")
        try:
            await bot.send_message(target_id, "✅ Вы были разблокированы администратором.")
        except Exception:
            pass
    else:
        await message.answer(f"ℹ️ Пользователь {target_id} не заблокирован.")


@dp.message(Command("broadcast"))
async def handle_broadcast(message: Message):
    if not message.from_user or message.from_user.id != ADMIN_ID:
        return
    await cmd_broadcast(message, bot, connected_users)


@dp.message(Command("alias"))
async def handle_alias(message: Message):
    """Управление пользовательскими синонимами команд (только админ).

    Использование:
      /alias add .синоним команда   — добавить (синоним должен начинаться с точки)
      /alias del .синоним           — удалить (только из своих)
      /alias list                   — показать все (встроенные + свои)
    """
    if not message.from_user or message.from_user.id != ADMIN_ID:
        return

    parts = (message.text or "").split()
    sub = parts[1].lower() if len(parts) > 1 else ""

    if sub == "list":
        lines = ["<b>📑 Синонимы команд:</b>", "", "<b>Встроенные:</b>"]
        for syn, cmd in sorted(DOT_ALIASES.items()):
            lines.append(f"<code>{syn}</code> → /{cmd}")
        lines.append("")
        if custom_aliases:
            lines.append("<b>Ваши:</b>")
            for syn, cmd in sorted(custom_aliases.items()):
                lines.append(f"<code>{syn}</code> → /{cmd}")
        else:
            lines.append(
                "<i>Ваших синонимов пока нет.\n"
                "Добавьте: <code>/alias add .название команда</code></i>"
            )
        await message.answer("\n".join(lines), parse_mode="HTML")
        return

    if sub == "add":
        if len(parts) < 4:
            await message.answer(
                "Использование: <code>/alias add .синоним команда</code>\n"
                "Пример: <code>/alias add .песня music</code>",
                parse_mode="HTML",
            )
            return
        syn = parts[2].lower()
        cmd = parts[3].lower().lstrip("/")
        if not syn.startswith("."):
            await message.answer(
                "❌ Синоним обязан начинаться с точки. Без точки бот реагировал "
                "бы на обычные слова в переписке. Пример: <code>.песня</code>.",
                parse_mode="HTML",
            )
            return
        if len(syn) < 2 or " " in syn:
            await message.answer("❌ Слишком короткий синоним или содержит пробелы.", parse_mode="HTML")
            return
        if syn in DOT_ALIASES:
            await message.answer(
                f"❌ <code>{syn}</code> — встроенный синоним для /{DOT_ALIASES[syn]}, "
                "его нельзя переопределить.",
                parse_mode="HTML",
            )
            return
        if cmd not in _ALIAS_TARGETS:
            allowed = ", ".join(f"/{c}" for c in sorted(_ALIAS_TARGETS))
            await message.answer(
                f"❌ Неизвестная команда <code>/{cmd}</code>.\nДоступные:\n{allowed}",
                parse_mode="HTML",
            )
            return
        prev = custom_aliases.get(syn)
        custom_aliases[syn] = cmd
        schedule_persist()
        if prev and prev != cmd:
            await message.answer(
                f"✅ Синоним <code>{syn}</code>: было /{prev} → стало /{cmd}.",
                parse_mode="HTML",
            )
        else:
            await message.answer(
                f"✅ Синоним <code>{syn}</code> → /{cmd} добавлен.",
                parse_mode="HTML",
            )
        return

    if sub in ("del", "delete", "rm", "remove"):
        if len(parts) < 3:
            await message.answer(
                "Использование: <code>/alias del .синоним</code>",
                parse_mode="HTML",
            )
            return
        syn = parts[2].lower()
        if syn in DOT_ALIASES:
            await message.answer(
                f"❌ <code>{syn}</code> — встроенный синоним, удалить его нельзя.",
                parse_mode="HTML",
            )
            return
        if syn not in custom_aliases:
            await message.answer(
                f"ℹ️ Синонима <code>{syn}</code> нет среди ваших. "
                "Посмотреть все: <code>/alias list</code>.",
                parse_mode="HTML",
            )
            return
        cmd = custom_aliases.pop(syn)
        schedule_persist()
        await message.answer(
            f"✅ Синоним <code>{syn}</code> (был → /{cmd}) удалён.",
            parse_mode="HTML",
        )
        return

    await message.answer(
        "<b>/alias</b> — управление синонимами команд (только для админа).\n\n"
        "<code>/alias add .синоним команда</code> — добавить\n"
        "<code>/alias del .синоним</code> — удалить\n"
        "<code>/alias list</code> — показать все\n\n"
        "<b>Пример:</b> <code>/alias add .песня music</code> — теперь "
        "<code>.песня Мияги</code> делает то же что и <code>/music Мияги</code>.\n\n"
        "<i>Точка в начале синонима обязательна.</i>",
        parse_mode="HTML",
    )


async def main():
    load_persistent_state()
    asyncio.create_task(cache_cleanup_task())
    asyncio.create_task(persist_loop())
    try:
        await dp.start_polling(bot)
    finally:
        # Финальный синхронный дамп, чтобы при штатной остановке всё попало на диск.
        try:
            _do_persist_sync()
        except Exception as e:
            logging.error(f"Финальное сохранение не удалось: {e}")


if __name__ == "__main__":
    asyncio.run(main())
