"""
info.py — ОСИНТ модуль для команды /id (v2)

Все результаты отправляются в ЛС владельцу бота (owner_id),
НЕ в чат с собеседником.

Что собирает:
  1. Базовая инфа (ID, username, имя, язык, флаги, bio)
  2. Аватарка профиля
  3. Проверка username на 11 платформах (GitHub, VK, TikTok, Telegram, Reddit, ...)
  4. Предполагаемые email-адреса (по шаблону username@домен) для ручной проверки
  5. Ссылки на OSINT-инструменты для ручного дорасследования
"""
import asyncio
import logging
import re
import urllib.request
import urllib.error
from io import BytesIO

from aiogram import Bot
from aiogram.types import Message, BufferedInputFile
from cmds import delete_command

log = logging.getLogger(__name__)

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# ── Таблица платформ ─────────────────────────────────────────────────────────
# (название, url, маркер_существования, маркер_отсутствия)
# Приоритет: если маркер_отсутствия найден в body — пользователя нет.
# Если маркер_существования найден — есть. Иначе судим по HTTP статусу.
_PLATFORMS: list[tuple[str, str, str | None, str | None]] = [
    (
        "Telegram",
        "https://t.me/{}",
        "tgme_page_title",
        "tgme_page_extra",   # страница-заглушка "нет такого пользователя" содержит этот class
    ),
    (
        "GitHub",
        "https://api.github.com/users/{}",   # JSON API — самый надёжный
        '"login"',
        '"message":"Not Found"',
    ),
    (
        "VKontakte",
        "https://vk.com/{}",
        "og:url",
        "Страница не найдена",
    ),
    (
        "Reddit",
        "https://www.reddit.com/user/{}/about.json",
        '"name"',
        '"error": 404',
    ),
    (
        "TikTok",
        "https://www.tiktok.com/@{}",
        '"followerCount"',
        "Couldn\\'t find this account",
    ),
    (
        "YouTube",
        "https://www.youtube.com/@{}",
        '"channelId"',
        None,
    ),
    (
        "Twitch",
        "https://www.twitch.tv/{}",
        '"channel":',
        "Sorry. Unless you",
    ),
    (
        "Pinterest",
        "https://www.pinterest.com/{}/",
        "pinterestapp://",
        "Sorry! We couldn",
    ),
    (
        "Steam",
        "https://steamcommunity.com/id/{}/",
        "og:title",
        "The specified profile could not be found",
    ),
    (
        "Instagram",
        "https://www.instagram.com/{}/",
        '"username"',
        "Sorry, this page",
    ),
    (
        "Twitter/X",
        "https://x.com/{}",
        "og:title",
        "This account doesn",
    ),
]

# Email-домены для угадывания по юзернейму
_EMAIL_DOMAINS = ["@gmail.com", "@mail.ru", "@yandex.ru", "@icloud.com", "@outlook.com"]

# OSINT-инструменты для ручного поиска
_OSINT_TOOLS = [
    ("Maigret",    "https://github.com/soxoj/maigret"),
    ("Sherlock",   "https://github.com/sherlock-project/sherlock"),
    ("HIBP",       "https://haveibeenpwned.com/"),
    ("Epieos",     "https://epieos.com/"),
    ("IntelTech",  "https://intelx.io/"),
]


def _esc(text: str) -> str:
    """Экранирование для HTML."""
    return (
        text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
    )


async def _check_platform(
    name: str,
    url_tpl: str,
    exist_marker: str | None,
    notfound_marker: str | None,
    username: str,
) -> tuple[str, bool, str]:
    """Проверяет наличие username на платформе. Возвращает (name, found, url)."""
    final_url = url_tpl.format(username)
    loop = asyncio.get_event_loop()

    def _fetch() -> tuple[int, str]:
        try:
            req = urllib.request.Request(
                final_url,
                headers={"User-Agent": _UA, "Accept-Language": "en-US,en;q=0.9"},
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status, resp.read(16384).decode("utf-8", errors="ignore")
        except urllib.error.HTTPError as e:
            return e.code, ""
        except Exception:
            return 0, ""

    try:
        status, body = await loop.run_in_executor(None, _fetch)
    except Exception:
        return name, False, final_url

    if status == 404:
        return name, False, final_url
    if status == 0:
        return name, False, final_url

    if notfound_marker and notfound_marker.lower() in body.lower():
        return name, False, final_url

    if exist_marker and exist_marker.lower() in body.lower():
        return name, True, final_url

    # Нет явных маркеров — судим по статусу
    return name, (status == 200), final_url


async def cmd_id(message: Message, bot: Bot, owner_id: int) -> None:
    """Основная функция: собирает ОСИНТ и отправляет отчёт в ЛС владельца."""

    # Определяем цель: reply → тот пользователь, иначе — собеседник
    if message.reply_to_message and message.reply_to_message.from_user:
        user = message.reply_to_message.from_user
    else:
        user = message.from_user

    # Удаляем команду из чата немедленно
    await delete_command(message, bot)

    if not user:
        await bot.send_message(owner_id, "❌ Не удалось определить пользователя.")
        return

    uid        = user.id
    full_name  = _esc(user.full_name or "—")
    username   = user.username or ""
    lang       = user.language_code or "—"

    # ── Флаги ────────────────────────────────────────────────────────────────
    flags: list[str] = []
    if getattr(user, "is_bot", False):       flags.append("🤖 Бот")
    if getattr(user, "is_premium", False):   flags.append("⭐️ Premium")
    if getattr(user, "is_verified", False):  flags.append("✅ Верифицирован")
    if getattr(user, "is_scam", False):      flags.append("⚠️ Scam")
    if getattr(user, "is_fake", False):      flags.append("⚠️ Fake")
    if getattr(user, "is_support", False):   flags.append("🛟 Support")

    # ── Расширенная инфа через get_chat ──────────────────────────────────────
    bio = ""
    dc_id = ""
    try:
        chat_obj = await bot.get_chat(uid)
        bio   = _esc(getattr(chat_obj, "bio", "") or "")
        dc_id = str(getattr(chat_obj, "dc_id", "") or "")
    except Exception:
        pass

    uname_line   = f"@{username}" if username else "нет"
    profile_url  = f"https://t.me/{username}" if username else f"tg://user?id={uid}"

    # ── Блок 1: базовые данные (отправляем сразу) ─────────────────────────────
    basic_parts = [
        "👤 <b>ОСИНТ — Telegram</b>",
        "",
        f"🆔 <b>ID:</b> <code>{uid}</code>",
        f"📛 <b>Имя:</b> {full_name}",
        f"🔗 <b>Username:</b> {uname_line}",
        f"🌐 <b>Язык интерфейса:</b> {lang}",
    ]
    if dc_id:
        basic_parts.append(f"🗄 <b>Дата-центр:</b> DC{dc_id}")
    if flags:
        basic_parts.append(f"🏷 <b>Флаги:</b> {' · '.join(flags)}")
    else:
        basic_parts.append("🏷 <b>Флаги:</b> обычный аккаунт")
    if bio:
        basic_parts.append(f"📝 <b>Bio:</b> {bio[:400]}")
    basic_parts.append(f"\n🔍 <a href=\"{profile_url}\">Открыть профиль</a>")

    await bot.send_message(
        owner_id,
        "\n".join(basic_parts),
        parse_mode="HTML",
        disable_web_page_preview=True,
    )

    # ── Аватарка ─────────────────────────────────────────────────────────────
    try:
        photos = await bot.get_user_profile_photos(uid, limit=1)
        if photos.total_count > 0:
            biggest = photos.photos[0][-1]
            buf = BytesIO()
            await bot.download(biggest.file_id, destination=buf)
            buf.seek(0)
            await bot.send_photo(
                owner_id,
                BufferedInputFile(buf.read(), "avatar.jpg"),
                caption=(
                    f"📸 Аватарка: {full_name}\n"
                    f"Всего фото в профиле: {photos.total_count}"
                ),
            )
        else:
            await bot.send_message(owner_id, "📸 Аватарка не установлена.")
    except Exception as e:
        log.debug(f"[info] avatar error: {e}")

    # ── Если нет username — платформы не проверяем ───────────────────────────
    if not username:
        await bot.send_message(
            owner_id,
            "ℹ️ <b>Username не задан</b> — проверка платформ и email-адресов недоступна.",
            parse_mode="HTML",
        )
        return

    # ── Блок 2: проверка платформ (параллельно) ───────────────────────────────
    wait_msg = await bot.send_message(
        owner_id,
        f"🔎 Проверяю <b>@{_esc(username)}</b> на {len(_PLATFORMS)} платформах…",
        parse_mode="HTML",
    )

    tasks = [
        _check_platform(p_name, p_url, p_exist, p_notfound, username)
        for p_name, p_url, p_exist, p_notfound in _PLATFORMS
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    found_lines:    list[str] = []
    notfound_lines: list[str] = []

    for r in results:
        if isinstance(r, Exception):
            continue
        p_name, found, p_url = r
        if found:
            found_lines.append(f"  ✅ <a href=\"{p_url}\">{p_name}</a>")
        else:
            notfound_lines.append(f"  ❌ {p_name}")

    plat_parts = [f"🌐 <b>Платформы (@{_esc(username)}):</b>"]
    if found_lines:
        plat_parts.append("")
        plat_parts.append("<b>Найден:</b>")
        plat_parts.extend(found_lines)
    if notfound_lines:
        plat_parts.append("")
        plat_parts.append("<b>Не найден / нет данных:</b>")
        plat_parts.extend(notfound_lines)
    if not found_lines and not notfound_lines:
        plat_parts.append("⚠️ Все проверки не дали результата (возможно, блокировка).")

    try:
        await bot.edit_message_text(
            "\n".join(plat_parts),
            chat_id=owner_id,
            message_id=wait_msg.message_id,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception:
        await bot.send_message(
            owner_id,
            "\n".join(plat_parts),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )

    # ── Блок 3: предполагаемые email ─────────────────────────────────────────
    u_lower = username.lower()
    email_guesses = [f"<code>{u_lower}{d}</code>" for d in _EMAIL_DOMAINS]
    email_text = (
        f"📧 <b>Предполагаемые email-адреса</b> (ручная проверка):\n\n"
        + "\n".join(email_guesses)
        + "\n\n"
        "💡 Проверить утечки: <a href=\"https://haveibeenpwned.com/\">HaveIBeenPwned</a> · "
        "<a href=\"https://epieos.com/\">Epieos</a>"
    )
    await bot.send_message(
        owner_id,
        email_text,
        parse_mode="HTML",
        disable_web_page_preview=True,
    )

    # ── Блок 4: ОСИНТ-инструменты ────────────────────────────────────────────
    tool_lines = ["🛠 <b>ОСИНТ-инструменты для углублённого поиска:</b>", ""]
    for t_name, t_url in _OSINT_TOOLS:
        tool_lines.append(f"  • <a href=\"{t_url}\">{t_name}</a>")
    tool_lines.append("")
    tool_lines.append(
        f"🔎 <a href=\"https://maigret.readthedocs.io/en/latest/\">Maigret</a> — "
        f"поиск <b>@{_esc(username)}</b> на 1500+ сайтах локально"
    )
    await bot.send_message(
        owner_id,
        "\n".join(tool_lines),
        parse_mode="HTML",
        disable_web_page_preview=True,
    )
