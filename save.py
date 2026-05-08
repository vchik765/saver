import asyncio
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
import urllib.request
import urllib.error
from aiogram import Bot
from aiogram.types import Message, InputMediaPhoto, InputMediaVideo, FSInputFile


def _probe_video(path: str) -> dict:
    """Возвращает {'width', 'height', 'duration'} для видеофайла."""
    try:
        out = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=width,height,duration:format=duration",
                "-of", "json", path,
            ],
            capture_output=True, text=True, timeout=8,
        )
        if out.returncode != 0:
            return {}
        data = json.loads(out.stdout or "{}")
        streams = data.get("streams") or []
        fmt = data.get("format") or {}
        if not streams:
            return {}
        s = streams[0]
        w = int(s.get("width") or 0)
        h = int(s.get("height") or 0)
        dur_raw = s.get("duration") or fmt.get("duration") or 0
        try:
            d = int(float(dur_raw))
        except (TypeError, ValueError):
            d = 0
        result: dict = {}
        if w > 0:
            result["width"] = w
        if h > 0:
            result["height"] = h
        if d > 0:
            result["duration"] = d
        return result
    except Exception as e:
        logging.warning(f"[save] ffprobe не отработал для {path}: {e}")
        return {}


LOADING_TEXT = '<tg-emoji emoji-id="5443127283898405358">⏳</tg-emoji>Загружаю…'

MAX_FILE_SIZE = 49 * 1024 * 1024  # 49 MB

SOCIAL_URL_PATTERN = re.compile(
    r'https?://(?:www\.)?'
    r'(?:tiktok\.com|vm\.tiktok\.com|vt\.tiktok\.com'
    r'|instagram\.com|instagr\.am'
    r'|youtube\.com|youtu\.be'
    r'|vk\.com|vkvideo\.ru'
    r'|twitter\.com|x\.com'
    r'|facebook\.com|fb\.com|fb\.watch'
    r'|reddit\.com|redd\.it'
    r'|pinterest\.com|pin\.it'
    r'|snapchat\.com'
    r'|twitch\.tv'
    r'|dailymotion\.com'
    r'|rumble\.com'
    r')[^\s<>"\']+'
)


def extract_url(text: str) -> str | None:
    match = SOCIAL_URL_PATTERN.search(text)
    if match:
        return match.group(0).rstrip('.,;!?)')
    return None


def is_image(path: str) -> bool:
    return path.lower().endswith(('.jpg', '.jpeg', '.png', '.webp'))


def is_video(path: str) -> bool:
    return path.lower().endswith(('.mp4', '.mov', '.avi', '.mkv', '.webm', '.m4v'))


def get_valid_files(tmpdir: str) -> list[str]:
    result = []
    for f in sorted(os.listdir(tmpdir)):
        full = os.path.join(tmpdir, f)
        if not os.path.isfile(full):
            continue
        if not (is_image(f) or is_video(f)):
            continue
        if os.path.getsize(full) > MAX_FILE_SIZE:
            logging.warning(f"Файл слишком большой, пропускаем: {f}")
            continue
        if os.path.getsize(full) == 0:
            logging.warning(f"Файл пустой, пропускаем: {f}")
            continue
        result.append(full)
    return result


# ─────────────────────────────────────────────
#  СПОСОБ 1: yt-dlp (основной)
# ─────────────────────────────────────────────

async def _download_ytdlp(url: str) -> tuple[list[str], str]:
    """Скачивает медиа через yt-dlp. Возвращает (файлы, tmpdir)."""
    tmpdir = tempfile.mkdtemp()
    ydl_opts = {
        'outtmpl': os.path.join(tmpdir, '%(autonumber)03d.%(ext)s'),
        # Явно требуем видео+аудио вместе; без аудио-стрима не берём
        'format': (
            'bestvideo[ext=mp4][filesize<49M]+bestaudio[ext=m4a]/'
            'bestvideo[ext=mp4][filesize<49M]+bestaudio/'
            'bestvideo[filesize<49M]+bestaudio/'
            'best[filesize<49M]/best'
        ),
        'merge_output_format': 'mp4',
        'quiet': True,
        'no_warnings': True,
        # ignoreerrors=False — чтобы не глотать ошибки аудио-мержа молча
        'ignoreerrors': False,
        'noplaylist': False,
        'max_filesize': MAX_FILE_SIZE,
        'concurrent_fragment_downloads': 4,
        'retries': 3,
        'fragment_retries': 3,
        'http_headers': {
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/124.0.0.0 Safari/537.36'
            ),
            'Accept-Language': 'en-US,en;q=0.9',
        },
        'extractor_args': {
            'tiktok': {'webpage_download': True},
            'instagram': {'extract_flat': False},
        },
        'postprocessors': [
            {
                'key': 'FFmpegVideoConvertor',
                'preferedformat': 'mp4',
            },
        ],
    }

    loop = asyncio.get_event_loop()

    def _download():
        try:
            import yt_dlp
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])
        except Exception as e:
            logging.warning(f"[yt-dlp] ошибка: {e}")

    await loop.run_in_executor(None, _download)
    files = get_valid_files(tmpdir)
    return files, tmpdir


# ─────────────────────────────────────────────
#  СПОСОБ 2: cobalt.tools (fallback, без ключа)
# ─────────────────────────────────────────────

COBALT_API = "https://api.cobalt.tools/"
COBALT_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
}


def _http_download_file(dl_url: str, dest: str) -> bool:
    """Скачивает файл по URL в dest. Возвращает True при успехе."""
    try:
        req = urllib.request.Request(
            dl_url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                )
            },
        )
        with urllib.request.urlopen(req, timeout=60) as resp, open(dest, "wb") as f:
            while True:
                chunk = resp.read(1024 * 256)
                if not chunk:
                    break
                f.write(chunk)
        size = os.path.getsize(dest)
        if size == 0:
            logging.warning(f"[cobalt] скачан пустой файл: {dest}")
            return False
        if size > MAX_FILE_SIZE:
            logging.warning(f"[cobalt] файл слишком большой ({size}): {dest}")
            return False
        return True
    except Exception as e:
        logging.warning(f"[cobalt] ошибка скачивания {dl_url}: {e}")
        return False


def _guess_ext_from_url(url: str) -> str:
    """Пытается угадать расширение из URL, по умолчанию mp4."""
    url_path = url.split("?")[0].lower()
    for ext in (".mp4", ".mov", ".webm", ".mkv", ".jpg", ".jpeg", ".png", ".webp"):
        if url_path.endswith(ext):
            return ext
    return ".mp4"


def _cobalt_request(url: str) -> dict | None:
    """Синхронный POST к cobalt API. Возвращает JSON или None."""
    import json as _json
    payload = _json.dumps({
        "url": url,
        "videoQuality": "1080",
        "downloadMode": "auto",
        "filenameStyle": "basic",
    }).encode()
    try:
        req = urllib.request.Request(
            COBALT_API,
            data=payload,
            headers=COBALT_HEADERS,
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            return _json.loads(resp.read())
    except Exception as e:
        logging.warning(f"[cobalt] API запрос не удался: {e}")
        return None


async def _download_cobalt(url: str) -> tuple[list[str], str]:
    """Скачивает медиа через cobalt.tools. Возвращает (файлы, tmpdir)."""
    tmpdir = tempfile.mkdtemp()
    loop = asyncio.get_event_loop()

    data = await loop.run_in_executor(None, _cobalt_request, url)
    if not data:
        return [], tmpdir

    status = data.get("status", "")
    logging.info(f"[cobalt] статус: {status}, ответ: {str(data)[:200]}")

    download_urls: list[tuple[str, str]] = []  # (dl_url, filename)

    if status in ("tunnel", "redirect"):
        dl_url = data.get("url") or data.get("tunnel")
        if dl_url:
            ext = _guess_ext_from_url(dl_url)
            download_urls.append((dl_url, f"001{ext}"))

    elif status == "picker":
        for i, item in enumerate(data.get("picker", []), 1):
            item_url = item.get("url") or item.get("tunnel")
            if item_url:
                ext = _guess_ext_from_url(item_url)
                download_urls.append((item_url, f"{i:03d}{ext}"))

    else:
        err = data.get("error", {})
        logging.warning(f"[cobalt] неизвестный статус или ошибка: {status} / {err}")
        return [], tmpdir

    if not download_urls:
        return [], tmpdir

    # Скачиваем все файлы параллельно через executor
    def _download_all():
        results = []
        for dl_url, fname in download_urls:
            dest = os.path.join(tmpdir, fname)
            ok = _http_download_file(dl_url, dest)
            if ok:
                results.append(dest)
        return results

    downloaded = await loop.run_in_executor(None, _download_all)
    files = [f for f in downloaded if os.path.exists(f) and os.path.getsize(f) > 0]
    return files, tmpdir


# ─────────────────────────────────────────────
#  Основная функция скачивания (с fallback)
# ─────────────────────────────────────────────

async def download_media(url: str) -> tuple[list[str], str]:
    """
    Пробует yt-dlp, при неудаче — cobalt.tools.
    Возвращает (список файлов, tmpdir). Caller отвечает за удаление tmpdir.
    """
    # Попытка 1: yt-dlp
    logging.info(f"[save] yt-dlp: {url}")
    files, tmpdir = await _download_ytdlp(url)
    if files:
        logging.info(f"[save] yt-dlp успех: {len(files)} файл(ов)")
        return files, tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)
    logging.info(f"[save] yt-dlp не дал файлов, пробуем cobalt.tools")

    # Попытка 2: cobalt.tools
    files, tmpdir = await _download_cobalt(url)
    if files:
        logging.info(f"[save] cobalt успех: {len(files)} файл(ов)")
    else:
        logging.warning(f"[save] оба способа не дали файлов для {url}")
    return files, tmpdir


async def _delete_messages_safe(bot: Bot, chat_id: int, msg_ids: list[int], bc_id: str):
    if not msg_ids:
        return
    try:
        await bot.delete_messages(
            chat_id=chat_id,
            message_ids=msg_ids,
            business_connection_id=bc_id,
        )
        return
    except Exception as e:
        logging.warning(f"Пакетное удаление {msg_ids} не удалось: {e}, пробую по одному")

    for mid in msg_ids:
        try:
            await bot.delete_messages(
                chat_id=chat_id,
                message_ids=[mid],
                business_connection_id=bc_id,
            )
        except Exception as e:
            logging.warning(f"Не удалось удалить сообщение {mid}: {e}")


async def _send_media_files(bot: Bot, chat_id: int, files: list[str], bc_id: str):
    if len(files) == 1:
        f = files[0]
        if is_video(f):
            meta = _probe_video(f)
            await bot.send_video(
                chat_id, FSInputFile(f),
                business_connection_id=bc_id,
                supports_streaming=True,
                **meta,
            )
        else:
            await bot.send_photo(chat_id, FSInputFile(f), business_connection_id=bc_id)
    else:
        chunks = [files[i:i + 10] for i in range(0, len(files), 10)]
        for chunk in chunks:
            media = []
            for f in chunk:
                if is_video(f):
                    meta = _probe_video(f)
                    media.append(InputMediaVideo(
                        media=FSInputFile(f),
                        supports_streaming=True,
                        **meta,
                    ))
                else:
                    media.append(InputMediaPhoto(media=FSInputFile(f)))
            await bot.send_media_group(chat_id, media=media, business_connection_id=bc_id)
            await asyncio.sleep(0.5)


async def _send_temp_error(bot: Bot, chat_id: int, text: str, bc_id: str, delay: float = 5.0):
    try:
        sent = await bot.send_message(chat_id, text, business_connection_id=bc_id)
        await asyncio.sleep(delay)
        await _delete_messages_safe(bot, chat_id, [sent.message_id], bc_id)
    except Exception as e:
        logging.warning(f"Ошибка временного сообщения об ошибке: {e}")


async def _do_download_and_send(
    bot: Bot,
    chat_id: int,
    url: str,
    bc_id: str,
    cleanup_msg_ids: list[int],
):
    loading_msg_id = None
    try:
        sent = await bot.send_message(
            chat_id,
            LOADING_TEXT,
            parse_mode="HTML",
            business_connection_id=bc_id,
        )
        loading_msg_id = sent.message_id
    except Exception as e:
        logging.warning(f"Не удалось отправить Загружаю: {e}")

    files, tmpdir = await download_media(url)

    if not files:
        if loading_msg_id:
            await _delete_messages_safe(bot, chat_id, [loading_msg_id], bc_id)
        await _send_temp_error(bot, chat_id, "❌ Не удалось скачать медиа по этой ссылке.", bc_id)
        shutil.rmtree(tmpdir, ignore_errors=True)
        return

    sent_ok = False
    try:
        await _send_media_files(bot, chat_id, files, bc_id)
        sent_ok = True
    except Exception as e:
        logging.error(f"Ошибка отправки медиа: {e}")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    to_delete: list[int] = []
    if loading_msg_id:
        to_delete.append(loading_msg_id)
    if sent_ok:
        to_delete.extend(cleanup_msg_ids)
    if to_delete:
        await _delete_messages_safe(bot, chat_id, to_delete, bc_id)

    if not sent_ok:
        await _send_temp_error(bot, chat_id, "❌ Не удалось отправить медиафайл.", bc_id)


async def cmd_save(message: Message, bot: Bot):
    url = None

    if message.reply_to_message:
        src_text = (
            message.reply_to_message.text
            or message.reply_to_message.caption
            or ""
        )
        url = extract_url(src_text)

    if not url:
        parts = (message.text or "").split(maxsplit=1)
        if len(parts) > 1:
            url = extract_url(parts[1])

    bc_id = message.business_connection_id
    chat_id = message.chat.id

    if not url:
        await _delete_messages_safe(bot, chat_id, [message.message_id], bc_id)
        await _send_temp_error(
            bot, chat_id,
            "❌ Ссылка не найдена. Ответь командой /save на сообщение со ссылкой.",
            bc_id,
        )
        return

    cleanup_ids = [message.message_id]
    if message.reply_to_message:
        cleanup_ids.append(message.reply_to_message.message_id)

    await _do_download_and_send(bot, chat_id, url, bc_id, cleanup_ids)


async def auto_download(message: Message, bot: Bot):
    url = extract_url(message.text or message.caption or "")
    if not url:
        return

    bc_id = message.business_connection_id
    chat_id = message.chat.id

    cleanup_ids = [message.message_id]
    await _do_download_and_send(bot, chat_id, url, bc_id, cleanup_ids)


async def cmd_broadcast(message: Message, bot: Bot, connected_users: dict):
    reply = message.reply_to_message
    broadcast_text = None

    if not reply:
        parts = (message.text or "").split(maxsplit=1)
        if len(parts) < 2:
            await message.answer(
                "Использование:\n"
                "• /broadcast <текст> — разослать текст\n"
                "• Ответь на сообщение командой /broadcast — разослать то сообщение"
            )
            return
        broadcast_text = parts[1]

    if not connected_users:
        await message.answer("👥 Нет подключённых пользователей для рассылки.")
        return

    success = 0
    fail = 0

    for uid in list(connected_users.keys()):
        try:
            if reply:
                await bot.copy_message(
                    chat_id=uid,
                    from_chat_id=reply.chat.id,
                    message_id=reply.message_id,
                )
            else:
                await bot.send_message(uid, broadcast_text)
            success += 1
        except Exception as e:
            logging.warning(f"Ошибка рассылки пользователю {uid}: {e}")
            fail += 1
        await asyncio.sleep(0.05)

    await message.answer(
        f"✅ Рассылка завершена:\n"
        f"✓ Доставлено: {success}\n"
        f"✗ Ошибок: {fail}"
    )
