"""Команда /music — поиск и отправка музыки из нескольких источников.

Использование:
  /music <запрос>            — поиск по тексту: VK + YouTube + SoundCloud (параллельно)
  /music vk <запрос>         — только VK Музыка
  /music yt <запрос>         — только YouTube
  /music sc <запрос>         — только SoundCloud
  /music                     — reply на голосовое/аудио/видео:
                               распознавание через Shazam, потом поиск.

Источники:
  • VK Музыка    — api.vk.com/method/audio.search (нужен env VK_TOKEN)
  • YouTube      — yt-dlp ytsearch1:
  • SoundCloud   — yt-dlp scsearch1:
  • Shazam       — shazamio.recognize() для определения трека по отрывку

Все источники запускаются ПАРАЛЛЕЛЬНО, побеждает первый, кто вернёт трек.
Бот всегда отправляет ОДИН результат.
"""
import asyncio
import logging
import os
import re
import shutil
import tempfile

from aiogram import Bot
from aiogram.types import Message, BufferedInputFile

# Лимиты
MAX_DURATION_SEC = 15 * 60          # 15 минут — отсекаем длинные миксы
MAX_OUTPUT_BYTES = 49 * 1024 * 1024  # ~49 МБ — лимит Bot API
PER_SOURCE_TIMEOUT = 60              # сек на каждый источник
SHAZAM_TIMEOUT = 30                  # сек на распознавание
SHAZAM_INPUT_LIMIT = 10 * 1024 * 1024  # 10 МБ — больше Shazam-у не нужно

# Источники в порядке приоритета (если пользователь не указал явно).
# Префикс "vk:" — наш собственный маркер (не yt-dlp), означает «ходить в VK API».
VK_SOURCE = "vk:"
DEFAULT_SOURCES = ("vk:", "ytsearch1:", "scsearch1:")
SOURCE_LABELS = {
    "vk:": "VK Музыка",
    "ytsearch1:": "YouTube",
    "scsearch1:": "SoundCloud",
}


def _has_ffmpeg() -> bool:
    return shutil.which("ffmpeg") is not None


def _has_ytdlp() -> bool:
    return shutil.which("yt-dlp") is not None


def _sanitize_filename(name: str) -> str:
    name = re.sub(r"[\x00-\x1f<>:\"/\\|?*]", "", name).strip()
    return name[:120] if name else "audio"


def _split_artist_title(raw_title: str) -> tuple[str, str]:
    """Из строки YouTube-названия вытаскивает (исполнитель, название)."""
    if not raw_title:
        return "", ""
    clean = re.sub(
        r"\s*[\(\[](official(\s+(music\s+)?(video|audio|lyric.?\s*video))?|"
        r"music\s+video|lyrics?(\s+video)?|audio|hd|hq|4k|"
        r"премьера(\s+клипа)?|клип|original\s+mix|extended)\s*[\)\]]",
        "",
        raw_title,
        flags=re.IGNORECASE,
    ).strip(" -—–|")
    for sep in (" - ", " — ", " – ", " | "):
        if sep in clean:
            left, right = clean.split(sep, 1)
            return left.strip(), right.strip()
    return "", clean


async def _run_subprocess(args: list[str], timeout: int) -> tuple[int, bytes, bytes]:
    """Запускает процесс. При таймауте или отмене — гарантированно убивает его."""
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        return proc.returncode or 0, stdout or b"", stderr or b""
    except (asyncio.TimeoutError, asyncio.CancelledError):
        # Источник отменили (другой нашёл первым) или таймаут — убиваем yt-dlp.
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        try:
            await proc.wait()
        except Exception:
            pass
        raise


async def _send_temp(
    bot: Bot,
    chat_id: int,
    text: str,
    bc_id: str | None,
    parse_mode: str | None = None,
) -> int | None:
    try:
        sent = await bot.send_message(
            chat_id,
            text,
            business_connection_id=bc_id,
            parse_mode=parse_mode,
        )
        return sent.message_id
    except Exception as e:
        logging.warning(f"[music] не удалось отправить статус: {e}")
        return None


async def _delete_temp(bot: Bot, chat_id: int, mid: int | None, bc_id: str | None):
    if not mid:
        return
    try:
        if bc_id:
            await bot.delete_business_messages(
                business_connection_id=bc_id,
                message_ids=[mid],
            )
        else:
            await bot.delete_message(chat_id=chat_id, message_id=mid)
    except Exception:
        pass


async def _edit_temp(
    bot: Bot,
    chat_id: int,
    mid: int | None,
    bc_id: str | None,
    text: str,
    parse_mode: str | None = None,
):
    if not mid:
        return
    try:
        await bot.edit_message_text(
            text=text,
            chat_id=chat_id,
            message_id=mid,
            business_connection_id=bc_id,
            parse_mode=parse_mode,
        )
    except Exception:
        # Если редактирование не вышло — не критично, статус просто не обновится.
        pass


# Кастомные тексты-статусы для /music. Premium-эмодзи требует HTML parse_mode.
SEARCH_STATUS = '<tg-emoji emoji-id="5346074681004801565">🔎</tg-emoji>| <b>Ищу песню</b>'
NOT_FOUND_STATUS = '<tg-emoji emoji-id="5208647293879721534">❌</tg-emoji>| <b>Ничего не найдено</b>'


def _parse_metadata_line(stdout: bytes) -> dict:
    """Парсит вывод --print after_move:%(title)s\\t%(uploader)s\\t..."""
    info_line = ""
    for line in stdout.decode("utf-8", errors="ignore").splitlines():
        if line.strip():
            info_line = line.strip()
    fields = info_line.split("\t") if info_line else []

    def f(i: int) -> str:
        return fields[i] if len(fields) > i and fields[i] != "NA" else ""

    try:
        duration_int = int(float(fields[4])) if len(fields) > 4 and fields[4] != "NA" else 0
    except Exception:
        duration_int = 0

    return {
        "title": f(0),
        "uploader": f(1),
        "artist": f(2),
        "track": f(3),
        "duration": duration_int,
        "filepath": fields[5] if len(fields) > 5 else "",
    }


async def _try_vk(query: str, tmpdir: str) -> dict | None:
    """Ищет трек в VK Музыке через api.vk.com и скачивает его (mp3 / HLS) ffmpeg-ом.

    Требует переменную окружения VK_TOKEN (Kate Mobile / VK Admin токен с правом audio).
    Возвращает dict в том же формате, что и _try_source, либо None.
    """
    token = os.environ.get("VK_TOKEN", "").strip()
    if not token:
        logging.info("[music] VK_TOKEN не задан — пропускаю VK")
        return None

    try:
        import aiohttp  # уже зависимость aiogram
    except ImportError:
        logging.warning("[music] aiohttp не установлен — пропускаю VK")
        return None

    api_url = "https://api.vk.com/method/audio.search"
    params = {
        "q": query,
        "count": "5",
        "auto_complete": "1",
        "sort": "2",            # по популярности
        "access_token": token,
        "v": "5.131",
    }

    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(api_url, params=params) as r:
                data = await r.json(content_type=None)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logging.warning(f"[music] VK API ошибка сети: {e}")
        return None

    if "error" in data:
        err = data.get("error") or {}
        logging.warning(
            f"[music] VK API error {err.get('error_code')}: {err.get('error_msg')}"
        )
        return None

    items = (data.get("response") or {}).get("items") or []
    # Берём первый элемент с непустым url (некоторые треки бывают без url —
    # удалены правообладателем).
    track = next((it for it in items if (it.get("url") or "").strip()), None)
    if not track:
        logging.info(f"[music] VK ничего не нашёл по '{query}'")
        return None

    url = track["url"]
    artist = (track.get("artist") or "").strip()
    title = (track.get("title") or "").strip()
    duration = int(track.get("duration") or 0)

    if duration and duration > MAX_DURATION_SEC:
        logging.info(f"[music] VK трек слишком длинный: {duration}s")
        return None

    out_path = os.path.join(tmpdir, "track_VK.mp3")

    # ffmpeg тянет и прямой mp3, и HLS (.m3u8) — VK выдаёт оба формата.
    args = [
        "ffmpeg", "-y",
        "-loglevel", "error",
        "-protocol_whitelist", "file,http,https,tcp,tls,crypto,pipe",
        "-i", url,
        "-vn",
        "-c:a", "libmp3lame",
        "-b:a", "192k",
        "-id3v2_version", "3",
        "-metadata", f"title={title}",
        "-metadata", f"artist={artist}",
        out_path,
    ]

    try:
        rc, _stdout, stderr = await _run_subprocess(args, timeout=PER_SOURCE_TIMEOUT)
    except asyncio.TimeoutError:
        logging.warning("[music] VK ffmpeg таймаут")
        return None

    if rc != 0 or not os.path.exists(out_path) or os.path.getsize(out_path) < 10_000:
        if stderr:
            logging.warning(
                f"[music] VK ffmpeg failed (rc={rc}): "
                f"{stderr.decode('utf-8', errors='ignore')[-200:]}"
            )
        return None

    if os.path.getsize(out_path) > MAX_OUTPUT_BYTES:
        logging.info("[music] VK трек больше лимита 49 МБ")
        return None

    return {
        "title": title,
        "uploader": artist,
        "artist": artist,
        "track": title,
        "duration": duration,
        "filepath": out_path,
    }


async def _try_source(query: str, source_prefix: str, tmpdir: str) -> dict | None:
    """Пробует один источник. Возвращает dict с метаданными или None."""
    out_template = os.path.join(tmpdir, f"track_{SOURCE_LABELS[source_prefix]}.%(ext)s")
    args = [
        "yt-dlp",
        f"{source_prefix}{query}",
        "-x",
        "--audio-format", "mp3",
        "--audio-quality", "0",
        "--no-playlist",
        "--no-warnings",
        "--quiet",
        "--no-progress",
        "--max-filesize", str(MAX_OUTPUT_BYTES),
        "--match-filter", f"duration < {MAX_DURATION_SEC}",
        "--print",
        "after_move:%(title)s\t%(uploader)s\t%(artist)s\t%(track)s\t%(duration)s\t%(filepath)s",
        "-o", out_template,
    ]
    try:
        rc, stdout, stderr = await _run_subprocess(args, timeout=PER_SOURCE_TIMEOUT)
    except asyncio.TimeoutError:
        logging.warning(f"[music] таймаут источника {source_prefix}")
        return None

    if rc != 0:
        err_text = stderr.decode("utf-8", errors="ignore")[-300:] if stderr else ""
        logging.info(f"[music] {source_prefix} не нашёл (rc={rc}): {err_text[:200]}")
        return None

    meta = _parse_metadata_line(stdout)
    fp = meta["filepath"]
    if not fp or not os.path.exists(fp):
        # ищем mp3 в tmpdir вручную
        for f in os.listdir(tmpdir):
            if f.lower().endswith(".mp3") and SOURCE_LABELS[source_prefix] in f:
                fp = os.path.join(tmpdir, f)
                meta["filepath"] = fp
                break

    if not fp or not os.path.exists(fp) or os.path.getsize(fp) == 0:
        return None
    return meta


async def _shazam_recognize(file_path: str) -> tuple[str, str] | None:
    """Распознаёт трек через Shazam. Возвращает (artist, title) или None."""
    try:
        from shazamio import Shazam
    except ImportError:
        logging.warning("[music] shazamio не установлен")
        return None

    try:
        shazam = Shazam()
        result = await asyncio.wait_for(
            shazam.recognize(file_path),
            timeout=SHAZAM_TIMEOUT,
        )
    except AttributeError:
        # Старая версия shazamio с recognize_song
        try:
            from shazamio import Shazam as Sh2
            shazam = Sh2()
            result = await asyncio.wait_for(
                shazam.recognize_song(file_path),
                timeout=SHAZAM_TIMEOUT,
            )
        except Exception as e:
            logging.warning(f"[music] shazam (старый API): {e}")
            return None
    except asyncio.TimeoutError:
        logging.warning("[music] shazam timeout")
        return None
    except Exception as e:
        logging.warning(f"[music] shazam error: {e}")
        return None

    track = (result or {}).get("track") or {}
    title = (track.get("title") or "").strip()
    artist = (track.get("subtitle") or "").strip()
    if not title:
        return None
    return artist, title


async def _download_telegram_file(bot: Bot, file_id: str, dst_path: str) -> bool:
    """Скачивает файл из Telegram во временный файл."""
    try:
        f = await bot.get_file(file_id)
        if f.file_size and f.file_size > SHAZAM_INPUT_LIMIT:
            logging.warning(f"[music] файл для Shazam слишком большой: {f.file_size}")
            return False
        await bot.download_file(f.file_path, destination=dst_path)
        return os.path.exists(dst_path) and os.path.getsize(dst_path) > 0
    except Exception as e:
        logging.warning(f"[music] download_file: {e}")
        return False


def _pick_audio_source(reply: Message) -> str | None:
    """Возвращает file_id из reply, если в нём есть аудио/видео-источник."""
    if not reply:
        return None
    if reply.voice:
        return reply.voice.file_id
    if reply.audio:
        return reply.audio.file_id
    if reply.video_note:
        return reply.video_note.file_id
    if reply.video:
        return reply.video.file_id
    if reply.animation:
        return reply.animation.file_id
    if reply.document and reply.document.mime_type:
        mt = reply.document.mime_type.lower()
        if mt.startswith("audio/") or mt.startswith("video/"):
            return reply.document.file_id
    return None


def _parse_query(text: str) -> tuple[str, tuple[str, ...]]:
    """Разбирает аргументы /music. Поддерживает префиксы yt/sc.
    Возвращает (запрос, кортеж_источников).
    """
    parts = text.split(maxsplit=2)
    rest = parts[1:] if len(parts) > 1 else []
    if not rest:
        return "", DEFAULT_SOURCES
    first = rest[0].lower()
    if first in ("vk", "вк", "vkmusic") and len(rest) > 1:
        return " ".join(rest[1:]).strip(), ("vk:",)
    if first in ("yt", "youtube") and len(rest) > 1:
        return " ".join(rest[1:]).strip(), ("ytsearch1:",)
    if first in ("sc", "soundcloud", "soundc") and len(rest) > 1:
        return " ".join(rest[1:]).strip(), ("scsearch1:",)
    # Без префикса — весь хвост запрос, источники по умолчанию.
    return " ".join(rest).strip(), DEFAULT_SOURCES


async def cmd_music(message: Message, bot: Bot):
    """Поиск и отправка музыки из нескольких источников."""
    chat_id = message.chat.id
    bc_id = message.business_connection_id

    raw = (message.text or message.caption or "").strip()
    text_query, sources = _parse_query(raw)

    reply = message.reply_to_message
    audio_file_id = _pick_audio_source(reply)
    use_shazam = bool(audio_file_id) and not text_query

    if not text_query and not use_shazam:
        sid = await _send_temp(
            bot, chat_id,
            "❌ Использование:\n"
            "/music <название> — поиск по тексту\n"
            "/music yt|sc <название> — конкретный источник (YouTube / SoundCloud)\n"
            "/music — reply на голосовое/аудио/видео — распознать через Shazam",
            bc_id,
        )
        await asyncio.sleep(7)
        await _delete_temp(bot, chat_id, sid, bc_id)
        return

    if not _has_ytdlp():
        sid = await _send_temp(bot, chat_id, "❌ yt-dlp не установлен.", bc_id)
        await asyncio.sleep(4)
        await _delete_temp(bot, chat_id, sid, bc_id)
        return
    if not _has_ffmpeg():
        sid = await _send_temp(bot, chat_id, "❌ ffmpeg не установлен.", bc_id)
        await asyncio.sleep(4)
        await _delete_temp(bot, chat_id, sid, bc_id)
        return

    tmpdir = tempfile.mkdtemp(prefix="music_")
    status_id: int | None = None
    shazam_meta: tuple[str, str] | None = None

    try:
        # === Этап 1: если нет текстового запроса, но есть reply с аудио — Shazam.
        query = text_query
        if use_shazam:
            status_id = await _send_temp(bot, chat_id, "🎙 Распознаю через Shazam…", bc_id)
            sample_path = os.path.join(tmpdir, "sample.bin")
            ok = await _download_telegram_file(bot, audio_file_id, sample_path)
            if not ok:
                await _delete_temp(bot, chat_id, status_id, bc_id)
                err = await _send_temp(bot, chat_id, "❌ Не удалось скачать файл для распознавания.", bc_id)
                await asyncio.sleep(5)
                await _delete_temp(bot, chat_id, err, bc_id)
                return
            recognized = await _shazam_recognize(sample_path)
            if not recognized:
                await _delete_temp(bot, chat_id, status_id, bc_id)
                err = await _send_temp(
                    bot, chat_id,
                    "🔇 Shazam не узнал эту запись. Попробуйте более чистый отрывок (5–15 сек).",
                    bc_id,
                )
                await asyncio.sleep(6)
                await _delete_temp(bot, chat_id, err, bc_id)
                return
            shazam_meta = recognized
            artist, title = recognized
            query = f"{artist} {title}".strip() if artist else title
            await _edit_temp(
                bot, chat_id, status_id, bc_id,
                f"🎙 Shazam: {artist + ' — ' if artist else ''}{title}\n🔎 Ищу полную версию…"
            )

        # === Этап 2: ПАРАЛЛЕЛЬНЫЙ поиск по всем источникам.
        # Все источники запускаются одновременно. Первый, кто вернёт результат,
        # побеждает — остальные сразу отменяются (yt-dlp процессы убиваются).
        if status_id is None:
            status_id = await _send_temp(
                bot, chat_id, SEARCH_STATUS, bc_id, parse_mode="HTML"
            )

        # Если VK_TOKEN не задан — молча убираем VK из списка, чтобы не тратить
        # на него «попытку» (он бы всё равно сразу вернул None).
        active_sources = tuple(
            p for p in sources
            if p != VK_SOURCE or os.environ.get("VK_TOKEN", "").strip()
        )
        if not active_sources:
            # Пользователь явно попросил vk, а токена нет.
            await _delete_temp(bot, chat_id, status_id, bc_id)
            err = await _send_temp(
                bot, chat_id,
                "❌ VK_TOKEN не настроен на сервере — VK Музыка недоступна.",
                bc_id,
            )
            await asyncio.sleep(5)
            await _delete_temp(bot, chat_id, err, bc_id)
            return

        # Кастомный статус-текст вместо технического "Ищу одновременно в ...".
        # Источники и query больше не показываем — пользователь хотел компактный
        # фирменный текст с premium-эмодзи.
        await _edit_temp(
            bot, chat_id, status_id, bc_id,
            SEARCH_STATUS, parse_mode="HTML",
        )

        # Каждому источнику — своя tmp-подпапка, чтобы файлы не путались
        # (и побеждённого можно было спокойно удалить вместе с папкой).
        async def _src_task(prefix: str) -> tuple[str, dict | None]:
            label = SOURCE_LABELS.get(prefix, prefix)
            sub = tempfile.mkdtemp(prefix=f"music_{label}_", dir=tmpdir)
            try:
                if prefix == VK_SOURCE:
                    res = await _try_vk(query, sub)
                else:
                    res = await _try_source(query, prefix, sub)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logging.warning(f"[music] {label} упал: {e}")
                res = None
            return label, res

        tasks = {asyncio.create_task(_src_task(p)): p for p in active_sources}
        found: dict | None = None
        used_source: str | None = None

        try:
            pending = set(tasks.keys())
            while pending and not found:
                done, pending = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED
                )
                for t in done:
                    try:
                        label, result = t.result()
                    except asyncio.CancelledError:
                        continue
                    except Exception as e:
                        logging.warning(f"[music] task error: {e}")
                        continue
                    if result:
                        found = result
                        used_source = label
                        break
        finally:
            # Отменяем всё, что ещё работает (если победитель найден или
            # все провалились — лишних yt-dlp процессов остаться не должно).
            for t in tasks:
                if not t.done():
                    t.cancel()
            # Дожидаемся, чтобы subprocess.kill() успел отработать.
            if tasks:
                await asyncio.gather(*tasks.keys(), return_exceptions=True)

        if not found:
            await _delete_temp(bot, chat_id, status_id, bc_id)
            err = await _send_temp(
                bot, chat_id,
                NOT_FOUND_STATUS,
                bc_id,
                parse_mode="HTML",
            )
            await asyncio.sleep(5)
            await _delete_temp(bot, chat_id, err, bc_id)
            return

        # === Этап 3: подготовка тегов и отправка.
        size = os.path.getsize(found["filepath"])
        if size > MAX_OUTPUT_BYTES:
            await _delete_temp(bot, chat_id, status_id, bc_id)
            err = await _send_temp(
                bot, chat_id,
                f"❌ Файл слишком большой ({size // (1024 * 1024)} МБ). Лимит — 49 МБ.",
                bc_id,
            )
            await asyncio.sleep(5)
            await _delete_temp(bot, chat_id, err, bc_id)
            return

        # Определяем теги: Shazam > yt-dlp track/artist > yt-dlp title-парсинг > запрос
        if shazam_meta:
            sh_artist, sh_title = shazam_meta
            performer = sh_artist
            title = sh_title
        elif found.get("track") and found.get("artist"):
            performer = found["artist"]
            title = found["track"]
        else:
            guessed_artist, guessed_title = _split_artist_title(found.get("title", ""))
            title = guessed_title or found.get("title", "") or query
            performer = guessed_artist or found.get("artist", "") or found.get("uploader", "") or ""

        title = (title or "").strip()[:64]
        performer = (performer or "").strip()[:64]

        with open(found["filepath"], "rb") as f:
            data = f.read()

        filename = _sanitize_filename(
            f"{performer} - {title}".strip(" -") if performer else title
        ) + ".mp3"

        reply_to = reply.message_id if reply else None
        duration = found.get("duration") or 0

        try:
            await bot.send_audio(
                chat_id,
                BufferedInputFile(data, filename=filename),
                title=title,
                performer=performer,
                duration=duration if duration > 0 else None,
                business_connection_id=bc_id,
                reply_to_message_id=reply_to,
            )
        except Exception as e:
            logging.error(f"[music] send_audio: {e}")
            await _delete_temp(bot, chat_id, status_id, bc_id)
            err = await _send_temp(bot, chat_id, "❌ Не удалось отправить аудио.", bc_id)
            await asyncio.sleep(5)
            await _delete_temp(bot, chat_id, err, bc_id)
            return

        # Успех — убираем статус.
        await _delete_temp(bot, chat_id, status_id, bc_id)
        logging.info(f"[music] sent '{performer} — {title}' from {used_source}")

    except Exception as e:
        logging.exception(f"[music] неожиданная ошибка: {e}")
        await _delete_temp(bot, chat_id, status_id, bc_id)
        try:
            err = await _send_temp(bot, chat_id, "❌ Что-то пошло не так. Попробуйте ещё раз.", bc_id)
            await asyncio.sleep(5)
            await _delete_temp(bot, chat_id, err, bc_id)
        except Exception:
            pass
    finally:
        try:
            shutil.rmtree(tmpdir, ignore_errors=True)
        except Exception:
            pass
