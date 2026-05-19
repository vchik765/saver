"""Команда /music (/sound) — поиск и отправка музыки из нескольких источников.

Использование:
  /sound <запрос>            — поиск: Deezer-нормализация + VK + YouTube + SoundCloud + Bandcamp
  /sound vk <запрос>         — только VK Музыка
  /sound yt <запрос>         — только YouTube
  /sound sc <запрос>         — только SoundCloud
  /sound bc <запрос>         — только Bandcamp
  /sound dz <запрос>         — только Deezer
  /sound                     — reply на голосовое/аудио/видео: Shazam → поиск

Улучшения v2:
  • Deezer API (бесплатно, без ключа) — нормализует запрос перед поиском.
    Даже если написать "sport miyagi" или "miyagi sport" — найдёт правильно.
  • iTunes Search API — дополнительный нормализатор для западной музыки.
  • Bandcamp — новый источник, хорош для инди и нишевой музыки.
  • Параллельный поиск по оригинальному И нормализованному запросу.
  • Таймаут увеличен, Bandcamp добавлен в дефолтные источники.
"""
import asyncio
import logging
import os
import re
import shutil
import tempfile
import urllib.parse

from aiogram import Bot
from aiogram.types import Message, BufferedInputFile

# ── Нежелательные версии (ремиксы, ускоренные и т.п.) ───────────────
JUNK_RE = re.compile(
    r"(?i)("
    r"remix|sped[\s\-_]?up|speed[\s\-_]?up|nightcore|slowed|reverb(?:ed)?|"
    r"cover(?:\s+version)?|karaoke|instrumental|tribute|mashup|"
    r"edit(?:ed)?(?:\s+version)?|"
    r"live(?:\s+at|\s+from|\s+version)?|concert|acoustic\s+version|"
    r"orchestral|piano\s+version|ringtone|"
    r"lyric[s]?\s+video|\blyrics\b|8d\s+audio|bass\s+boost(?:ed)?|"
    r"phonk(?:\s+version)?|1\s*hour(?:\s+loop)?|[23456789]0\s*minutes?\s+loop|"
    r"extended\s+mix|radio\s+edit|demo|rehearsal|tiktok|"
    r"ускоренн|замедленн|слоу|nightcore|кавер"
    r")"
)

# ── Лимиты ─────────────────────────────────────────────────────────
MAX_DURATION_SEC    = 15 * 60
MAX_OUTPUT_BYTES    = 49 * 1024 * 1024
PER_SOURCE_TIMEOUT  = 90          # увеличен с 60 до 90 сек
SHAZAM_TIMEOUT      = 30
SHAZAM_INPUT_LIMIT  = 10 * 1024 * 1024
DEEZER_TIMEOUT      = 8           # сек на нормализацию через Deezer
ITUNES_TIMEOUT      = 6           # сек на нормализацию через iTunes

# ── Источники ───────────────────────────────────────────────────────
VK_SOURCE      = "vk:"
DEFAULT_SOURCES = ("vk:", "dzmsearch5:", "ytsearch10:", "scsearch10:", "bcsearch1:")
SOURCE_LABELS   = {
    "vk:":         "VK",
    "dzmsearch5:": "Deezer",
    "ytsearch10:": "YouTube",
    "scsearch10:": "SoundCloud",
    "bcsearch1:":  "Bandcamp",
}

# ── Статусы (premium-эмодзи) ─────────────────────────────────────────
SEARCH_STATUS    = '<tg-emoji emoji-id="5346074681004801565">🔎</tg-emoji>| <b>Ищу звук</b>'
NOT_FOUND_STATUS = '<tg-emoji emoji-id="5208647293879721534">❌</tg-emoji>| <b>Ничего не найдено</b>'


# ════════════════════════════════════════════════════════════════════
#  Утилиты
# ════════════════════════════════════════════════════════════════════

def _has_ffmpeg() -> bool:
    return shutil.which("ffmpeg") is not None

def _has_ytdlp() -> bool:
    return shutil.which("yt-dlp") is not None

def _sanitize_filename(name: str) -> str:
    name = re.sub(r"[\x00-\x1f<>:\"/\\|?*]", "", name).strip()
    return name[:120] if name else "audio"

def _split_artist_title(raw_title: str) -> tuple[str, str]:
    if not raw_title:
        return "", ""
    clean = re.sub(
        r"\s*[\(\[](official(\s+(music\s+)?(video|audio|lyric.?\s*video))?|"
        r"music\s+video|lyrics?(\s+video)?|audio|hd|hq|4k|"
        r"премьера(\s+клипа)?|клип|original\s+mix|extended)\s*[\)\]]",
        "", raw_title, flags=re.IGNORECASE,
    ).strip(" -—–|")
    for sep in (" - ", " — ", " – ", " | "):
        if sep in clean:
            left, right = clean.split(sep, 1)
            return left.strip(), right.strip()
    return "", clean


# ════════════════════════════════════════════════════════════════════
#  Нормализация запроса через Deezer и iTunes (бесплатно, без ключа)
# ════════════════════════════════════════════════════════════════════

async def _normalize_via_deezer(query: str) -> str | None:
    """Ищет трек в Deezer API (бесплатно, без ключа).
    Возвращает 'Artist Title' в правильном порядке или None.
    Исправляет 'sport miyagi' → 'Miyagi & Andy Panda Sport' и т.п."""
    try:
        import aiohttp
        url = f"https://api.deezer.com/search?q={urllib.parse.quote(query)}&limit=3"
        timeout = aiohttp.ClientTimeout(total=DEEZER_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as r:
                data = await r.json(content_type=None)
        items = (data or {}).get("data") or []
        if not items:
            return None
        track = items[0]
        artist = (track.get("artist") or {}).get("name", "").strip()
        title  = (track.get("title") or "").strip()
        if artist and title:
            logging.info(f"[music] Deezer: '{query}' → '{artist} - {title}'")
            return f"{artist} {title}"
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logging.debug(f"[music] Deezer normalize failed: {e}")
    return None


async def _normalize_via_itunes(query: str) -> str | None:
    """Ищет трек в iTunes Search API (бесплатно, без ключа).
    Хорош для западной музыки и английских названий."""
    try:
        import aiohttp
        url = (
            f"https://itunes.apple.com/search"
            f"?term={urllib.parse.quote(query)}&media=music&limit=3"
        )
        timeout = aiohttp.ClientTimeout(total=ITUNES_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as r:
                data = await r.json(content_type=None)
        results = (data or {}).get("results") or []
        if not results:
            return None
        track  = results[0]
        artist = (track.get("artistName") or "").strip()
        title  = (track.get("trackName") or "").strip()
        if artist and title:
            logging.info(f"[music] iTunes: '{query}' → '{artist} - {title}'")
            return f"{artist} {title}"
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logging.debug(f"[music] iTunes normalize failed: {e}")
    return None


async def _get_best_query(query: str) -> tuple[str, str | None]:
    """Запускает Deezer и iTunes параллельно.
    Возвращает (оригинальный_запрос, нормализованный_или_None)."""
    try:
        deezer_task = asyncio.create_task(_normalize_via_deezer(query))
        itunes_task = asyncio.create_task(_normalize_via_itunes(query))
        done, pending = await asyncio.wait(
            [deezer_task, itunes_task],
            timeout=max(DEEZER_TIMEOUT, ITUNES_TIMEOUT),
            return_when=asyncio.FIRST_COMPLETED,
        )
        normalized: str | None = None
        for t in done:
            try:
                res = t.result()
                if res:
                    normalized = res
                    break
            except Exception:
                pass
        # Отменяем то, что ещё не завершилось
        for t in pending:
            t.cancel()
        await asyncio.gather(deezer_task, itunes_task, return_exceptions=True)
        return query, normalized
    except Exception as e:
        logging.debug(f"[music] normalize error: {e}")
        return query, None


# ════════════════════════════════════════════════════════════════════
#  Subprocess / yt-dlp
# ════════════════════════════════════════════════════════════════════

async def _run_subprocess(args: list[str], timeout: int) -> tuple[int, bytes, bytes]:
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        return proc.returncode or 0, stdout or b"", stderr or b""
    except (asyncio.TimeoutError, asyncio.CancelledError):
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        try:
            await proc.wait()
        except Exception:
            pass
        raise


# ════════════════════════════════════════════════════════════════════
#  Telegram helpers
# ════════════════════════════════════════════════════════════════════

async def _send_temp(bot, chat_id, text, bc_id, parse_mode=None):
    try:
        sent = await bot.send_message(chat_id, text, business_connection_id=bc_id, parse_mode=parse_mode)
        return sent.message_id
    except Exception as e:
        logging.warning(f"[music] send_temp: {e}")
        return None

async def _delete_temp(bot, chat_id, mid, bc_id):
    if not mid:
        return
    try:
        if bc_id:
            await bot.delete_business_messages(business_connection_id=bc_id, message_ids=[mid])
        else:
            await bot.delete_message(chat_id=chat_id, message_id=mid)
    except Exception:
        pass

async def _edit_temp(bot, chat_id, mid, bc_id, text, parse_mode=None):
    if not mid:
        return
    try:
        await bot.edit_message_text(text=text, chat_id=chat_id, message_id=mid,
                                    business_connection_id=bc_id, parse_mode=parse_mode)
    except Exception:
        pass


# ════════════════════════════════════════════════════════════════════
#  Источники скачивания
# ════════════════════════════════════════════════════════════════════

def _parse_metadata_line(stdout: bytes) -> dict:
    info_line = ""
    for line in stdout.decode("utf-8", errors="ignore").splitlines():
        if line.strip():
            info_line = line.strip()
    fields = info_line.split("\t") if info_line else []

    def f(i):
        return fields[i] if len(fields) > i and fields[i] != "NA" else ""
    try:
        duration_int = int(float(fields[4])) if len(fields) > 4 and fields[4] != "NA" else 0
    except Exception:
        duration_int = 0
    return {
        "title":    f(0),
        "uploader": f(1),
        "artist":   f(2),
        "track":    f(3),
        "duration": duration_int,
        "filepath": fields[5] if len(fields) > 5 else "",
    }


async def _try_vk(query: str, tmpdir: str) -> dict | None:
    token = os.environ.get("VK_TOKEN", "").strip()
    if not token:
        return None
    try:
        import aiohttp
    except ImportError:
        return None

    api_url = "https://api.vk.com/method/audio.search"
    params  = {
        "q": query, "count": "5", "auto_complete": "1",
        "sort": "2", "access_token": token, "v": "5.131",
    }
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(api_url, params=params) as r:
                data = await r.json(content_type=None)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logging.warning(f"[music] VK API сеть: {e}")
        return None

    if "error" in data:
        err = data.get("error") or {}
        logging.warning(f"[music] VK API error {err.get('error_code')}: {err.get('error_msg')}")
        return None

    items = (data.get("response") or {}).get("items") or []
    # Prefer originals — skip remixes/sped-up/etc when possible
    valid = [it for it in items if (it.get("url") or "").strip()]
    clean = [
        it for it in valid
        if not JUNK_RE.search(f"{it.get('artist', '')} {it.get('title', '')}")
    ]
    track = (clean or valid)[0] if (clean or valid) else None
    if not track:
        return None

    url      = track["url"]
    artist   = (track.get("artist") or "").strip()
    title    = (track.get("title")  or "").strip()
    duration = int(track.get("duration") or 0)

    if duration and duration > MAX_DURATION_SEC:
        return None

    out_path = os.path.join(tmpdir, "track_VK.mp3")
    args = [
        "ffmpeg", "-y", "-loglevel", "error",
        "-protocol_whitelist", "file,http,https,tcp,tls,crypto,pipe",
        "-i", url, "-vn", "-c:a", "libmp3lame", "-b:a", "192k",
        "-id3v2_version", "3",
        "-metadata", f"title={title}",
        "-metadata", f"artist={artist}",
        out_path,
    ]
    try:
        rc, _out, stderr = await _run_subprocess(args, timeout=PER_SOURCE_TIMEOUT)
    except asyncio.TimeoutError:
        return None

    if rc != 0 or not os.path.exists(out_path) or os.path.getsize(out_path) < 10_000:
        return None
    if os.path.getsize(out_path) > MAX_OUTPUT_BYTES:
        return None

    return {"title": title, "uploader": artist, "artist": artist,
            "track": title, "duration": duration, "filepath": out_path}


async def _try_source(query: str, source_prefix: str, tmpdir: str) -> dict | None:
    """Универсальный источник через yt-dlp (YouTube, SoundCloud, Bandcamp)."""
    label        = SOURCE_LABELS.get(source_prefix, source_prefix)
    out_template = os.path.join(tmpdir, f"track_{label}.%(ext)s")
    args = [
        "yt-dlp",
        f"{source_prefix}{query}",
        "-x",
        "--audio-format", "mp3",
        "--audio-quality", "0",
        "--no-warnings",
        "--quiet",
        "--no-progress",
        "--max-downloads", "1",
        "--reject-title",
        r"(?i)(remix|sped[\s\-_]?up|speed[\s\-_]?up|nightcore|slowed|reverb(?:ed)?|"
        r"cover(?:\s+version)?|karaoke|instrumental|tribute|mashup|"
        r"edit(?:ed)?(?:\s+version)?|live(?:\s+at|\s+from|\s+version)?|concert|"
        r"acoustic\s+version|orchestral|piano\s+version|ringtone|"
        r"lyric[s]?\s+video|\blyrics\b|8d\s+audio|bass\s+boost(?:ed)?|"
        r"phonk(?:\s+version)?|1\s*hour(?:\s+loop)?|extended\s+mix|"
        r"radio\s+edit|demo|rehearsal|tiktok|"
        r"ускоренн|замедленн|слоу|кавер)",
        "--max-filesize", str(MAX_OUTPUT_BYTES),
        "--match-filter", f"duration < {MAX_DURATION_SEC}",
        "--print",
        "after_move:%(title)s\t%(uploader)s\t%(artist)s\t%(track)s\t%(duration)s\t%(filepath)s",
        "-o", out_template,
    ]
    try:
        rc, stdout, stderr = await _run_subprocess(args, timeout=PER_SOURCE_TIMEOUT)
    except asyncio.TimeoutError:
        logging.warning(f"[music] таймаут {label}")
        return None

    if rc != 0:
        err = stderr.decode("utf-8", errors="ignore")[-200:] if stderr else ""
        logging.info(f"[music] {label} не нашёл (rc={rc}): {err[:150]}")
        return None

    meta = _parse_metadata_line(stdout)
    fp   = meta["filepath"]
    if not fp or not os.path.exists(fp):
        for fn in os.listdir(tmpdir):
            if fn.lower().endswith(".mp3") and label in fn:
                fp = os.path.join(tmpdir, fn)
                meta["filepath"] = fp
                break

    if not fp or not os.path.exists(fp) or os.path.getsize(fp) == 0:
        return None
    return meta


# ════════════════════════════════════════════════════════════════════
#  Shazam
# ════════════════════════════════════════════════════════════════════

async def _shazam_recognize(file_path: str) -> tuple[str, str] | None:
    try:
        from shazamio import Shazam
    except ImportError:
        logging.warning("[music] shazamio не установлен")
        return None
    try:
        shazam = Shazam()
        result = await asyncio.wait_for(shazam.recognize(file_path), timeout=SHAZAM_TIMEOUT)
    except AttributeError:
        try:
            from shazamio import Shazam as Sh2
            shazam = Sh2()
            result = await asyncio.wait_for(shazam.recognize_song(file_path), timeout=SHAZAM_TIMEOUT)
        except Exception as e:
            logging.warning(f"[music] shazam (старый API): {e}")
            return None
    except asyncio.TimeoutError:
        logging.warning("[music] shazam timeout")
        return None
    except Exception as e:
        logging.warning(f"[music] shazam error: {e}")
        return None

    track  = (result or {}).get("track") or {}
    title  = (track.get("title")    or "").strip()
    artist = (track.get("subtitle") or "").strip()
    return (artist, title) if title else None


async def _download_telegram_file(bot, file_id: str, dst_path: str) -> bool:
    try:
        f = await bot.get_file(file_id)
        if f.file_size and f.file_size > SHAZAM_INPUT_LIMIT:
            return False
        await bot.download_file(f.file_path, destination=dst_path)
        return os.path.exists(dst_path) and os.path.getsize(dst_path) > 0
    except Exception as e:
        logging.warning(f"[music] download_file: {e}")
        return False


def _pick_audio_source(reply) -> str | None:
    if not reply:
        return None
    if reply.voice:       return reply.voice.file_id
    if reply.audio:       return reply.audio.file_id
    if reply.video_note:  return reply.video_note.file_id
    if reply.video:       return reply.video.file_id
    if reply.animation:   return reply.animation.file_id
    if reply.document and reply.document.mime_type:
        mt = reply.document.mime_type.lower()
        if mt.startswith("audio/") or mt.startswith("video/"):
            return reply.document.file_id
    return None


# ════════════════════════════════════════════════════════════════════
#  Парсинг аргументов
# ════════════════════════════════════════════════════════════════════

def _parse_query(text: str) -> tuple[str, tuple[str, ...]]:
    parts = text.split(maxsplit=2)
    rest  = parts[1:] if len(parts) > 1 else []
    if not rest:
        return "", DEFAULT_SOURCES
    first = rest[0].lower()
    if first in ("vk", "вк", "vkmusic") and len(rest) > 1:
        return " ".join(rest[1:]).strip(), ("vk:",)
    if first in ("yt", "youtube") and len(rest) > 1:
        return " ".join(rest[1:]).strip(), ("ytsearch1:",)
    if first in ("sc", "soundcloud", "soundc") and len(rest) > 1:
        return " ".join(rest[1:]).strip(), ("scsearch1:",)
    if first in ("bc", "bandcamp") and len(rest) > 1:
        return " ".join(rest[1:]).strip(), ("bcsearch1:",)
    if first in ("dz", "deezer") and len(rest) > 1:
        return " ".join(rest[1:]).strip(), ("dzmsearch5:",)
    return " ".join(rest).strip(), DEFAULT_SOURCES


# ════════════════════════════════════════════════════════════════════
#  Основная функция поиска
# ════════════════════════════════════════════════════════════════════

async def cmd_music(message: Message, bot: Bot):
    chat_id = message.chat.id
    bc_id   = message.business_connection_id

    raw               = (message.text or message.caption or "").strip()
    text_query, sources = _parse_query(raw)

    reply         = message.reply_to_message
    audio_file_id = _pick_audio_source(reply)
    use_shazam    = bool(audio_file_id) and not text_query

    if not text_query and not use_shazam:
        sid = await _send_temp(
            bot, chat_id,
            "❌ Использование:\n"
            "/sound <название> — поиск по тексту\n"
            "/sound yt|sc|bc|vk <название> — конкретный источник\n"
            "/sound — reply на голосовое/аудио — распознать через Shazam",
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

    tmpdir     = tempfile.mkdtemp(prefix="music_")
    status_id  = None
    shazam_meta: tuple[str, str] | None = None

    try:
        # ── Этап 1: Shazam (если reply на аудио без текстового запроса) ──
        query = text_query
        if use_shazam:
            status_id  = await _send_temp(bot, chat_id, "🎙 Распознаю через Shazam…", bc_id)
            sample_path = os.path.join(tmpdir, "sample.bin")
            ok = await _download_telegram_file(bot, audio_file_id, sample_path)
            if not ok:
                await _delete_temp(bot, chat_id, status_id, bc_id)
                err = await _send_temp(bot, chat_id, "❌ Не удалось скачать файл.", bc_id)
                await asyncio.sleep(5)
                await _delete_temp(bot, chat_id, err, bc_id)
                return
            recognized = await _shazam_recognize(sample_path)
            if not recognized:
                await _delete_temp(bot, chat_id, status_id, bc_id)
                err = await _send_temp(
                    bot, chat_id,
                    "🔇 Shazam не узнал. Попробуйте чистый отрывок (5–15 сек).", bc_id,
                )
                await asyncio.sleep(6)
                await _delete_temp(bot, chat_id, err, bc_id)
                return
            shazam_meta  = recognized
            artist, title = recognized
            query = f"{artist} {title}".strip() if artist else title
            await _edit_temp(
                bot, chat_id, status_id, bc_id,
                f"🎙 Shazam: {artist + ' — ' if artist else ''}{title}\n🔎 Ищу полную версию…"
            )

        # ── Этап 2: Нормализация запроса через Deezer + iTunes ──
        # Запускаем параллельно с первым поиском — не теряем время.
        # Нормализация нужна только при текстовом запросе (не Shazam).
        normalized_query: str | None = None
        if text_query and sources == DEFAULT_SOURCES:
            _, normalized_query = await _get_best_query(query)
            # Если нормализованный == оригинальному — не дублируем поиск
            if normalized_query and normalized_query.lower().strip() == query.lower().strip():
                normalized_query = None

        # ── Этап 3: Статус ──
        if status_id is None:
            status_id = await _send_temp(bot, chat_id, SEARCH_STATUS, bc_id, parse_mode="HTML")
        else:
            await _edit_temp(bot, chat_id, status_id, bc_id, SEARCH_STATUS, parse_mode="HTML")

        # ── Этап 4: Параллельный поиск ──
        # Убираем VK если токена нет.
        active_sources = tuple(
            p for p in sources
            if p != VK_SOURCE or os.environ.get("VK_TOKEN", "").strip()
        )
        if not active_sources:
            await _delete_temp(bot, chat_id, status_id, bc_id)
            err = await _send_temp(bot, chat_id, "❌ VK_TOKEN не настроен.", bc_id)
            await asyncio.sleep(5)
            await _delete_temp(bot, chat_id, err, bc_id)
            return

        # Формируем задачи:
        #   • Оригинальный запрос — все источники
        #   • Нормализованный запрос (если отличается) — только YouTube + VK
        #     (они лучше всего ищут по точному названию)
        async def _src_task(q: str, prefix: str) -> tuple[str, dict | None]:
            label = SOURCE_LABELS.get(prefix, prefix)
            suffix = "_norm" if q != query else ""
            sub = tempfile.mkdtemp(prefix=f"music_{label}{suffix}_", dir=tmpdir)
            try:
                if prefix == VK_SOURCE:
                    res = await _try_vk(q, sub)
                else:
                    res = await _try_source(q, prefix, sub)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logging.warning(f"[music] {label} упал: {e}")
                res = None
            return label, res

        tasks_list = [(_src_task(query, p), p) for p in active_sources]
        # Добавляем нормализованный запрос на YouTube и VK
        if normalized_query:
            for p in ("ytsearch10:", "dzmsearch5:", "vk:"):
                if p in active_sources:
                    tasks_list.append((_src_task(normalized_query, p), p + "_norm"))

        tasks = {asyncio.create_task(coro): tag for coro, tag in tasks_list}

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
                    except (asyncio.CancelledError, Exception):
                        continue
                    if result:
                        found       = result
                        used_source = label
                        break
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks.keys(), return_exceptions=True)

        if not found:
            await _delete_temp(bot, chat_id, status_id, bc_id)
            err = await _send_temp(bot, chat_id, NOT_FOUND_STATUS, bc_id, parse_mode="HTML")
            await asyncio.sleep(5)
            await _delete_temp(bot, chat_id, err, bc_id)
            return

        # ── Этап 5: Теги и отправка ──
        size = os.path.getsize(found["filepath"])
        if size > MAX_OUTPUT_BYTES:
            await _delete_temp(bot, chat_id, status_id, bc_id)
            err = await _send_temp(
                bot, chat_id,
                f"❌ Файл слишком большой ({size // (1024*1024)} МБ). Лимит — 49 МБ.", bc_id,
            )
            await asyncio.sleep(5)
            await _delete_temp(bot, chat_id, err, bc_id)
            return

        if shazam_meta:
            sh_artist, sh_title = shazam_meta
            performer = sh_artist
            title     = sh_title
        elif found.get("track") and found.get("artist"):
            performer = found["artist"]
            title     = found["track"]
        else:
            guessed_artist, guessed_title = _split_artist_title(found.get("title", ""))
            title     = guessed_title or found.get("title", "") or query
            performer = (guessed_artist or found.get("artist", "")
                         or found.get("uploader", "") or "")

        title     = (title     or "").strip()[:64]
        performer = (performer or "").strip()[:64]

        with open(found["filepath"], "rb") as fh:
            data = fh.read()

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

        await _delete_temp(bot, chat_id, status_id, bc_id)
        logging.info(f"[music] отправлено '{performer} — {title}' из {used_source}")

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
        shutil.rmtree(tmpdir, ignore_errors=True)
