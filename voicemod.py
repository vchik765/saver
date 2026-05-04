"""Команда /voicemod — изменение голоса в реальном времени.

Использование:
  /voicemod ребенок   — детский, высокий голос
  /voicemod робот     — роботизированный
  /voicemod пришелец  — инопланетный (очень высокий + вибрато)
  /voicemod мужчина   — грубый, мощный низкий голос
  /voicemod пикми     — мягкий аниме-голос

Синонимы: .войсмод
Стоп: /stop (или .стоп / стоп)

Механика:
- Работает только в чате, где активирована, только у владельца.
- Голосовые владельца удаляются, взамен отправляется изменённая версия.
- Голосовые собеседника не трогаются.
- Удалённый оригинал не отображается как «удалённое» в боте.
- Второй /voicemod автоматически стопает первый.
- Reply-контекст сохраняется.
"""

import asyncio
import logging
import os
import shutil
import tempfile

from aiogram import Bot
from aiogram.types import Message, BufferedInputFile

from fun import delete_command

# === Типы голосов: слово/эмодзи → ffmpeg-фильтр ===
_CHILD  = "asetrate=81600,aresample=48000,atempo=0.588"
_ROBOT  = "vibrato=f=20:d=0.9,aecho=0.7:0.9:30:0.5"
_ALIEN  = "asetrate=91200,aresample=48000,atempo=0.526,vibrato=f=8:d=1.0"
_MAN    = "asetrate=26400,aresample=48000,atempo=1.818,bass=g=12,volume=2.0"
_PIKMI  = "asetrate=91200,aresample=48000,atempo=0.526,highpass=f=160,equalizer=f=700:width_type=o:width=2:g=3,equalizer=f=3500:width_type=o:width=2:g=-2,lowpass=f=11000"

VOICE_TYPES: dict[str, str] = {
    "ребенок":       _CHILD,
    "ребёнок":       _CHILD,
    "👶":            _CHILD,
    "👶🏿":          _CHILD,
    "робот":         _ROBOT,
    "🤖":            _ROBOT,
    "инопланетянин": _ALIEN,
    "пришелец":      _ALIEN,
    "👽":            _ALIEN,
    "мужчина":       _MAN,
    "💪":            _MAN,
    "пикми":         _PIKMI,
    "🌸":            _PIKMI,
    "🎀":            _PIKMI,
}

VOICE_DISPLAY: dict[str, str] = {
    _CHILD: "Ребенок 👶",
    _ROBOT: "Робот 🤖",
    _ALIEN: "Инопланетянин 👽",
    _MAN:   "Мужчина 💪",  # deep + bass +12dB + 2x volume
    _PIKMI: "Пикми 🌸",
}

# (owner_id, chat_id) → ffmpeg-фильтр активного voicemod
voicemod_active: dict[tuple[int, int], str] = {}

# (chat_id, msg_id) голосовых, удалённых ботом ради voicemod.
# handle_deleted_event пропускает их.
voicemod_deleted_msgs: set[tuple[int, int]] = set()
_VOICEMOD_DELETED_MAX = 5000

MAX_INPUT_BYTES = 20 * 1024 * 1024  # 20 МБ
FFMPEG_TIMEOUT  = 30                 # секунд


def _has_ffmpeg() -> bool:
    return shutil.which("ffmpeg") is not None


async def _run_ffmpeg(args: list[str]) -> tuple[bool, str]:
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-loglevel", "error", *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=FFMPEG_TIMEOUT)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return False, "ffmpeg timeout"
        if proc.returncode != 0:
            tail = (stderr or b"").decode("utf-8", errors="ignore")[-400:]
            return False, tail
        return True, ""
    except Exception as e:
        return False, str(e)


async def _download_voice(bot: Bot, file_id: str, dst: str) -> bool:
    try:
        f = await bot.get_file(file_id)
        if f.file_size and f.file_size > MAX_INPUT_BYTES:
            return False
        await bot.download_file(f.file_path, destination=dst)
        return os.path.exists(dst) and os.path.getsize(dst) > 0
    except Exception as e:
        logging.warning(f"[voicemod] download: {e}")
        return False


async def _delete_msg(bot: Bot, bc_id: str, chat_id: int, msg_id: int) -> None:
    try:
        await bot.delete_business_messages(
            business_connection_id=bc_id,
            message_ids=[msg_id],
        )
    except AttributeError:
        try:
            from aiogram.methods import DeleteBusinessMessages
            await bot(DeleteBusinessMessages(
                business_connection_id=bc_id,
                message_ids=[msg_id],
            ))
        except Exception as e:
            logging.warning(f"[voicemod] raw delete: {e}")
    except Exception as e:
        logging.warning(f"[voicemod] delete {msg_id}: {e}")


async def cmd_voicemod(message: Message, bot: Bot, owner_id: int) -> None:
    """Обработчик команды /voicemod <голос>."""
    chat_id = message.chat.id
    bc_id   = message.business_connection_id

    parts = (message.text or "").strip().split(maxsplit=1)
    arg   = parts[1].strip().lower() if len(parts) > 1 else ""

    await delete_command(message, bot)

    filter_str = VOICE_TYPES.get(arg)
    if not filter_str:
        help_text = (
            "Использование: <code>/voicemod &lt;голос&gt;</code>\n\n"
            "Доступные голоса:\n"
            "• <code>ребенок</code> 👶 — детский высокий\n"
            "• <code>робот</code> 🤖 — роботизированный\n"
            "• <code>пришелец</code> 👽 — инопланетный (очень высокий)\n"
            "• <code>мужчина</code> 💪 — грубый, мощный\n"
            "• <code>пикми</code> 🌸 — мягкий аниме-голос\n\n"
            "Остановить: <code>/stop</code>"
        )
        try:
            sent = await bot.send_message(
                chat_id, help_text, parse_mode="HTML",
                business_connection_id=bc_id,
            )
            await asyncio.sleep(8)
            await _delete_msg(bot, bc_id, chat_id, sent.message_id)
        except Exception as e:
            logging.warning(f"[voicemod] help: {e}")
        return

    if not _has_ffmpeg():
        try:
            sent = await bot.send_message(
                chat_id, "❌ ffmpeg не установлен.",
                business_connection_id=bc_id,
            )
            await asyncio.sleep(5)
            await _delete_msg(bot, bc_id, chat_id, sent.message_id)
        except Exception:
            pass
        return

    # Стопаем предыдущий voicemod (если был) и активируем новый
    voicemod_active[(owner_id, chat_id)] = filter_str
    display = VOICE_DISPLAY.get(filter_str, "???")

    try:
        sent = await bot.send_message(
            chat_id,
            f'<tg-emoji emoji-id="5346074681004801565">🎙</tg-emoji> VoiceMod вкл: <b>{display}</b>',
            parse_mode="HTML",
            business_connection_id=bc_id,
        )
        await asyncio.sleep(3)
        await _delete_msg(bot, bc_id, chat_id, sent.message_id)
    except Exception as e:
        logging.warning(f"[voicemod] status: {e}")


async def process_voicemod_voice(
    message: Message,
    bot: Bot,
    owner_id: int,
) -> None:
    """Перехватывает голосовое владельца: удаляет оригинал, отправляет изменённое."""
    chat_id = message.chat.id
    bc_id   = message.business_connection_id
    msg_id  = message.message_id

    filter_str = voicemod_active.get((owner_id, chat_id))
    if not filter_str:
        return

    # Помечаем ДО удаления — handle_deleted_event пропустит его
    voicemod_deleted_msgs.add((chat_id, msg_id))
    if len(voicemod_deleted_msgs) > _VOICEMOD_DELETED_MAX:
        voicemod_deleted_msgs.clear()

    tmpdir   = tempfile.mkdtemp(prefix="voicemod_")
    src_path = os.path.join(tmpdir, "input.ogg")
    out_path = os.path.join(tmpdir, "output.ogg")

    try:
        # Скачиваем оригинал и одновременно удаляем его из чата
        dl_ok, _ = await asyncio.gather(
            _download_voice(bot, message.voice.file_id, src_path),
            _delete_msg(bot, bc_id, chat_id, msg_id),
        )

        if not dl_ok:
            logging.warning(f"[voicemod] не удалось скачать {msg_id}")
            return

        # Применяем ffmpeg-эффект
        ok, err = await _run_ffmpeg([
            "-i", src_path,
            "-af", filter_str,
            "-c:a", "libopus",
            "-b:a", "64k",
            "-ar", "48000",
            "-ac", "1",
            out_path,
        ])

        if not ok or not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
            logging.error(f"[voicemod] ffmpeg error: {err}")
            return

        with open(out_path, "rb") as fh:
            data = fh.read()

        reply_to = (
            message.reply_to_message.message_id
            if message.reply_to_message else None
        )

        await bot.send_voice(
            chat_id,
            BufferedInputFile(data, filename="voice.ogg"),
            business_connection_id=bc_id,
            reply_to_message_id=reply_to,
            duration=message.voice.duration,
        )

    except Exception as e:
        logging.error(f"[voicemod] process: {e}")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
