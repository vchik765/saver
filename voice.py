"""Команды /voice и /audio — конвертация аудио ↔ голосовое.

/voice — реплай на аудио (или видео/кружок) → отправляет голосовое.
/audio — реплай на голосовое (или видео/кружок) → отправляет аудио (название пустое).

Команда удаляется СРАЗУ (приоритет), чтобы её не было видно в переписке.
Конвертация — через ffmpeg в фоне (не блокирует event loop).
"""
import asyncio
import logging
import os
import shutil
import tempfile
from io import BytesIO

from aiogram import Bot
from aiogram.types import Message, BufferedInputFile

# Лимиты
MAX_INPUT_BYTES = 49 * 1024 * 1024  # 49 MB — лимит загрузки бота через get_file
HTTP_TIMEOUT = 60


def _has_ffmpeg() -> bool:
    return shutil.which("ffmpeg") is not None


def _pick_source_file_id(reply: Message) -> tuple[str | None, str]:
    """Возвращает (file_id, исходный_тип) у сообщения-источника.

    Поддерживаемые источники: voice, audio, video, video_note, animation, document(audio/video).
    """
    if not reply:
        return None, ""
    if reply.voice:
        return reply.voice.file_id, "voice"
    if reply.audio:
        return reply.audio.file_id, "audio"
    if reply.video_note:
        return reply.video_note.file_id, "video_note"
    if reply.video:
        return reply.video.file_id, "video"
    if reply.animation:
        return reply.animation.file_id, "animation"
    if reply.document and reply.document.mime_type:
        mt = reply.document.mime_type.lower()
        if mt.startswith("audio/") or mt.startswith("video/"):
            return reply.document.file_id, "document"
    return None, ""


async def _download_file(bot: Bot, file_id: str, dst_path: str) -> bool:
    try:
        f = await bot.get_file(file_id)
        if f.file_size and f.file_size > MAX_INPUT_BYTES:
            logging.warning(f"[voice] файл слишком большой: {f.file_size}")
            return False
        await bot.download_file(f.file_path, destination=dst_path)
        return os.path.exists(dst_path) and os.path.getsize(dst_path) > 0
    except Exception as e:
        logging.warning(f"[voice] download_file: {e}")
        return False


async def _run_ffmpeg(args: list[str]) -> tuple[bool, str]:
    """Запускает ffmpeg в фоне. Возвращает (ok, stderr_tail)."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-loglevel", "error", *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=HTTP_TIMEOUT)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return False, "timeout"
        if proc.returncode != 0:
            tail = (stderr or b"").decode("utf-8", errors="ignore")[-500:]
            return False, tail
        return True, ""
    except Exception as e:
        return False, str(e)


async def _to_ogg_opus(src_path: str, dst_path: str) -> tuple[bool, str]:
    """Конвертирует в OGG/Opus 48k mono (формат telegram voice)."""
    return await _run_ffmpeg([
        "-i", src_path,
        "-vn",
        "-ac", "1",
        "-ar", "48000",
        "-c:a", "libopus",
        "-b:a", "64k",
        "-application", "voip",
        dst_path,
    ])


async def _to_mp3(src_path: str, dst_path: str) -> tuple[bool, str]:
    """Конвертирует в MP3 (формат telegram audio)."""
    return await _run_ffmpeg([
        "-i", src_path,
        "-vn",
        "-ac", "2",
        "-ar", "44100",
        "-c:a", "libmp3lame",
        "-q:a", "4",
        dst_path,
    ])


async def _send_temp_error(bot: Bot, chat_id: int, text: str, bc_id: str | None, delay: float = 4.0):
    try:
        sent = await bot.send_message(chat_id, text, business_connection_id=bc_id)
        await asyncio.sleep(delay)
        try:
            await bot.delete_messages(
                chat_id=chat_id,
                message_ids=[sent.message_id],
                business_connection_id=bc_id,
            )
        except Exception:
            pass
    except Exception as e:
        logging.warning(f"[voice] _send_temp_error: {e}")


async def _convert_and_send(
    bot: Bot,
    message: Message,
    target: str,  # "voice" или "audio"
):
    """Общая часть: качаем источник, конвертируем, отправляем reply на исходное сообщение."""
    chat_id = message.chat.id
    bc_id = message.business_connection_id

    if not _has_ffmpeg():
        await _send_temp_error(bot, chat_id, "❌ На сервере не установлен ffmpeg.", bc_id)
        return

    reply = message.reply_to_message
    if not reply:
        cmd_label = "/voice" if target == "voice" else "/audio"
        await _send_temp_error(
            bot, chat_id,
            f"❌ Ответьте командой {cmd_label} на голосовое/аудио/видео/кружок.",
            bc_id,
        )
        return

    file_id, src_kind = _pick_source_file_id(reply)
    if not file_id:
        await _send_temp_error(
            bot, chat_id,
            "❌ В исходном сообщении нет аудио/голосового/видео/кружка.",
            bc_id,
        )
        return

    tmpdir = tempfile.mkdtemp(prefix="va_")
    src_path = os.path.join(tmpdir, "input.bin")
    out_ext = "ogg" if target == "voice" else "mp3"
    out_path = os.path.join(tmpdir, f"out.{out_ext}")

    try:
        ok = await _download_file(bot, file_id, src_path)
        if not ok:
            await _send_temp_error(bot, chat_id, "❌ Не удалось скачать исходный файл.", bc_id)
            return

        if target == "voice":
            ok, err = await _to_ogg_opus(src_path, out_path)
        else:
            ok, err = await _to_mp3(src_path, out_path)

        if not ok or not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
            logging.error(f"[voice] ffmpeg failed: {err}")
            await _send_temp_error(bot, chat_id, "❌ Не удалось конвертировать файл.", bc_id)
            return

        with open(out_path, "rb") as f:
            data = f.read()

        try:
            if target == "voice":
                await bot.send_voice(
                    chat_id,
                    BufferedInputFile(data, filename="voice.ogg"),
                    business_connection_id=bc_id,
                    reply_to_message_id=reply.message_id,
                )
            else:
                # Название аудио должно быть пустым: title="" и filename без значимого названия.
                await bot.send_audio(
                    chat_id,
                    BufferedInputFile(data, filename="audio.mp3"),
                    title="",
                    performer="",
                    business_connection_id=bc_id,
                    reply_to_message_id=reply.message_id,
                )
        except Exception as e:
            logging.error(f"[voice] send error: {e}")
            await _send_temp_error(bot, chat_id, "❌ Не удалось отправить результат.", bc_id)
    finally:
        try:
            shutil.rmtree(tmpdir, ignore_errors=True)
        except Exception:
            pass


async def cmd_voice(message: Message, bot: Bot):
    """Конвертирует исходник (audio/video/video_note/...) в голосовое (.ogg opus)."""
    await _convert_and_send(bot, message, "voice")


async def cmd_audio(message: Message, bot: Bot):
    """Конвертирует исходник (voice/video/video_note/...) в аудио (.mp3, без названия)."""
    await _convert_and_send(bot, message, "audio")
