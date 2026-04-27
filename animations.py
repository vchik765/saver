"""Анимированные команды бота.

Здесь живут эффектные команды, которые вместо обычного ответа постепенно
"печатают" содержимое сообщения, добавляя по одному эмодзи / символу за раз.

Сейчас тут одна команда — /love (алиас .love), которая собирает большое
сердце из премиум-эмодзи и дописывает «I love you 🤍» внизу.
"""

import asyncio
import logging

from aiogram import Bot
from aiogram.exceptions import TelegramRetryAfter
from aiogram.types import Message

from fun import delete_command


# === /love (алиас .love) ============================================
#
# Финальная картинка состоит из 7 строк:
#   строки 1, 2: 4 пробела отступа + 4 эмодзи
#   строки 3, 4: 4 пробела отступа + 5 эмодзи
#   строки 5, 6: 4 пробела отступа + 4 эмодзи
#   строка 7  : 2 пробела отступа + "I    love    you  " + сердце-эмодзи
#
# Каждая строка набирается слева направо, а сообщение редактируется после
# каждого добавленного эмодзи / символа, чтобы получился эффект «бот
# постепенно набирает сердце».

LOVE_LINES: list[list[int]] = [
    # строка 1 (4 эмодзи)
    [5382347079978875135, 5382318913583349082, 5384211937598931713, 5382295948393220845],
    # строка 2 (4 эмодзи)
    [5384123809164991002, 5382283321189366841, 5384245588667695429, 5384571576685460338],
    # строка 3 (5 эмодзи)
    [5384123212164532351, 5384101406615570174, 5384167123910163518, 5381906523708491747, 5382312123240056655],
    # строка 4 (5 эмодзи)
    [5381948275085573986, 5384140456458222962, 5382035419972009739, 5382298950575357422, 5381803023586593728],
    # строка 5 (4 эмодзи)
    [5382313244226517423, 5384353924922769569, 5381937992933867807, 5381807808180157532],
    # строка 6 (4 эмодзи)
    [5384453774322465850, 5384257520086843283, 5382133005923944771, 5384058590086593620],
]

LOVE_HEART_ID: int = 5969864985067656768

LOVE_LINE_INDENT: str = "    "      # 4 пробела для строк 2–6
LOVE_LINE1_INDENT: str = "       "  # 7 пробелов для строки 1 (она чуть смещена вправо)
LOVE_FINAL_INDENT: str = "  "       # 2 пробела для строки 7
LOVE_FINAL_TEXT: str = "I    love    you  "

# Пауза между правками сообщения. Чем меньше — тем «бодрее» анимация,
# но Telegram быстро ответит TelegramRetryAfter, если переборщить.
LOVE_DELAY: float = 0.25

# Запасной символ, который видят клиенты без премиум-эмодзи.
LOVE_FALLBACK_EMOJI: str = "🤩"
LOVE_HEART_FALLBACK: str = "🤍"


def _emoji_tag(emoji_id: int, fallback: str = LOVE_FALLBACK_EMOJI) -> str:
    return f'<tg-emoji emoji-id="{emoji_id}">{fallback}</tg-emoji>'


def _build_love_snapshots() -> list[str]:
    """Возвращает список «снимков» сообщения — по одному на каждый шаг
    анимации. snapshots[0] — первое состояние (отправляется как новое
    сообщение), snapshots[i>0] — состояния для последующих edit_message_text.

    Чтобы не тратить правки на невидимые символы (\n, отступы), переводы
    строк и пробелы клеятся к ПЕРВОМУ эмодзи новой строки в одном шаге.
    """
    snapshots: list[str] = []
    current = ""

    for i, line in enumerate(LOVE_LINES):
        indent = LOVE_LINE1_INDENT if i == 0 else LOVE_LINE_INDENT
        line_prefix = ("\n" if i > 0 else "") + indent
        for j, emoji_id in enumerate(line):
            current += (line_prefix if j == 0 else "") + _emoji_tag(emoji_id)
            snapshots.append(current)

    # Строка 7 — печатается посимвольно, чтобы было видно, как фраза
    # «I love you» постепенно дописывается под сердцем.
    line7_prefix = "\n" + LOVE_FINAL_INDENT
    for k, ch in enumerate(LOVE_FINAL_TEXT):
        current += (line7_prefix if k == 0 else "") + ch
        snapshots.append(current)

    current += _emoji_tag(LOVE_HEART_ID, LOVE_HEART_FALLBACK)
    snapshots.append(current)

    return snapshots


# Считаем один раз при импорте — список не меняется.
_LOVE_SNAPSHOTS: list[str] = _build_love_snapshots()


async def _edit_with_retry(
    bot: Bot,
    chat_id: int,
    message_id: int,
    text: str,
    business_connection_id: str | None,
) -> bool:
    """Пытается обновить сообщение, переживая TelegramRetryAfter и тихие
    ошибки «message is not modified». Возвращает False, если сообщение
    исчезло (тогда продолжать анимацию бессмысленно)."""
    for _ in range(3):
        try:
            await bot.edit_message_text(
                text=text,
                chat_id=chat_id,
                message_id=message_id,
                parse_mode="HTML",
                business_connection_id=business_connection_id,
            )
            return True
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after + 0.3)
            continue
        except Exception as e:
            err = str(e).lower()
            if "message is not modified" in err:
                return True
            if "message to edit not found" in err or "message_id_invalid" in err:
                logging.info(f"/love: сообщение {message_id} удалено, останавливаемся")
                return False
            logging.warning(f"/love: edit failed: {e}")
            return True
    return True


async def cmd_love(message: Message, bot: Bot) -> None:
    """Анимация /love (и .love). Удаляет команду пользователя, отправляет
    первое состояние сердца и постепенно дописывает остальное сообщение
    через edit_message_text. Работает и в обычном чате, и в бизнес-чате."""
    chat_id = message.chat.id
    bc_id = message.business_connection_id

    # Удаляем саму команду пользователя.
    try:
        await delete_command(message, bot)
    except Exception as e:
        logging.warning(f"/love: не удалось удалить команду: {e}")

    if not _LOVE_SNAPSHOTS:
        return

    try:
        sent = await bot.send_message(
            chat_id,
            _LOVE_SNAPSHOTS[0],
            parse_mode="HTML",
            business_connection_id=bc_id,
        )
    except Exception as e:
        logging.error(f"/love: не удалось отправить стартовое сообщение: {e}")
        return

    msg_id = sent.message_id

    for snapshot in _LOVE_SNAPSHOTS[1:]:
        await asyncio.sleep(LOVE_DELAY)
        ok = await _edit_with_retry(bot, chat_id, msg_id, snapshot, bc_id)
        if not ok:
            return
