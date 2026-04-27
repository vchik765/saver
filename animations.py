import asyncio
import logging

from aiogram import Bot
from aiogram.exceptions import TelegramRetryAfter
from aiogram.types import Message

from fun import delete_command


# === /love ==========================================================

LOVE_LINES: list[list[int]] = [
    [5382347079978875135, 5382318913583349082, 5384211937598931713, 5382295948393220845],
    [5384123809164991002, 5382283321189366841, 5384245588667695429, 5384571576685460338],
    [5384123212164532351, 5384101406615570174, 5384167123910163518, 5381906523708491747, 5382312123240056655],
    [5381948275085573986, 5384140456458222962, 5382035419972009739, 5382298950575357422, 5381803023586593728],
    [5382313244226517423, 5384353924922769569, 5381937992933867807, 5381807808180157532],
    [5384453774322465850, 5384257520086843283, 5382133005923944771, 5384058590086593620],
]

LOVE_HEART_ID: int = 5969864985067656768

# ⚠️ Используем &nbsp; вместо обычных пробелов
NBSP = "&nbsp;"

LOVE_LINE_INDENT = NBSP * 4
LOVE_LINE1_INDENT = NBSP * 7
LOVE_FINAL_INDENT = NBSP * 2

LOVE_FINAL_TEXT = "I" + NBSP * 4 + "love" + NBSP * 4 + "you" + NBSP * 2

LOVE_DELAY: float = 0.25

LOVE_FALLBACK_EMOJI = "🤩"
LOVE_HEART_FALLBACK = "🤍"


def _emoji_tag(emoji_id: int, fallback: str = LOVE_FALLBACK_EMOJI) -> str:
    return f'<tg-emoji emoji-id="{emoji_id}">{fallback}</tg-emoji>'


def _build_love_snapshots() -> list[str]:
    snapshots: list[str] = []
    current = ""

    for i, line in enumerate(LOVE_LINES):
        indent = LOVE_LINE1_INDENT if i == 0 else LOVE_LINE_INDENT
        line_prefix = ("<br>" if i > 0 else "") + indent

        for j, emoji_id in enumerate(line):
            current += (line_prefix if j == 0 else "") + _emoji_tag(emoji_id)
            snapshots.append(current)

    # строка с текстом
    line7_prefix = "<br>" + LOVE_FINAL_INDENT

    for k, ch in enumerate(LOVE_FINAL_TEXT):
        current += (line7_prefix if k == 0 else "") + ch
        snapshots.append(current)

    current += _emoji_tag(LOVE_HEART_ID, LOVE_HEART_FALLBACK)
    snapshots.append(current)

    return snapshots


_LOVE_SNAPSHOTS = _build_love_snapshots()


async def _edit_with_retry(
    bot: Bot,
    chat_id: int,
    message_id: int,
    text: str,
    business_connection_id: str | None,
) -> bool:
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

        except Exception as e:
            err = str(e).lower()

            if "message is not modified" in err:
                return True

            if "message to edit not found" in err or "message_id_invalid" in err:
                return False

            logging.warning(f"/love: edit failed: {e}")
            return True

    return True


async def cmd_love(message: Message, bot: Bot) -> None:
    chat_id = message.chat.id
    bc_id = message.business_connection_id

    try:
        await delete_command(message, bot)
    except Exception:
        pass

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
        logging.error(f"/love: send failed: {e}")
        return

    msg_id = sent.message_id

    for snapshot in _LOVE_SNAPSHOTS[1:]:
        await asyncio.sleep(LOVE_DELAY)

        ok = await _edit_with_retry(
            bot, chat_id, msg_id, snapshot, bc_id
        )
        if not ok:
            return
