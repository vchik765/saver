import asyncio
import logging
import random
from aiogram import Bot
from aiogram.types import Message, MessageEntity
from aiogram.exceptions import TelegramRetryAfter


spam_running: dict[int, bool] = {}

N_EMOJIS = [
    "5467706063178453976",
    "5366389751760851269",
    "5368428705880219702",
    "5368376753955806970",
    "5368808192010625717",
    "5368523530168180585",
    "5289505907267364732",
    "5402149709596861623",
    "5442823788624371882",
    "5415858570895846595",
    # +5 новых вариантов для лайкера (запрос пользователя)
    "5431824012307087977",
    "5258276353949575281",
    "5391272000545110592",
    "5298800431244258880",
    "5222472673346482653",
]

A_EMOJIS = [
    "5211025120918785460",
    "5447614579829921377",
    "5211155151053675393",
    "5246899131611377553",
    "5395591955960851534",
    "5188439983853169383",
    "5310299561934207014",
    "5440740896989540064",
    "5442861262214030402",
    "5393305396976828658",
    # +5 новых вариантов для лайкера (запрос пользователя)
    "5429393563328728003",
    "5431437246207118009",
    "5217895054252715986",
    "5249003098650736064",
    "5219935756423818133",
]


def get_display_name(user) -> str:
    if not user:
        return "Неизвестный"
    name = user.first_name or ""
    if user.last_name:
        name += f" {user.last_name}"
    if name.strip():
        return name.strip()
    if user.username:
        return f"@{user.username}"
    return str(user.id)


def get_like_suffix() -> str:
    n = random.choice(N_EMOJIS)
    a = random.choice(A_EMOJIS)
    return (
        f'.<tg-emoji emoji-id="{n}">❤</tg-emoji>'
        f'<tg-emoji emoji-id="{a}">❤</tg-emoji>'
    )


async def delete_command(message: Message, bot: Bot):
    """Удаляет одно сообщение (команду).

    В Business-чате используется специальный метод deleteBusinessMessages
    (только он работает для удаления сообщений владельца через подключение).
    В обычном чате — стандартный deleteMessage.
    """
    last_err = None
    bc_id = message.business_connection_id
    chat_id = message.chat.id
    mid = message.message_id

    if bc_id:
        # Business-чат: только delete_business_messages работает корректно
        # для удаления сообщений владельца через подключение бизнес-бота.
        for _ in range(2):
            try:
                await bot.delete_business_messages(
                    business_connection_id=bc_id,
                    message_ids=[mid],
                )
                return
            except TelegramRetryAfter as e:
                await asyncio.sleep(e.retry_after + 0.3)
                continue
            except Exception as e:
                last_err = e
                err_text = str(e).lower()
                if "message to delete not found" in err_text or "message_id_invalid" in err_text:
                    return
                break
    else:
        # Обычный чат.
        for _ in range(2):
            try:
                await bot.delete_message(chat_id=chat_id, message_id=mid)
                return
            except TelegramRetryAfter as e:
                await asyncio.sleep(e.retry_after + 0.3)
                continue
            except Exception as e:
                last_err = e
                err_text = str(e).lower()
                if "message to delete not found" in err_text or "message_id_invalid" in err_text:
                    return
                break

    if last_err:
        logging.warning(f"Не удалось удалить команду {mid} (bc={bool(bc_id)}): {last_err}")


async def cmd_spam(message: Message, bot: Bot):
    chat_id = message.chat.id
    raw = message.text or ""
    parts = raw.split(maxsplit=2)

    if spam_running.get(chat_id, False):
        try:
            await bot.send_message(
                chat_id,
                "⏳ Спам уже запущен. Дождитесь завершения или используйте /stop",
                business_connection_id=message.business_connection_id,
            )
        except Exception:
            pass
        return

    if len(parts) < 3:
        await bot.send_message(
            chat_id,
            "❌ Использование: /spam [кол-во 1–99] [текст до 200 символов]",
            business_connection_id=message.business_connection_id,
            reply_to_message_id=message.message_id,
        )
        return

    if not parts[1].isdigit():
        await bot.send_message(
            chat_id,
            "❌ Количество должно быть числом от 1 до 99",
            business_connection_id=message.business_connection_id,
            reply_to_message_id=message.message_id,
        )
        return

    count = max(1, min(int(parts[1]), 99))
    spam_text = parts[2]

    if len(spam_text) > 200:
        await bot.send_message(
            chat_id,
            "❌ Текст не должен превышать 200 символов",
            business_connection_id=message.business_connection_id,
            reply_to_message_id=message.message_id,
        )
        return

    text_offset = raw.index(spam_text)

    shifted_entities: list[MessageEntity] = []
    if message.entities:
        for ent in message.entities:
            ent_end = ent.offset + ent.length
            if ent_end > text_offset:
                new_offset = max(ent.offset - text_offset, 0)
                new_length = ent.length - max(text_offset - ent.offset, 0)
                if new_length > 0:
                    shifted_entities.append(MessageEntity(
                        type=ent.type,
                        offset=new_offset,
                        length=new_length,
                        url=getattr(ent, "url", None),
                        user=getattr(ent, "user", None),
                        language=getattr(ent, "language", None),
                        custom_emoji_id=getattr(ent, "custom_emoji_id", None),
                    ))

    await delete_command(message, bot)

    spam_running[chat_id] = True
    try:
        for _ in range(count):
            if not spam_running.get(chat_id, False):
                break
            try:
                await bot.send_message(
                    chat_id,
                    spam_text,
                    entities=shifted_entities if shifted_entities else None,
                    business_connection_id=message.business_connection_id,
                )
                await asyncio.sleep(0.35)
            except TelegramRetryAfter as e:
                await asyncio.sleep(e.retry_after + 0.5)
            except Exception as e:
                logging.error(f"Ошибка спама: {e}")
                break
    finally:
        spam_running[chat_id] = False


