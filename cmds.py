import asyncio
import logging
import random
from aiogram import Bot
from aiogram.types import Message
from aiogram.exceptions import TelegramRetryAfter
from fun import delete_command


# === /mother (встроенная подкоманда внутри /troll) ===
# Раньше /mother была отдельной командой, потом её убрали (см. коммит
# 79b8f2e). По просьбе пользователя возвращаем — но НЕ как самостоятельную
# команду, а как часть комбо /troll. Просто /mother отдельно работать
# не должна.
MOTHER_MESSAGES = [
    '<tg-emoji emoji-id="5431824012307087977">❤</tg-emoji>| Твою мать разъебали',
    '<tg-emoji emoji-id="5429609793457264027">❤</tg-emoji>| Я ебал твою родословную',
    '<tg-emoji emoji-id="5467430798724470930">❤</tg-emoji>| Твой пахан немужчина',
    '<tg-emoji emoji-id="5467777273736237111">❤</tg-emoji>| Твоя семья умрет',
    '<tg-emoji emoji-id="5467460713171683916">❤</tg-emoji>| Забил гол в ворота твоей сестры',
    '<tg-emoji emoji-id="5467813493195441676">❤</tg-emoji>| Ты сын жирной хуйни',
    '<tg-emoji emoji-id="5911384560041463738">❤</tg-emoji>| Я ебал твой рот',
]

# Флаг "идёт ли цикл mother в этом чате". Сбрасывается через /stop.
mother_running: dict[int, bool] = {}


async def cmd_id(message: Message, bot: Bot):
    if message.reply_to_message and message.reply_to_message.from_user:
        user = message.reply_to_message.from_user
    else:
        user = message.from_user

    await delete_command(message, bot)

    uid = user.id if user else "—"
    username = f"@{user.username}" if (user and user.username) else "—"

    text = (
        f'<tg-emoji emoji-id="5904630315946611415">👤</tg-emoji> '
        f'<b>{username}</b> | <code>{uid}</code>'
    )
    await bot.send_message(
        message.chat.id,
        text,
        parse_mode="HTML",
        business_connection_id=message.business_connection_id,
    )


MUTE_TEXT = '<tg-emoji emoji-id="5431449413849486465">❤</tg-emoji>| Помолчи'
UNMUTE_TEXT = '<tg-emoji emoji-id="5388632425314140043">❤</tg-emoji>| Говори'


async def _edit_command_to(message: Message, bot: Bot, new_text: str):
    """Заменяет текст команды в чате на заданный (через edit_message_text).

    Если редактирование не удалось — fallback: удаляем команду и отправляем
    новое сообщение от имени владельца.
    """
    bc_id = message.business_connection_id
    try:
        await bot.edit_message_text(
            text=new_text,
            chat_id=message.chat.id,
            message_id=message.message_id,
            business_connection_id=bc_id,
            parse_mode="HTML",
        )
        return
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after + 0.3)
        try:
            await bot.edit_message_text(
                text=new_text,
                chat_id=message.chat.id,
                message_id=message.message_id,
                business_connection_id=bc_id,
                parse_mode="HTML",
            )
            return
        except Exception as e2:
            logging.warning(f"Не удалось отредактировать команду после RetryAfter: {e2}")
    except Exception as e:
        logging.warning(f"Не удалось отредактировать команду: {e}")

    # Fallback: удалить и отправить заново
    try:
        await delete_command(message, bot)
    except Exception:
        pass
    try:
        await bot.send_message(
            message.chat.id,
            new_text,
            parse_mode="HTML",
            business_connection_id=bc_id,
        )
    except Exception as e:
        logging.error(f"Fallback send_message не удалось: {e}")


typing_running: dict[int, bool] = {}


async def cmd_typing(message: Message, bot: Bot):
    """Непрерывно показывает собеседнику индикатор «печатает…» пока
    не будет вызвана команда /stop. Удаление команды выполняется в bot.py
    приоритетно ДО запуска этой задачи.
    """
    chat_id = message.chat.id
    bc_id = message.business_connection_id

    if typing_running.get(chat_id, False):
        return

    typing_running[chat_id] = True
    try:
        while typing_running.get(chat_id, False):
            try:
                await bot.send_chat_action(
                    chat_id=chat_id,
                    action="typing",
                    business_connection_id=bc_id,
                )
            except TelegramRetryAfter as e:
                await asyncio.sleep(e.retry_after + 0.3)
                continue
            except Exception as e:
                logging.warning(f"[TYPING] send_chat_action: {e}")
            # Индикатор живёт ~5 сек, обновляем каждые 4.
            await asyncio.sleep(4.0)
    finally:
        typing_running[chat_id] = False


async def cmd_ignore(message: Message, bot: Bot, ignore_chats: set):
    """Включает авто-игнор: бот сразу помечает входящие сообщения
    собеседника как прочитанные. Удаление команды — в bot.py приоритетно.
    Останавливается командой /stop.
    """
    ignore_chats.add(message.chat.id)


async def cmd_mirror(message: Message, bot: Bot, mirror_chats: set):
    """Включает режим зеркала: бот будет повторять сообщения собеседника
    от имени владельца. Удаление команды — в bot.py приоритетно.
    Останавливается командой /stop.
    """
    mirror_chats.add(message.chat.id)


async def cmd_mute(message: Message, bot: Bot, muted_chats: set):
    """Добавляет чат в режим мута. Все сообщения собеседника будут удаляться."""
    chat_id = message.chat.id
    muted_chats.add(chat_id)
    await _edit_command_to(message, bot, MUTE_TEXT)


async def cmd_unmute(message: Message, bot: Bot, muted_chats: set):
    """Снимает режим мута. Должна срабатывать с первого раза, без задержек."""
    chat_id = message.chat.id
    # Сбрасываем состояние СРАЗУ — приоритет над любыми гонками.
    muted_chats.discard(chat_id)
    await _edit_command_to(message, bot, UNMUTE_TEXT)


async def cmd_troll(
    message: Message,
    bot: Bot,
    muted_chats: set,
    mirror_chats: set,  # не используется, передаётся для единообразия
    ignore_chats: set,
):
    """/troll = /mute + /ignore + /typing + рандомные mother-тексты.

    Удаление команды происходит в bot.py приоритетно (до запуска задачи).
    Здесь только включаем все режимы. Останавливается ПОЛНОСТЬЮ командой
    /stop (или её синонимами .стоп / стоп) — она гасит ВСЕ четыре режима
    разом, включая mute. Раньше /unmute приходилось дожимать вручную —
    больше не нужно, /stop работает как безотказный тормоз.
    """
    chat_id = message.chat.id
    bc_id = message.business_connection_id

    # /mute — заглушаем собеседника
    muted_chats.add(chat_id)

    # /ignore — авто-чтение входящих
    ignore_chats.add(chat_id)

    # /typing — постоянный индикатор «печатает…»
    if not typing_running.get(chat_id, False):
        typing_running[chat_id] = True
        asyncio.create_task(_typing_loop(bot, chat_id, bc_id))

    # /mother — рандомные оскорбительные тексты в фоне (по просьбе
    # пользователя возвращена как часть /troll, но НЕ как самостоятельная
    # команда). Тексты крутятся пока не пришёл /stop.
    if not mother_running.get(chat_id, False):
        mother_running[chat_id] = True
        asyncio.create_task(_mother_loop(bot, chat_id, bc_id))


async def _typing_loop(bot: Bot, chat_id: int, bc_id: str | None):
    try:
        while typing_running.get(chat_id, False):
            try:
                await bot.send_chat_action(
                    chat_id=chat_id,
                    action="typing",
                    business_connection_id=bc_id,
                )
            except TelegramRetryAfter as e:
                await asyncio.sleep(e.retry_after + 0.3)
                continue
            except Exception as e:
                logging.warning(f"[TROLL/TYPING] send_chat_action: {e}")
            await asyncio.sleep(4.0)
    finally:
        typing_running[chat_id] = False


async def _mother_loop(bot: Bot, chat_id: int, bc_id: str | None):
    """Шлёт случайный текст из MOTHER_MESSAGES раз в ~3 секунды,
    пока mother_running[chat_id] = True. /stop сбрасывает флаг
    в bot.py — следующая итерация цикла увидит это и выйдет.
    """
    try:
        while mother_running.get(chat_id, False):
            try:
                text = random.choice(MOTHER_MESSAGES)
                await bot.send_message(
                    chat_id,
                    text,
                    parse_mode="HTML",
                    business_connection_id=bc_id,
                )
            except TelegramRetryAfter as e:
                await asyncio.sleep(e.retry_after + 0.5)
                continue
            except Exception as e:
                logging.warning(f"[TROLL/MOTHER] send: {e}")
                # На сетевой/HTML-ошибке не убиваем весь цикл — просто
                # пропускаем итерацию и идём дальше.
            await asyncio.sleep(3.0)
    finally:
        mother_running[chat_id] = False


