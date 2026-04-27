import os
import logging
import aiohttp
from aiogram import Bot
from aiogram.types import Message
from fun import delete_command

AI_API_KEY = os.getenv("AI_API_KEY", "")
AI_BASE_URL = os.getenv("AI_BASE_URL", "https://openrouter.ai/api/v1").rstrip("/")
AI_MODEL = os.getenv("AI_MODEL", "openai/gpt-oss-120b:free")
AI_TIMEOUT = float(os.getenv("AI_TIMEOUT", "30"))

SYSTEM_PROMPT = (
    "Ты помощник в личной переписке Telegram. Отвечай на вопрос собеседника "
    "по существу, кратко и по-человечески, на том же языке, на котором задан вопрос. "
    "Без приветствий, без вводных фраз, без markdown-разметки и без эмодзи. "
    "Если вопрос требует актуальных данных, которых у тебя нет (погода, курс, новости), "
    "честно ответь, что не знаешь точных текущих данных, и дай общий ориентир."
)


async def _ask_ai(question: str) -> str:
    if not AI_API_KEY:
        return "⚠️ AI_API_KEY не задан в переменных окружения."

    url = f"{AI_BASE_URL}/chat/completions"
    headers = {
        "Authorization": f"Bearer {AI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": AI_MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": question},
        ],
        "temperature": 0.7,
    }

    timeout = aiohttp.ClientTimeout(total=AI_TIMEOUT)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, headers=headers, json=payload) as resp:
                data = await resp.json()
                if resp.status != 200:
                    logging.error(f"AI API error {resp.status}: {data}")
                    err = ""
                    if isinstance(data, dict):
                        err = (data.get("error") or {}).get("message", "") if isinstance(data.get("error"), dict) else str(data.get("error", ""))
                    return f"⚠️ Ошибка AI ({resp.status}). {err}".strip()
                try:
                    text = data["choices"][0]["message"]["content"].strip()
                    if not text:
                        return "⚠️ AI вернул пустой ответ."
                    return text
                except (KeyError, IndexError, TypeError):
                    logging.error(f"Неожиданный ответ AI: {data}")
                    return "⚠️ Неожиданный ответ от AI."
    except Exception as e:
        logging.error(f"Ошибка запроса к AI: {e}")
        return "⚠️ Не удалось получить ответ от AI."


async def cmd_search(message: Message, bot: Bot):
    """
    /search — отвечая на сообщение, генерирует ответ через AI
    и отправляет его от имени владельца через бизнес-подключение.
    """
    question = ""
    if message.reply_to_message:
        question = (
            message.reply_to_message.text
            or message.reply_to_message.caption
            or ""
        ).strip()

    parts = (message.text or "").split(maxsplit=1)
    extra = parts[1].strip() if len(parts) > 1 else ""
    if extra:
        question = f"{question}\n\n{extra}".strip() if question else extra

    if not question:
        await delete_command(message, bot)
        try:
            await bot.send_message(
                message.chat.id,
                "ℹ️ Используйте /search в ответ на сообщение с вопросом, "
                "либо напишите вопрос после команды: /search ваш вопрос",
                business_connection_id=message.business_connection_id,
            )
        except Exception as e:
            logging.warning(f"Не удалось отправить подсказку /search: {e}")
        return

    await delete_command(message, bot)

    answer = await _ask_ai(question)

    if len(answer) > 4000:
        answer = answer[:4000] + "…"

    try:
        await bot.send_message(
            message.chat.id,
            answer,
            business_connection_id=message.business_connection_id,
            reply_to_message_id=(
                message.reply_to_message.message_id
                if message.reply_to_message else None
            ),
        )
    except Exception as e:
        logging.error(f"Ошибка отправки ответа /search: {e}")
        try:
            await bot.send_message(
                message.chat.id,
                answer,
                business_connection_id=message.business_connection_id,
            )
        except Exception as e2:
            logging.error(f"Повторная ошибка /search: {e2}")
