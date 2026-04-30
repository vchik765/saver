import os
import logging
import aiohttp
from aiogram import Bot
from aiogram.types import Message
from fun import delete_command

# Groq предоставляет OpenAI-совместимый chat completions endpoint.
# Ключ берём из того же GROQ_API_KEY, что используется для распознавания
# Корана через Whisper в quran.py — он уже задан на Railway.
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_BASE_URL = os.getenv("GROQ_BASE_URL", "https://api.groq.com/openai/v1").rstrip("/")
# По умолчанию — самая свежая популярная модель Groq на конец 2025 года.
# Можно переопределить через env GROQ_MODEL без правок кода.
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
GROQ_TIMEOUT = float(os.getenv("GROQ_TIMEOUT", "30"))

SYSTEM_PROMPT = (
    "Ты помощник в личной переписке Telegram. Отвечай на вопрос собеседника "
    "по существу, кратко и по-человечески, на том же языке, на котором задан вопрос. "
    "Без приветствий, без вводных фраз, без markdown-разметки и без эмодзи. "
    "Если вопрос требует актуальных данных, которых у тебя нет (погода, курс, новости), "
    "честно ответь, что не знаешь точных текущих данных, и дай общий ориентир."
)


async def _ask_groq(question: str) -> str:
    if not GROQ_API_KEY:
        return "⚠️ GROQ_API_KEY не задан в переменных окружения."

    url = f"{GROQ_BASE_URL}/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": GROQ_MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": question},
        ],
        "temperature": 0.7,
    }

    timeout = aiohttp.ClientTimeout(total=GROQ_TIMEOUT)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, headers=headers, json=payload) as resp:
                data = await resp.json()
                if resp.status != 200:
                    logging.error(f"Groq API error {resp.status}: {data}")
                    err = ""
                    if isinstance(data, dict):
                        err_obj = data.get("error")
                        if isinstance(err_obj, dict):
                            err = err_obj.get("message", "")
                        elif err_obj:
                            err = str(err_obj)
                    return f"⚠️ Ошибка Groq ({resp.status}). {err}".strip()
                try:
                    text = data["choices"][0]["message"]["content"].strip()
                    if not text:
                        return "⚠️ Groq вернул пустой ответ."
                    return text
                except (KeyError, IndexError, TypeError):
                    logging.error(f"Неожиданный ответ Groq: {data}")
                    return "⚠️ Неожиданный ответ от Groq."
    except Exception as e:
        logging.error(f"Ошибка запроса к Groq: {e}")
        return "⚠️ Не удалось получить ответ от Groq."


async def cmd_groq(message: Message, bot: Bot):
    """
    /groq — отвечая на сообщение, генерирует ответ через Groq
    и отправляет его от имени владельца через бизнес-подключение.
    Логика идентична /search, отличается только нейронка-провайдер.
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
                "ℹ️ Используйте /groq в ответ на сообщение с вопросом, "
                "либо напишите вопрос после команды: /groq ваш вопрос",
                business_connection_id=message.business_connection_id,
            )
        except Exception as e:
            logging.warning(f"Не удалось отправить подсказку /groq: {e}")
        return

    await delete_command(message, bot)

    answer = await _ask_groq(question)

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
        logging.error(f"Ошибка отправки ответа /groq: {e}")
        try:
            await bot.send_message(
                message.chat.id,
                answer,
                business_connection_id=message.business_connection_id,
            )
        except Exception as e2:
            logging.error(f"Повторная ошибка /groq: {e2}")
