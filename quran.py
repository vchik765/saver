"""Команда /quran — распознаёт суру и аят(ы) из чтения Корана.

Использование: ответить /quran (или .Коран / .сура / .аят) на голосовое /
аудио / видео / кружок с чтением Корана.

Алгоритм:
  1. Скачиваем исходник, конвертируем в mp3 16kHz mono (оптимально для Whisper).
  2. Распознаём арабскую речь:
       а) основной движок — Groq Whisper Large v3 (быстро + бесплатно);
       б) фолбэк — HuggingFace tarteel-ai/whisper-base-ar-quran (модель,
          специально дообученная на чтениях Корана). Если первый не ответил
          или вернул пусто — пробуем второй.
  3. Нормализуем арабский текст (убираем диакритику, унифицируем алифы и т.д.)
     и ищем совпадения с полным текстом Корана через индекс 4-грамм слов
     (6236 аятов).
  4. Группируем найденные аяты по сурам, считаем диапазоны n-a; если в записи
     несколько сур — отдаём список.
  5. Берём случайного чтеца из списка с публичными CDN и подставляем прямую
     ссылку на полную суру с mp3quran.net. Распознавание чтеца по аудио —
     честно говорим: открытых API нет, поэтому фолбэк рандом из списка.
  6. Отправляем audio + цитату с метаданными.

При ошибке/отсутствии распознавания — короткое сообщение, которое исчезает
через ~2.5 секунды.
"""
import asyncio
import json
import logging
import os
import random
import re
import shutil
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Optional

import aiohttp
from aiogram import Bot
from aiogram.types import Message, BufferedInputFile


# === Внешние сервисы ===
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_URL = "https://api.groq.com/openai/v1/audio/transcriptions"
GROQ_MODEL = os.getenv("GROQ_WHISPER_MODEL", "whisper-large-v3")
GROQ_TIMEOUT = 60

HF_TOKEN = os.getenv("HF_TOKEN", "")
HF_MODEL_URL = "https://api-inference.huggingface.co/models/tarteel-ai/whisper-base-ar-quran"
HF_TIMEOUT = 90

MAX_INPUT_BYTES = 49 * 1024 * 1024
MAX_AUDIO_OUT_BYTES = 49 * 1024 * 1024
ERROR_AUTO_DELETE_SEC = 2.5

# === Эмодзи (ID — премиум-эмодзи Telegram, как в задании) ===
EMOJI_SEARCH = '<tg-emoji emoji-id="6298321174510175872">🔎</tg-emoji>'
EMOJI_ERROR = '<tg-emoji emoji-id="5208647293879721534">❌</tg-emoji>'
EMOJI_SURA = '<tg-emoji emoji-id="5870995890180722030">📖</tg-emoji>'
EMOJI_AYAH = '<tg-emoji emoji-id="5348060643817715492">✨</tg-emoji>'
EMOJI_RECITER = '<tg-emoji emoji-id="5904630315946611415">🎙</tg-emoji>'

PROGRESS_TEXT = f"{EMOJI_SEARCH}| Поиск суры и аята"
ERROR_TEXT = f"{EMOJI_ERROR}| Ничего не найдено"

# === Чтецы ===
# Каждая запись:
#   "name"     — отображаемое имя (точно как пользователь хотел увидеть)
#   "url"      — шаблон URL с {n} = 001..114, либо None если CDN нет
#   "suras"    — список доступных сур: None = все 1..114, [] = ни одной,
#                [12, 36, 67, ...] = только перечисленные
#   "aliases"  — варианты, по которым пользователь может выбрать чтеца
#                (одно или несколько слов в нижнем регистре)
#
# По мейнстримным CDN (mp3quran.net) полного Корана у новых имён —
# Махди/Арби Аш-Шишани, Умайр Шамим, Умар Сильдинский, Абдуллах Аль-Карни,
# Мухаммад Ан-Накийдан — нет. Они выложены кусками в Telegram-каналах,
# поэтому suras=[] и для каждой запрошенной суры бот возвращает
# фирменный «нет этой суры» с подсказкой.
RECITERS: list[dict] = [
    {
        "name": "Мухаммад Аль-Люхайдан",
        "url":  "https://server8.mp3quran.net/lhdan/{n}.mp3",
        "suras": None,
        "aliases": ["люхайдан", "лухайдан", "аль-люхайдан", "аллюхайдан"],
    },
    {
        "name": "Ясир Ад-Даусари",
        "url":  "https://server11.mp3quran.net/yasser/{n}.mp3",
        "suras": None,
        "aliases": [
            "ясир", "даусари", "доусари", "досари",
            "ад-даусари", "аддаусари", "ясир ад-даусари",
        ],
    },
    {
        "name": "Мишари Рашид",
        "url":  "https://server8.mp3quran.net/afs/{n}.mp3",
        "suras": None,
        "aliases": [
            "мишари", "мишары", "афаси", "альафаси", "аль-афаси",
            "мишари рашид", "рашид аль-афаси",
        ],
    },
    {
        "name": "Мухаммад Аль-Миншави",
        "url":  "https://server10.mp3quran.net/minsh/{n}.mp3",
        "suras": None,
        "aliases": ["миншави", "миншауи", "минши", "аль-миншави"],
    },
    # === Без публичных CDN. Бот их «знает», но при запросе суры выдаёт
    # фирменный отказ со ссылкой `.<алиас>` для просмотра доступного.
    {
        "name": "Махди Аш-Шишани",
        "url":  None, "suras": [],
        "aliases": ["махди", "махди аш-шишани", "махди шишани"],
    },
    {
        "name": "Умайр Шамим",
        "url":  None, "suras": [],
        "aliases": ["умайр", "шамим", "умайр шамим"],
    },
    {
        "name": "Арби Аш-Шишани",
        "url":  None, "suras": [],
        "aliases": ["арби", "арби аш-шишани", "арби шишани"],
    },
    {
        "name": "Умар Сильдинский",
        "url":  None, "suras": [],
        "aliases": ["умар", "сильдинский", "силдинский", "умар сильдинский"],
    },
    {
        "name": "Абдуллах Аль-Карни",
        "url":  None, "suras": [],
        "aliases": [
            "карни", "аль-карни", "абдуллах",
            "абдуллах карни", "абдуллах аль-карни",
        ],
    },
    {
        "name": "Мухаммад Ан-Накийдан",
        "url":  None, "suras": [],
        "aliases": [
            "накийдан", "накийдон", "ан-накийдан",
            "мухаммад ан-накийдан", "мухаммад накийдан",
        ],
    },
    # Старые «нет CDN» — оставляем по совместимости со старыми сообщениями.
    {
        "name": "Сиратулло Раупов",
        "url":  None, "suras": [],
        "aliases": ["сиратулло", "раупов"],
    },
    {
        "name": "Абдул Рахман Моссад",
        "url":  None, "suras": [],
        "aliases": [
            "моссад", "мосад", "рахман",
            "абдулрахман", "абдуррахман",
        ],
    },
]

# Lookup: алиас → запись чтеца. Точка спереди и регистр игнорируются
# (например, ".люхайдан", "Люхайдан", "аль-Люхайдан" → один и тот же).
_RECITER_BY_ALIAS: dict[str, dict] = {}
for _rec in RECITERS:
    for _al in _rec["aliases"]:
        _RECITER_BY_ALIAS[_al.lower()] = _rec


def _normalize_alias(s: str) -> str:
    return s.strip().lower().lstrip(".").strip()


def _find_reciter_by_alias(text: str) -> Optional[dict]:
    """Возвращает запись чтеца по алиасу, либо None."""
    if not text:
        return None
    return _RECITER_BY_ALIAS.get(_normalize_alias(text))


def _primary_alias(rec: dict) -> str:
    """Главный (короткий) алиас чтеца — для подсказки `.X` в сообщениях."""
    return rec["aliases"][0]


def _split_reciter_and_text(arg: str) -> tuple[Optional[dict], str]:
    """Разбирает аргумент команды → (запись_чтеца | None, оставшийся_текст).

    Поддерживает многословные алиасы: пробует первые 1..3 слова как алиас.
    Примеры:
      ''                    → (None, '')
      'мишари'              → (rec, '')
      'мишари رب العالمين'  → (rec, 'رب العالمين')
      'махди аш-шишани 36'  → (rec, '36')
      'الحمد لله'           → (None, 'الحمد لله')
    """
    if not arg or not arg.strip():
        return None, ""
    s = arg.strip()
    rec = _find_reciter_by_alias(s)
    if rec is not None:
        return rec, ""
    parts = s.split()
    for n in range(min(3, len(parts)), 0, -1):
        candidate = " ".join(parts[:n])
        rec = _find_reciter_by_alias(candidate)
        if rec is not None:
            return rec, " ".join(parts[n:]).strip()
    return None, s


# === Корпус Корана + индекс 4-грамм ===
_QURAN_DATA: list[dict] = []
_NGRAM_INDEX: dict[str, list[tuple[int, int]]] = {}
NGRAM = 4
_DATA_PATH = Path(__file__).parent / "quran_data.json"


def _normalize_arabic(text: str) -> str:
    """Та же нормализация, что применена в quran_data.json при сборке."""
    if not text:
        return ""
    # Удаляем диакритику и доп. знаки
    text = re.sub(r"[\u064B-\u065F\u0670\u06D6-\u06ED]", "", text)
    # Унифицируем буквы
    text = re.sub(r"[إأآاٱ]", "ا", text)
    text = text.replace("ى", "ي").replace("ؤ", "و").replace("ئ", "ي").replace("ة", "ه")
    # Только арабские буквы и пробелы
    text = re.sub(r"[^\u0621-\u063A\u0641-\u064A\s]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


# === Best-effort транслитерация Cyrillic/Latin → арабская графика ===
# Нужна, чтобы можно было искать аят, написав «бисмиллях ар-рахман ар-рахим»
# или «bismillah ir-rahman ir-rahim». Совпадение с корпусом не идеальное
# (диалектов транслита много), но для типичных формул работает.
_TRANSLIT_MULTI: list[tuple[str, str]] = [
    # Сначала более длинные сочетания.
    ("аль-", "ال"), ("ал-", "ال"), ("эль-", "ال"), ("эл-", "ال"),
    ("al-", "ال"), ("el-", "ال"), ("ul-", "ال"),
    ("дж", "ج"), ("кх", "خ"), ("гх", "غ"),
    ("sh", "ش"), ("kh", "خ"), ("gh", "غ"), ("th", "ث"),
    ("dh", "ذ"), ("zh", "ج"), ("ch", "ش"),
    ("aa", "ا"), ("ii", "ي"), ("uu", "و"), ("oo", "و"),
    ("ий", "ي"), ("ия", "ي"),
    ("ш", "ش"), ("щ", "ش"), ("ч", "ش"),
]

_TRANSLIT_SINGLE: dict[str, str] = {
    # Кириллица
    "а": "ا", "б": "ب", "в": "و", "г": "غ", "д": "د",
    "е": "ي", "ё": "و", "ж": "ج", "з": "ز", "и": "ي",
    "й": "ي", "к": "ك", "л": "ل", "м": "م", "н": "ن",
    "о": "و", "п": "ب", "р": "ر", "с": "س", "т": "ت",
    "у": "و", "ф": "ف", "х": "ح", "ц": "س",
    "ы": "ي", "э": "ا", "ю": "و", "я": "ا",
    "ь": "", "ъ": "", "'": "", "’": "", "`": "", "ʼ": "",
    # Латиница
    "a": "ا", "b": "ب", "c": "ك", "d": "د", "e": "ي",
    "f": "ف", "g": "غ", "h": "ح", "i": "ي", "j": "ج",
    "k": "ك", "l": "ل", "m": "م", "n": "ن", "o": "و",
    "p": "ب", "q": "ق", "r": "ر", "s": "س", "t": "ت",
    "u": "و", "v": "و", "w": "و", "x": "خ", "y": "ي", "z": "ز",
}


def _has_arabic(text: str) -> bool:
    """True, если в строке есть хотя бы одна арабская буква."""
    return any("\u0621" <= ch <= "\u064A" for ch in text)


def _transliterate_to_arabic(text: str) -> str:
    """Best-effort: переводит кириллицу/латиницу в арабскую графику."""
    if not text:
        return ""
    s = text.lower()
    for src, dst in _TRANSLIT_MULTI:
        s = s.replace(src, dst)
    out: list[str] = []
    for ch in s:
        if "\u0621" <= ch <= "\u064A":
            out.append(ch)
        elif ch.isspace():
            out.append(" ")
        elif ch in _TRANSLIT_SINGLE:
            out.append(_TRANSLIT_SINGLE[ch])
        # остальное (цифры, пунктуация) выкидываем
    return "".join(out)


def _parse_manual_request(text: str) -> Optional[dict]:
    """Ручной запрос: '<сура>' / '<сура> <аят>' / '<сура> <от>-<до>' /
    '<сура>:<аят>' / '<сура>:<от>-<до>'.

    Возвращает {"sura": N, "ayahs": [...]} или None, если строка не похожа
    на ручной запрос. Аяты валидируются по корпусу. Без указания аята
    подставляем полный диапазон 1..max_ayah.
    """
    if not text or not _QURAN_DATA:
        return None
    s = text.strip().replace(":", " ")
    parts = s.split()
    if not parts:
        return None

    try:
        sura_n = int(parts[0])
    except ValueError:
        return None
    if not (1 <= sura_n <= 114):
        return None

    sura_obj = next((x for x in _QURAN_DATA if x["n"] == sura_n), None)
    if not sura_obj:
        return None
    max_ayah = max(a["n"] for a in sura_obj["a"])

    # Только сура — отдаём весь диапазон, в подписи будет «1-max».
    if len(parts) == 1:
        return {"sura": sura_n, "ayahs": [1, max_ayah]}
    # Лишние слова после '<сура> <аят>' — это уже не ручной запрос.
    if len(parts) > 2:
        return None

    ayah_spec = parts[1]
    if "-" in ayah_spec:
        try:
            a_str, b_str = ayah_spec.split("-", 1)
            a, b = int(a_str), int(b_str)
        except ValueError:
            return None
        if a > b:
            a, b = b, a
        if a < 1 or b > max_ayah:
            return None
        return {"sura": sura_n, "ayahs": list(range(a, b + 1))}

    try:
        a = int(ayah_spec)
    except ValueError:
        return None
    if not (1 <= a <= max_ayah):
        return None
    return {"sura": sura_n, "ayahs": [a]}


def _resolve_search_text(raw: str) -> str:
    """Готовит текст для поиска по корпусу.

    • Если есть арабские буквы — нормализуем как обычно (этот путь надёжный).
    • Иначе пробуем транслит → нормализация. Менее надёжно, но шанс есть.
    """
    if not raw:
        return ""
    if _has_arabic(raw):
        return _normalize_arabic(raw)
    return _normalize_arabic(_transliterate_to_arabic(raw))


def _load_quran() -> None:
    """Лениво загружаем JSON Корана и строим индекс. Один раз за процесс."""
    global _QURAN_DATA, _NGRAM_INDEX
    if _QURAN_DATA:
        return
    try:
        with open(_DATA_PATH, "r", encoding="utf-8") as f:
            _QURAN_DATA = json.load(f)
    except Exception as e:
        logging.error(f"[quran] не удалось загрузить quran_data.json: {e}")
        return

    index: dict[str, list[tuple[int, int]]] = defaultdict(list)
    for sura in _QURAN_DATA:
        sn = sura["n"]
        for ayah in sura["a"]:
            an = ayah["n"]
            words = ayah["t"].split()
            if not words:
                continue
            if len(words) < NGRAM:
                # Очень короткие аяты индексируем как единый ключ из всех слов
                key = " ".join(words)
                index[key].append((sn, an))
                continue
            for i in range(len(words) - NGRAM + 1):
                key = " ".join(words[i:i + NGRAM])
                index[key].append((sn, an))
    _NGRAM_INDEX = dict(index)
    total_ayahs = sum(len(s["a"]) for s in _QURAN_DATA)
    logging.info(
        f"[quran] загружено: {len(_QURAN_DATA)} сур, {total_ayahs} аятов, "
        f"{len(_NGRAM_INDEX)} n-грамм"
    )


# === Транскрибация ===
async def _transcribe_groq(file_path: str) -> str:
    if not GROQ_API_KEY:
        return ""
    try:
        timeout = aiohttp.ClientTimeout(total=GROQ_TIMEOUT)
        with open(file_path, "rb") as f:
            data = aiohttp.FormData()
            data.add_field("file", f, filename="audio.mp3", content_type="audio/mpeg")
            data.add_field("model", GROQ_MODEL)
            data.add_field("language", "ar")
            data.add_field("response_format", "json")
            data.add_field("temperature", "0")
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    GROQ_URL,
                    headers={"Authorization": f"Bearer {GROQ_API_KEY}"},
                    data=data,
                ) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logging.warning(f"[quran] Groq HTTP {resp.status}: {body[:200]}")
                        return ""
                    payload = await resp.json()
                    return (payload.get("text") or "").strip()
    except Exception as e:
        logging.warning(f"[quran] Groq ошибка: {e}")
        return ""


async def _transcribe_hf(file_path: str) -> str:
    if not HF_TOKEN:
        return ""
    try:
        with open(file_path, "rb") as f:
            audio_bytes = f.read()
        timeout = aiohttp.ClientTimeout(total=HF_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                HF_MODEL_URL,
                headers={
                    "Authorization": f"Bearer {HF_TOKEN}",
                    "Content-Type": "audio/mpeg",
                },
                data=audio_bytes,
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logging.warning(f"[quran] HF HTTP {resp.status}: {body[:200]}")
                    return ""
                payload = await resp.json()
                if isinstance(payload, dict):
                    return (payload.get("text") or "").strip()
                if isinstance(payload, list) and payload and isinstance(payload[0], dict):
                    return (payload[0].get("text") or "").strip()
                return ""
    except Exception as e:
        logging.warning(f"[quran] HF ошибка: {e}")
        return ""


async def _transcribe(file_path: str) -> str:
    text = await _transcribe_groq(file_path)
    if text:
        return text
    return await _transcribe_hf(file_path)


# === Поиск аятов ===
def _find_best_match(transcript_norm: str) -> Optional[dict]:
    """Выбирает ОДНУ наиболее вероятную суру по количеству совпадений
    4-грамм с транскриптом и возвращает её вместе со списком аятов,
    в которых эти 4-граммы встретились.

    Возвращает {"sura": N, "ayahs": [a1, a2, ...]} или None.

    Раньше отдавали все суры, где было хоть одно совпадение, и из-за этого
    «спамило» — общие формулы вроде «الله رب العالمين» матчатся в десятках
    сур. Теперь побеждает одна — та, где наибольшее число РАЗНЫХ 4-грамм
    транскрипта, и при равенстве — где больше суммарных попаданий внутри.
    """
    if not transcript_norm or not _NGRAM_INDEX:
        return None
    words = transcript_norm.split()
    if not words:
        return None

    # sura_ngrams[s] — множество индексов 4-грамм транскрипта, попавших в суру s
    # sura_ayah_hits[s][a] — счётчик попаданий в конкретный аят
    sura_ngrams: dict[int, set[int]] = defaultdict(set)
    sura_ayah_hits: dict[int, dict[int, int]] = defaultdict(
        lambda: defaultdict(int)
    )

    if len(words) < NGRAM:
        keys = [(0, " ".join(words))]
    else:
        keys = [
            (i, " ".join(words[i:i + NGRAM]))
            for i in range(len(words) - NGRAM + 1)
        ]

    for ngram_idx, key in keys:
        for (sn, an) in _NGRAM_INDEX.get(key, []):
            sura_ngrams[sn].add(ngram_idx)
            sura_ayah_hits[sn][an] += 1

    if not sura_ngrams:
        return None

    # Порог надёжности: для длинных транскриптов нужно ≥2 разных n-грамм,
    # для коротких (<= 8 слов) хватит и одного — иначе вообще ничего не найдём.
    short_input = len(words) <= 8
    min_hits = 1 if short_input else 2
    qualified = {s: ng for s, ng in sura_ngrams.items() if len(ng) >= min_hits}
    if not qualified:
        return None

    # Победитель: больше уникальных 4-грамм, при равенстве — больше попаданий.
    best_sura = max(
        qualified.keys(),
        key=lambda s: (
            len(qualified[s]),
            sum(sura_ayah_hits[s].values()),
        ),
    )
    best_score = len(qualified[best_sura])

    # Топ-3 для лога — на случай, если придётся объяснять выбор.
    top = sorted(
        sura_ngrams.items(), key=lambda kv: len(kv[1]), reverse=True
    )[:3]
    logging.info(
        "[quran] best=%s score=%s top=%s",
        best_sura,
        best_score,
        [(s, len(ng)) for s, ng in top],
    )

    ayahs = _refine_ayahs(sura_ayah_hits[best_sura])
    return {"sura": best_sura, "ayahs": ayahs}


def _refine_ayahs(ayah_hits: dict[int, int]) -> list[int]:
    """Из набора {аят: число_попаданий} выбирает ОДИН плотный кластер
    подряд идущих аятов и отбрасывает шумовые единичные совпадения.

    Зачем: общие фразы вроде «الله رب العالمين» 4-граммой попадают в
    десятки случайных аятов сразу. Раньше мы возвращали min..max от ВСЕХ
    попавших, поэтому диапазон постоянно врал (например 1-87 вместо 5-7).
    Теперь:
      1. Сортируем аяты с попаданиями.
      2. Группируем в кластеры — новый кластер начинаем при разрыве > GAP.
      3. Берём кластер с максимальной суммой попаданий, при равенстве — с
         наибольшим числом аятов (выбор по плотности recitation, а не по
         случайному шуму).
      4. Возвращаем отсортированный список аятов кластера; вызывающий
         сделает min..max.
    """
    if not ayah_hits:
        return []
    sorted_ayahs = sorted(ayah_hits.keys())
    if len(sorted_ayahs) == 1:
        return sorted_ayahs

    GAP = 3   # допустимый «провал» внутри одного кластера (в номерах аятов)
    clusters: list[list[int]] = [[sorted_ayahs[0]]]
    for a in sorted_ayahs[1:]:
        if a - clusters[-1][-1] <= GAP:
            clusters[-1].append(a)
        else:
            clusters.append([a])

    def score(cluster: list[int]) -> tuple[int, int]:
        return (sum(ayah_hits[a] for a in cluster), len(cluster))

    best = max(clusters, key=score)
    return best


def _format_ayah_range(ayahs: list[int]) -> str:
    if not ayahs:
        return "?"
    if len(ayahs) == 1:
        return str(ayahs[0])
    return f"{min(ayahs)}-{max(ayahs)}"


def _pick_reciter(forced: Optional[dict] = None) -> dict:
    """Возвращает запись чтеца. Если задан — ровно его, иначе случайного
    из тех, у кого есть полный CDN. Используется только для отправки аудио;
    для текстового режима чтец не нужен и эта функция не вызывается."""
    if forced is not None:
        return forced
    available = [r for r in RECITERS if r["url"] and r["suras"] is None]
    return random.choice(available)


def _reciter_has_sura(rec: dict, sura_n: int) -> bool:
    """True, если у чтеца есть указанная сура (CDN + список доступных)."""
    if not rec.get("url"):
        return False
    suras = rec.get("suras")
    if suras is None:        # None = весь Коран
        return 1 <= sura_n <= 114
    return sura_n in suras


def _format_one_caption(
    sura: int,
    ayahs: list[int],
    reciter_name: str,
) -> str:
    """HTML-цитата с метаданными для аудио-сообщения (сура+аят+чтец)."""
    body = (
        f"{EMOJI_SURA}| Сура - {sura}\n"
        f"{EMOJI_AYAH}| Аят - {_format_ayah_range(ayahs)}\n"
        f"{EMOJI_RECITER}| Чтец - {reciter_name}"
    )
    return f"<blockquote>{body}</blockquote>"


# Подпись для режима «распознал по аудио/видео/гс/кружку» — без чтеца и без
# самого аудио. Третья строка предупреждает, что распознавание ненадёжное.
WARN_LINE = (
    "⚠️| Данные могут быть неправильными, "
    "пере проверяйте всё вручную для точности!"
)


def _format_text_only_caption(sura: int, ayahs: list[int]) -> str:
    body = (
        f"{EMOJI_SURA}| Сура - {sura}\n"
        f"{EMOJI_AYAH}| Аят - {_format_ayah_range(ayahs)}\n"
        f"{WARN_LINE}"
    )
    return f"<blockquote>{body}</blockquote>"


def _format_no_sura_for_reciter(rec: dict) -> str:
    """Сообщение, когда запрошена сура, которой нет у выбранного чтеца.
    Формат строго по запросу пользователя (HTML-эмодзи в начале)."""
    primary = _primary_alias(rec)
    return (
        f'<tg-emoji emoji-id="5208647293879721534">❌</tg-emoji>| '
        f"Нет этой суры в исполнении {rec['name']}, посмотрите доступный "
        f"список сур в его исполнении написав боту .{primary}"
    )


def _extract_command_arg(text: str) -> str:
    """Возвращает всё, что идёт ПОСЛЕ первого слова (команды).

    Поддерживает: '/quran мишари', '.коран ясир', '/quran@bot люхайдан'.
    """
    if not text:
        return ""
    parts = text.strip().split(None, 1)
    if len(parts) < 2:
        return ""
    return parts[1].strip()


# === Утилиты для аудио ===
def _has_ffmpeg() -> bool:
    return shutil.which("ffmpeg") is not None


def _pick_source_file_id(reply: Message) -> tuple[Optional[str], str]:
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
            logging.warning(f"[quran] файл слишком большой: {f.file_size}")
            return False
        await bot.download_file(f.file_path, destination=dst_path)
        return os.path.exists(dst_path) and os.path.getsize(dst_path) > 0
    except Exception as e:
        logging.warning(f"[quran] download_file: {e}")
        return False


async def _shrink_audio(src_path: str, dst_path: str) -> bool:
    """Пережимает mp3 в очень низкий битрейт (32 kbps mono, 22050 Hz),
    чтобы длинные суры (Аль-Бакара ~90+ МБ оригинал) уместились в лимит
    Telegram Bot API на отправку (50 МБ). Битрейт выбран компромиссно:
    32k mono у Lame даёт разборчивую речь — для чтения Корана этого хватает,
    а час записи весит ~14 МБ. Возвращает True при успехе и непустом файле."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-loglevel", "error", "-i", src_path,
            "-vn", "-ac", "1", "-ar", "22050",
            "-c:a", "libmp3lame", "-b:a", "32k",
            dst_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=180)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return False
        if proc.returncode != 0:
            tail = (stderr or b"").decode("utf-8", errors="ignore")[-300:]
            logging.warning(f"[quran] ffmpeg shrink: {tail}")
            return False
        return os.path.exists(dst_path) and os.path.getsize(dst_path) > 0
    except Exception as e:
        logging.warning(f"[quran] ffmpeg shrink exception: {e}")
        return False


async def _to_mp3_16k_mono(src_path: str, dst_path: str) -> bool:
    """16kHz mono mp3 — оптимально для Whisper и сильно меньше по объёму."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-loglevel", "error", "-i", src_path,
            "-vn", "-ac", "1", "-ar", "16000",
            "-c:a", "libmp3lame", "-q:a", "5",
            dst_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=60)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return False
        if proc.returncode != 0:
            tail = (stderr or b"").decode("utf-8", errors="ignore")[-300:]
            logging.warning(f"[quran] ffmpeg: {tail}")
            return False
        return os.path.exists(dst_path) and os.path.getsize(dst_path) > 0
    except Exception as e:
        logging.warning(f"[quran] ffmpeg exception: {e}")
        return False


async def _delete_msg(bot: Bot, chat_id: int, message_id: int,
                      bc_id: Optional[str]) -> bool:
    """Удаляет одно сообщение. В business-чате обязательно используется
    delete_business_messages (только он работает для удаления сообщений,
    отправленных через бизнес-подключение). См. fun.delete_command для
    проверенного шаблона."""
    try:
        if bc_id:
            await bot.delete_business_messages(
                business_connection_id=bc_id,
                message_ids=[message_id],
            )
        else:
            await bot.delete_message(chat_id=chat_id, message_id=message_id)
        return True
    except Exception as e:
        logging.warning(f"[quran] не удалось удалить msg {message_id}: {e}")
        return False


async def _delete_after(bot: Bot, chat_id: int, message_id: int,
                        bc_id: Optional[str], delay: float):
    try:
        await asyncio.sleep(delay)
        await _delete_msg(bot, chat_id, message_id, bc_id)
    except Exception:
        pass


async def _send_temp_error(bot: Bot, chat_id: int, bc_id: Optional[str]):
    try:
        sent = await bot.send_message(
            chat_id, ERROR_TEXT, parse_mode="HTML",
            business_connection_id=bc_id,
        )
        asyncio.create_task(
            _delete_after(bot, chat_id, sent.message_id, bc_id, ERROR_AUTO_DELETE_SEC)
        )
    except Exception as e:
        logging.warning(f"[quran] _send_temp_error: {e}")


async def _send_one_sura_audio(
    bot: Bot,
    chat_id: int,
    bc_id: Optional[str],
    reply_to: int,
    sura_n: int,
    sura_url: str,
    caption: str,
    reciter_name: str,
) -> bool:
    """Шлёт ОДНО аудио с одной сурой и подписью. Сначала пробует по URL,
    потом скачивает и шлёт файлом. Возвращает True, если что-то отправилось
    (включая текстовый фолбэк)."""
    # 1) По URL — быстро
    try:
        await bot.send_audio(
            chat_id,
            sura_url,
            caption=caption,
            parse_mode="HTML",
            title=f"Сура {sura_n}",
            performer=reciter_name,
            business_connection_id=bc_id,
            reply_to_message_id=reply_to,
        )
        return True
    except Exception as e:
        logging.warning(
            f"[quran] send_audio по URL для суры {sura_n} не удалось: {e}, "
            f"скачиваю вручную"
        )

    # 2) Скачиваем mp3, при необходимости — пережимаем в низкий битрейт
    # («план Б» для длинных сур вроде Аль-Бакары — оригинал 90+ МБ,
    # после re-encode в 32 kbps mono ~12 МБ, спокойно лезет в лимит).
    tmp_dir: Optional[str] = None
    try:
        tmp_dir = tempfile.mkdtemp(prefix="quran_send_")
        raw_path = os.path.join(tmp_dir, f"sura_{sura_n:03d}_raw.mp3")
        out_path = raw_path

        timeout = aiohttp.ClientTimeout(total=180)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.get(sura_url) as r:
                if r.status != 200:
                    raise RuntimeError(f"HTTP {r.status}")
                with open(raw_path, "wb") as f:
                    while True:
                        chunk = await r.content.read(64 * 1024)
                        if not chunk:
                            break
                        f.write(chunk)

        size = os.path.getsize(raw_path)
        if size > MAX_AUDIO_OUT_BYTES:
            shrunk_path = os.path.join(tmp_dir, f"sura_{sura_n:03d}.mp3")
            shrunk_ok = await _shrink_audio(raw_path, shrunk_path)
            if not shrunk_ok:
                raise RuntimeError(
                    f"файл слишком большой ({size} б) и пережать не вышло"
                )
            out_path = shrunk_path
            new_size = os.path.getsize(out_path)
            logging.info(
                f"[quran] re-encode сура {sura_n}: {size} → {new_size} байт"
            )
            if new_size > MAX_AUDIO_OUT_BYTES:
                raise RuntimeError(
                    f"даже после пережатия файл больше лимита: {new_size}"
                )

        with open(out_path, "rb") as f:
            audio_bytes = f.read()

        await bot.send_audio(
            chat_id,
            BufferedInputFile(audio_bytes, filename=f"sura_{sura_n:03d}.mp3"),
            caption=caption,
            parse_mode="HTML",
            title=f"Сура {sura_n}",
            performer=reciter_name,
            business_connection_id=bc_id,
            reply_to_message_id=reply_to,
        )
        return True
    except Exception as e2:
        logging.error(
            f"[quran] фолбэк-аудио для суры {sura_n} не удался: {e2}"
        )
    finally:
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    # 3) Совсем плохо — отправим хотя бы текстовую цитату
    try:
        await bot.send_message(
            chat_id,
            caption,
            parse_mode="HTML",
            business_connection_id=bc_id,
            reply_to_message_id=reply_to,
        )
        return True
    except Exception as e3:
        logging.error(
            f"[quran] не удалось отправить даже текст для суры {sura_n}: {e3}"
        )
        return False


async def cmd_quran(message: Message, bot: Bot):
    """Команда `/quran` (и алиасы `.коран` / `.сура` / `.аят`).

    Источник для распознавания (в порядке приоритета):
      1. Реплай на голосовое / аудио / видео — транскрибируем (Groq → HF).
      2. Реплай на текстовое сообщение — берём текст реплая.
      3. Текст после самой команды (например «/quran الحمد لله رب»).

    Поддерживается ручной выбор чтеца — первое слово после команды
    («/quran мишари», «.коран ясир الحمد لله»). Команда уже удалена
    в bot.py до запуска этой задачи.
    """
    chat_id = message.chat.id
    bc_id = message.business_connection_id
    reply = message.reply_to_message

    _load_quran()
    if not _NGRAM_INDEX:
        await _send_temp_error(bot, chat_id, bc_id)
        return

    # Парсим аргумент команды: чтец (если первые 1..3 слова — алиас) + остаток.
    raw_arg = _extract_command_arg(message.text or "")
    forced_rec, arg_text = _split_reciter_and_text(raw_arg)

    async def _send_audio_or_error(sura_n: int, ayahs: list[int],
                                   reply_to: Optional[int]) -> bool:
        """Шлёт аудио выбранным/случайным чтецом или фирменный отказ,
        если у запрошенного чтеца нет этой суры. Возвращает True при успехе."""
        if forced_rec is not None and not _reciter_has_sura(forced_rec, sura_n):
            await bot.send_message(
                chat_id,
                _format_no_sura_for_reciter(forced_rec),
                parse_mode="HTML",
                business_connection_id=bc_id,
                reply_to_message_id=reply_to,
            )
            return True
        rec = _pick_reciter(forced_rec)
        sura_url = rec["url"].format(n=f"{sura_n:03d}")
        caption = _format_one_caption(sura_n, ayahs, rec["name"])
        return await _send_one_sura_audio(
            bot=bot, chat_id=chat_id, bc_id=bc_id,
            reply_to=reply_to, sura_n=sura_n, sura_url=sura_url,
            caption=caption, reciter_name=rec["name"],
        )

    # === Ручной режим: '/quran <сура> [<аят>|<от>-<до>]' ===
    manual = _parse_manual_request(arg_text)
    if manual is not None:
        ok = await _send_audio_or_error(
            manual["sura"], manual["ayahs"],
            reply.message_id if reply else None,
        )
        if not ok:
            await _send_temp_error(bot, chat_id, bc_id)
        return

    # === Источник для распознавания ===
    source_type: Optional[str] = None     # "audio" | "text"
    file_id: Optional[str] = None
    text_input: str = ""
    reply_to: Optional[int] = reply.message_id if reply else None

    if reply:
        fid, _ = _pick_source_file_id(reply)
        if fid:
            source_type = "audio"
            file_id = fid
        else:
            replied_text = (reply.text or reply.caption or "").strip()
            if replied_text:
                source_type = "text"
                text_input = replied_text

    if source_type is None and arg_text:
        source_type = "text"
        text_input = arg_text
        reply_to = None

    if source_type is None:
        await _send_temp_error(bot, chat_id, bc_id)
        return

    if source_type == "audio":
        if not _has_ffmpeg():
            logging.error("[quran] ffmpeg отсутствует на сервере")
            await _send_temp_error(bot, chat_id, bc_id)
            return
        if not GROQ_API_KEY and not HF_TOKEN:
            logging.error("[quran] не задан ни GROQ_API_KEY, ни HF_TOKEN")
            await _send_temp_error(bot, chat_id, bc_id)
            return

    progress_msg = None
    try:
        progress_msg = await bot.send_message(
            chat_id, PROGRESS_TEXT, parse_mode="HTML",
            business_connection_id=bc_id,
        )
    except Exception as e:
        logging.warning(f"[quran] не удалось отправить прогресс: {e}")

    async def _delete_progress():
        if progress_msg is None:
            return
        await _delete_msg(bot, chat_id, progress_msg.message_id, bc_id)

    async def _fail():
        await _delete_progress()
        await _send_temp_error(bot, chat_id, bc_id)

    tmpdir: Optional[str] = None
    try:
        norm = ""
        if source_type == "audio":
            tmpdir = tempfile.mkdtemp(prefix="quran_")
            src_path = os.path.join(tmpdir, "input.bin")
            mp3_path = os.path.join(tmpdir, "audio.mp3")

            if not await _download_file(bot, file_id, src_path):
                await _fail()
                return
            if not await _to_mp3_16k_mono(src_path, mp3_path):
                await _fail()
                return

            transcript = await _transcribe(mp3_path)
            logging.info(
                f"[quran] транскрипт ({len(transcript)} симв.): "
                f"{transcript[:200]}"
            )
            norm = _normalize_arabic(transcript)
        else:  # text
            logging.info(
                f"[quran] вход (текст, {len(text_input)} симв.): "
                f"{text_input[:200]}"
            )
            norm = _resolve_search_text(text_input)

        if not norm:
            await _fail()
            return

        best = _find_best_match(norm)
        if not best:
            await _fail()
            return

        sura_n = best["sura"]
        ayahs = best["ayahs"]

        await _delete_progress()

        # Распознали по медиа (видео/гс/аудио/кружок) — отдаём ТОЛЬКО текст.
        # Аудио и чтеца не шлём (по запросу пользователя).
        if source_type == "audio":
            try:
                await bot.send_message(
                    chat_id,
                    _format_text_only_caption(sura_n, ayahs),
                    parse_mode="HTML",
                    business_connection_id=bc_id,
                    reply_to_message_id=reply_to,
                )
            except Exception as e:
                logging.error(f"[quran] не удалось отправить текст-карточку: {e}")
                await _send_temp_error(bot, chat_id, bc_id)
            return

        # Распознали по тексту — отдаём аудио (или фирменный отказ).
        ok = await _send_audio_or_error(sura_n, ayahs, reply_to)
        if not ok:
            await _send_temp_error(bot, chat_id, bc_id)
    finally:
        try:
            shutil.rmtree(tmpdir, ignore_errors=True)
        except Exception:
            pass


# === PM-команды: «список чтецов» и «список сур чтеца X» ===

def _format_reciters_list_text() -> str:
    """HTML-цитата с пронумерованным списком всех чтецов."""
    lines = [f"{i + 1}. {r['name']}" for i, r in enumerate(RECITERS)]
    body = "Чтецы:\n" + "\n".join(lines)
    return f"<blockquote>{body}</blockquote>"


def _format_reciter_suras_text(rec: dict) -> str:
    """HTML-цитата со списком доступных сур у конкретного чтеца."""
    if rec["suras"] is None:
        body = (
            f"{rec['name']} — есть весь Коран (1–114).\n"
            f"Можно вызвать как: /quran <сура> {_primary_alias(rec)}"
        )
    elif not rec["suras"]:
        body = (
            f"{rec['name']} — пока нет ни одной суры в открытых источниках.\n"
            f"Когда добавим, появится здесь."
        )
    else:
        nums = ", ".join(str(n) for n in rec["suras"])
        body = (
            f"{rec['name']} — доступные суры:\n{nums}\n\n"
            f"Вызов: /quran <сура> {_primary_alias(rec)}"
        )
    return f"<blockquote>{body}</blockquote>"


async def cmd_reciters_list(message: Message, bot: Bot) -> None:
    """Команда вида `/чтецы`, `чтецы`, `список чтецов` в личке бота."""
    try:
        await bot.send_message(
            message.chat.id,
            _format_reciters_list_text(),
            parse_mode="HTML",
        )
    except Exception as e:
        logging.warning(f"[quran] cmd_reciters_list: {e}")


async def cmd_reciter_suras(message: Message, bot: Bot, rec: dict) -> None:
    """Ответ в ЛС на `.<алиас>` — список сур у чтеца."""
    try:
        await bot.send_message(
            message.chat.id,
            _format_reciter_suras_text(rec),
            parse_mode="HTML",
        )
    except Exception as e:
        logging.warning(f"[quran] cmd_reciter_suras: {e}")
