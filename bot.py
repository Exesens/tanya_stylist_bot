#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram bot for stylist/makeup artist (Кириллова Татьяна).
Functions: greeting, services & prices, contacts, bookings (inline calendar + smart slots w/ Google Calendar), reviews,
and Google Sheets logging (Записи + Статистика).

Deps:
pip install python-telegram-bot==21.4 python-dotenv google-api-python-client google-auth google-auth-httplib2 pytz gspread
"""

import os
import json
import csv
import logging
import urllib.parse
from datetime import datetime, timedelta, date as date_cls
from calendar import monthrange
from dotenv import load_dotenv

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)
from telegram.request import HTTPXRequest
import asyncio
from telegram.error import TimedOut, NetworkError, BadRequest

# --- Google Calendar ---
from google.oauth2 import service_account
from googleapiclient.discovery import build
import pytz

# --- Google Sheets ---
import gspread
from gspread.exceptions import SpreadsheetNotFound, APIError, WorksheetNotFound

# ---------- Logging ----------
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ---------- Config ----------
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")

# Google Calendar config
GCAL_ID = os.getenv("GOOGLE_CALENDAR_ID")
GCAL_SA_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")
TZ = os.getenv("TZ", "Europe/Moscow")

# Google Sheets config
GSHEET_ID = os.getenv("GSHEET_ID")

# ---------- Booking rules ----------
WORK_START = "08:00"       # старт рабочего дня
WORK_END   = "18:00"       # конец рабочего дня (старт в 18:00 допускается как крайний)
SLOT_STEP_MIN = 15         # шаг предлагаемых слотов
BUFFER_MIN    = 15         # минимальный перерыв между записями

# Brand info
BRAND = {
    "owner_fullname": "Кириллова Татьяна",
    "brand_short": "MAKEUP ROOM",
    "about": (
        "Привет! Я Татьяна, стилист и визажист. Делаю макияж и укладки для фотосессий, "
        "свадеб, выпускных и мероприятий."
    ),
    "phone": "+79159998556",
    "whatsapp": "+79159998556",
    "instagram": "@kiselevamake",
    "telegram_username": "tanya_kir30",  # без @
    "city": "Ярославль",
    "address": 'гор. Ярославль, проспект Октября, д. 42, офис 101, 1 этаж, студия "MAKEUP ROOM"',
}

# Services & prices (длительность парсится автоматически)
SERVICES = [
    {"name": "Макияж дневной", "price": 60, "currency": "€", "duration": "60–75 мин"},
    {"name": "Макияж вечерний/для съемки", "price": 80, "currency": "€", "duration": "75–90 мин"},
    {"name": "Свадебный макияж (репетиция отдельно)", "price": 130, "currency": "€", "duration": "90–120 мин"},
    {"name": "Укладка (волны/локоны/объем)", "price": 60, "currency": "€", "duration": "45–75 мин"},
    {"name": "Свадебная прическа", "price": 120, "currency": "€", "duration": "90–120 мин"},
    {"name": "Коррекция/оформление бровей", "price": 30, "currency": "€", "duration": "30 мин"},
]

# Paths
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data")
REVIEWS_JSON = os.path.join(DATA_DIR, "reviews.json")
BOOKINGS_CSV = os.path.join(DATA_DIR, "bookings.csv")

# Ensure data files exist
os.makedirs(DATA_DIR, exist_ok=True)
if not os.path.exists(REVIEWS_JSON):
    with open(REVIEWS_JSON, "w", encoding="utf-8") as f:
        json.dump([], f, ensure_ascii=False, indent=2)
if not os.path.exists(BOOKINGS_CSV):
    with open(BOOKINGS_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp", "username", "name", "phone", "service", "date", "time", "notes", "eventId", "htmlLink"
        ])

# ---------- Admin helpers ----------
def _parse_admin_ids(env_value: str) -> list[int]:
    ids: list[int] = []
    if not env_value:
        return ids
    for part in str(env_value).split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ids.append(int(part))
        except ValueError:
            logger.warning("ADMIN_CHAT_ID содержит нечисловое значение: %r", part)
    return ids

ADMIN_IDS = _parse_admin_ids(ADMIN_CHAT_ID)

async def send_admin(context: ContextTypes.DEFAULT_TYPE, text: str):
    if not ADMIN_IDS:
        logger.info("ADMIN_CHAT_ID не задан — уведомление не отправлено.")
        return
    for cid in ADMIN_IDS:
        try:
            await context.bot.send_message(chat_id=cid, text=text)
            logger.info("Админу %s отправлено уведомление.", cid)
        except Exception as e:
            logger.exception("Не удалось отправить админу %s: %s", cid, e)

# ---------- Safe helpers ----------
async def safe_answer_callback(query):
    """Подтверждает callback и игнорирует 'Query is too old...' ошибки."""
    try:
        await query.answer(cache_time=0)
    except BadRequest as e:
        msg = str(e)
        if "Query is too old" in msg or "query id is invalid" in msg:
            logger.warning("Stale callback ignored: %s", msg)
        else:
            raise

async def safe_reply_text(message, text, **kwargs):
    delays = [0, 1.5, 3.0]
    last_exc = None
    for d in delays:
        if d:
            await asyncio.sleep(d)
        try:
            return await message.reply_text(text, **kwargs)
        except (TimedOut, NetworkError) as e:
            last_exc = e
            logger.warning("reply_text timeout/network error, retrying: %s", e)
    logger.exception("reply_text failed after retries: %s", last_exc)
    return None

async def safe_edit_message_text(target_with_edit, text, **kwargs):
    """
    Надёжный edit_message_text с ретраями.
    target_with_edit — объект с методом edit_message_text (обычно CallbackQuery).
    """
    delays = [0, 1.5, 3.0]
    last_exc = None
    for d in delays:
        if d:
            await asyncio.sleep(d)
        try:
            return await target_with_edit.edit_message_text(text, **kwargs)
        except (TimedOut, NetworkError) as e:
            last_exc = e
            logger.warning("edit_message_text timeout/network error, retrying: %s", e)
        except BadRequest as e:
            # например, message is not modified → можно молча игнорировать
            if "message is not modified" in str(e).lower():
                return None
            last_exc = e
            logger.warning("edit_message_text BadRequest: %s", e)
            break
    logger.exception("edit_message_text failed after retries: %s", last_exc)
    return None

# ---------- Google Calendar helpers ----------
_calendar_service = None

def get_calendar_service():
    global _calendar_service
    if _calendar_service:
        return _calendar_service
    if not (GCAL_ID and GCAL_SA_FILE and os.path.exists(GCAL_SA_FILE)):
        logger.warning("Google Calendar не настроен (нет GOOGLE_CALENDAR_ID или файла ключа).")
        return None
    scopes = ["https://www.googleapis.com/auth/calendar"]
    creds = service_account.Credentials.from_service_account_file(GCAL_SA_FILE, scopes=scopes)
    _calendar_service = build("calendar", "v3", credentials=creds, cache_discovery=False)
    return _calendar_service

def _extract_minutes_from_duration_string(duration_str: str) -> int:
    """'60–75 мин' -> среднее (округляем), '30 мин' -> 30, иначе 60."""
    try:
        s = duration_str.replace(" ", "")
        nums = []
        num = ""
        for ch in s:
            if ch.isdigit():
                num += ch
            else:
                if num:
                    nums.append(int(num))
                    num = ""
        if num:
            nums.append(int(num))
        if not nums:
            return 60
        if len(nums) == 1:
            return nums[0]
        return round(sum(nums[:2]) / 2)
    except Exception:
        return 60

def _service_duration_minutes(service_name: str) -> int:
    for s in SERVICES:
        if s["name"] == service_name:
            return _extract_minutes_from_duration_string(s.get("duration", "")) or 60
    return 60

def _service_price(service_name: str) -> float:
    for s in SERVICES:
        if s["name"] == service_name:
            return float(s.get("price", 0))
    return 0.0

def _tz() -> pytz.timezone:
    return pytz.timezone(TZ)

def _dt_on(date_obj: date_cls, hhmm: str) -> datetime:
    h, m = map(int, hhmm.split(":"))
    return _tz().localize(datetime(date_obj.year, date_obj.month, date_obj.day, h, m))

def _iso(dt: datetime) -> str:
    return dt.isoformat()

def list_busy_intervals(date_obj: date_cls) -> list[tuple[datetime, datetime]]:
    """Считываем события на день и возвращаем список занятых интервалов с учётом BUFFER_MIN."""
    service = get_calendar_service()
    if not service:
        return []
    start_day = _dt_on(date_obj, "00:00")
    end_day = _dt_on(date_obj, "23:59")
    try:
        items = service.events().list(
            calendarId=GCAL_ID,
            timeMin=_iso(start_day),
            timeMax=_iso(end_day),
            singleEvents=True,
            orderBy="startTime",
        ).execute().get("items", [])
        result = []
        for e in items:
            if e.get("status") == "cancelled":
                continue
            st_raw = e["start"].get("dateTime") or (e["start"].get("date") + "T00:00:00")
            en_raw = e["end"].get("dateTime") or (e["end"].get("date") + "T23:59:59")
            st = datetime.fromisoformat(st_raw.replace("Z", "+00:00")).astimezone(_tz())
            en = datetime.fromisoformat(en_raw.replace("Z", "+00:00")).astimezone(_tz())
            # расширяем занятый интервал на BUFFER_MIN до и после
            st_buf = st - timedelta(minutes=BUFFER_MIN)
            en_buf = en + timedelta(minutes=BUFFER_MIN)
            result.append((st_buf, en_buf))
        # мерджим пересекающиеся
        result.sort(key=lambda x: x[0])
        merged = []
        for s, e in result:
            if not merged or s > merged[-1][1]:
                merged.append([s, e])
            else:
                merged[-1][1] = max(merged[-1][1], e)
        return [(s, e) for s, e in merged]
    except Exception as ex:
        logger.exception("Не удалось получить занятость календаря: %s", ex)
        return []

# Создание события (c проверкой коллизий; allow_overlap=True — создавать даже при конфликте)
async def create_event_on_calendar(booking: dict) -> dict | None:
    service = get_calendar_service()
    if not service:
        return None

    duration_min = _service_duration_minutes(booking["service"])
    # booking["date"] у нас в формате "ДД.ММ.ГГГГ", time — "HH:MM"
    start_dt = _tz().localize(datetime.strptime(f"{booking['date']} {booking['time']}", "%d.%m.%Y %H:%M"))
    end_dt = start_dt + timedelta(minutes=duration_min)

    allow_overlap = bool(booking.get("allow_overlap"))
    overlapped = False

    if not allow_overlap:
        # Жёсткая проверка: если пересечение — возвращаем conflict
        for (bs, be) in list_busy_intervals(start_dt.date()):
            if not (end_dt <= bs or start_dt >= be):
                return {
                    "conflict": True,
                    "events": [{
                        "start": {"dateTime": bs.isoformat()},
                        "end": {"dateTime": be.isoformat()},
                        "summary": "Занято"
                    }]
                }
    else:
        # Мягкая проверка: отмечаем, что пересечение есть (для предупреждения)
        for (bs, be) in list_busy_intervals(start_dt.date()):
            if not (end_dt <= bs or start_dt >= be):
                overlapped = True
                break

    summary = f"{booking['service']} — {booking['name']}"
    description = (
        f"Имя: {booking['name']}\n"
        f"Телефон: {booking['phone']}\n"
        f"Комментарий: {booking.get('notes') or '—'}\n"
        f"От бота Telegram (@{booking.get('username', '')})"
    )
    location = BRAND.get("address") or "Студия MAKEUP ROOM"
    event = {
        "summary": summary,
        "description": description,
        "location": location,
        "start": {"dateTime": start_dt.isoformat(), "timeZone": TZ},
        "end": {"dateTime": end_dt.isoformat(), "timeZone": TZ},
        "reminders": {
            "useDefault": False,
            "overrides": [
                {"method": "popup", "minutes": 120},
                {"method": "popup", "minutes": 30},
            ],
        },
    }
    try:
        created = service.events().insert(calendarId=GCAL_ID, body=event).execute()
        result = {"eventId": created.get("id"), "htmlLink": created.get("htmlLink")}
        if overlapped:
            result["overlapped"] = True
        return result
    except Exception as e:
        logger.exception("Ошибка создания события в Google Calendar: %s", e)
        return None

def compute_free_slots_for_date(date_obj: date_cls, needed_minutes: int) -> list[datetime]:
    """
    Считает доступные стартовые моменты (dt) с шагом SLOT_STEP_MIN, учитывая занятость и буфер.
    Особый случай: допускаем старт ровно в WORK_END (18:00), даже если закончится позже.
    """
    day_start = _dt_on(date_obj, WORK_START) + timedelta(minutes=BUFFER_MIN)
    day_end = _dt_on(date_obj, WORK_END)  # старт в 18:00 разрешён

    if day_end <= day_start:
        return []

    busy = list_busy_intervals(date_obj)
    # Свободные интервалы внутри рабочего окна
    free = []
    cursor = day_start
    for (bs, be) in busy:
        if be <= day_start or bs >= day_end:
            continue
        if bs > cursor:
            free.append((cursor, min(bs, day_end)))
        cursor = max(cursor, be)
        if cursor >= day_end:
            break
    if cursor < day_end:
        free.append((cursor, day_end))

    slots = []
    step = timedelta(minutes=SLOT_STEP_MIN)
    need = timedelta(minutes=needed_minutes)
    for (fs, fe) in free:
        cur = fs
        minutes_mod = (cur.minute % SLOT_STEP_MIN)
        if minutes_mod != 0:
            cur = cur + timedelta(minutes=(SLOT_STEP_MIN - minutes_mod))
            cur = cur.replace(second=0, microsecond=0)
        while cur <= fe:  # допускаем cur == fe (например, 18:00)
            if cur > datetime.now(tz=_tz()):
                if cur < fe:
                    if cur + need <= fe:
                        slots.append(cur)
                else:
                    if fe.time().strftime("%H:%M") == WORK_END:
                        slots.append(cur)
            cur += step
    return sorted({s for s in slots})

# ---------- Google Sheets helpers ----------
_gs_client = None
_stats_ready = False  # ленивый флажок инициализации листа "Статистика"

def get_gspread_client():
    global _gs_client
    if _gs_client:
        return _gs_client
    if not (GSHEET_ID and GCAL_SA_FILE and os.path.exists(GCAL_SA_FILE)):
        logger.warning("Google Sheets не настроен (нет GSHEET_ID или файла ключа).")
        return None
    try:
        creds = service_account.Credentials.from_service_account_file(
            GCAL_SA_FILE,
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ],
        )
        _gs_client = gspread.authorize(creds)
        return _gs_client
    except Exception as e:
        logger.exception("Ошибка аутентификации в Google Sheets: %s", e)
        return None

def ensure_headers(ws):
    """Гарантируем заголовки на листе 'Записи' (+ ISO дата и ключ месяца)."""
    headers = [
        "№п/п", "ФИО", "телеграм-ник", "телефон",
        "выбранная услуга", "стоимость услуги",
        "дата визита", "время визита", "продолжительность визита (мин)",
        "комментарий клиента", "дата формирования записи",
        "id события в Google Calendar", "ссылка на событие в календаре",
        "дата визита ISO", "месяц (YYYY-MM)",
    ]
    try:
        existing = ws.row_values(1)
        if existing != headers:
            ws.resize(1)
            ws.update(values=[headers], range_name='A1:O1')
    except Exception as e:
        logger.exception("Ошибка установки заголовков: %s", e)

def ensure_stats_sheet(sh):
    """Создаёт/обновляет лист 'Статистика': итоги + помесячная сводка через QUERY."""
    try:
        try:
            ws = sh.worksheet("Статистика")
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title="Статистика", rows=200, cols=10)

        ws.clear()

        # Итоги (как было)
        ws.update(values=[["Итоги"]], range_name='A1')
        ws.update(values=[["Сумма выручки"]], range_name='A2')
        ws.update(values=[["Суммарные минуты"]], range_name='A3')
        ws.update(values=[["Суммарные часы"]], range_name='A4')

        ws.update(values=[["=SUM(Записи!F2:F)"]], range_name='B2')   # Выручка
        ws.update(values=[["=SUM(Записи!I2:I)"]], range_name='B3')   # Минуты
        ws.update(values=[["=B3/60"]],            range_name='B4')   # Часы

        # Помесячная сводка
        ws.update(values=[["Сводка по месяцам"]], range_name='A6')
        # QUERY по колонкам:
        #   O — месяц (YYYY-MM)
        #   F — стоимость
        #   I — минуты
        query = (
            "=QUERY({Записи!O2:O, Записи!F2:F, Записи!I2:I}, "
            "\"select Col1, sum(Col2), sum(Col3), sum(Col3)/60 "
            "where Col1 is not null "
            "group by Col1 "
            "order by Col1 "
            "label Col1 'Месяц', sum(Col2) 'Выручка', sum(Col3) 'Минуты', sum(Col3)/60 'Часы'\", 0)"
        )
        ws.update(values=[[query]], range_name='A7')

        # Подсказки
        ws.update(values=[["Подсказка"]], range_name='A10')
        ws.update(values=[["• В листе «Записи» колонки N и O используются для расчётов (ISO-дата и ключ месяца)."]], range_name='A11')
        ws.update(values=[["• Здесь можно строить графики на основе таблицы начиная с A7."]], range_name='A12')

    except Exception as e:
        logger.exception("Ошибка обновления листа 'Статистика': %s", e)

def get_gsheet():
    """Возвращает tuple (worksheet 'Записи', spreadsheet) или (None, None)."""
    client = get_gspread_client()
    if not client:
        return None, None
    try:
        sh = client.open_by_key(GSHEET_ID)

        # Лист «Записи»
        try:
            ws = sh.worksheet("Записи")
        except WorksheetNotFound:
            ws = sh.add_worksheet(title="Записи", rows=1000, cols=13)
        ensure_headers(ws)

        # Лист «Статистика» — один раз на запуск
        global _stats_ready
        if not _stats_ready:
            ensure_stats_sheet(sh)
            _stats_ready = True

        return ws, sh

    except SpreadsheetNotFound:
        logger.error("Google Sheets: таблица с ID '%s' не найдена или нет доступа.", GSHEET_ID)
    except APIError as e:
        logger.exception("Ошибка открытия Google Sheets: %s", e)
    except Exception as e:
        logger.exception("Неожиданная ошибка Google Sheets: %s", e)
    return None, None

def append_gsheet_row(booking: dict, event: dict | None):
    """Записывает строку в Google Sheets/Записи. Тихо пропускает, если GSheets не настроен."""
    ws, _ = get_gsheet()
    if not ws:
        return

    # поля
    fio = booking.get("name", "")
    username = booking.get("username", "")
    if username and not username.startswith("@"):
        username = f"@{username}"
    phone = booking.get("phone", "")
    service = booking.get("service", "")
    price = _service_price(service)  # число
    date_visit = booking.get("date", "")       # "ДД.ММ.ГГГГ"
    time_visit = booking.get("time", "")       # "HH:MM"
    dur_min = _service_duration_minutes(service)
    note = booking.get("notes", "")
    ts = booking.get("timestamp", "")
    event_id = (event or {}).get("eventId", "") if isinstance(event, dict) else ""
    html_link = (event or {}).get("htmlLink", "") if isinstance(event, dict) else ""

    # ISO дата и ключ месяца
    date_iso = ""
    month_key = ""
    try:
        dt_vis = datetime.strptime(date_visit, "%d.%m.%Y")
        date_iso = dt_vis.strftime("%Y-%m-%d")
        month_key = dt_vis.strftime("%Y-%m")
    except Exception:
        pass

    # №п/п — формула считает номер автоматически
    row = [
        "=ROW()-1",          # A — №п/п
        fio,                 # B
        username,            # C
        phone,               # D
        service,             # E
        price,               # F
        date_visit,          # G
        time_visit,          # H
        dur_min,             # I
        note,                # J
        ts,                  # K
        event_id,            # L
        html_link,           # M
        date_iso,            # N — дата визита ISO
        month_key,           # O — месяц (YYYY-MM)
    ]
    try:
        ws.append_row(row, value_input_option="USER_ENTERED")
    except Exception as e:
        logger.exception("Не удалось добавить строку в Google Sheets: %s", e)

# ---------- UI helpers ----------
def main_menu_keyboard() -> InlineKeyboardMarkup:
    tg_link = f"https://t.me/{BRAND['telegram_username']}" if BRAND.get("telegram_username") else "https://t.me"
    buttons = [
        [InlineKeyboardButton("💄 Услуги и цены", callback_data="menu_services")],
        [InlineKeyboardButton("📝 Записаться", callback_data="menu_book")],
        [InlineKeyboardButton("⭐ Отзывы", callback_data="menu_reviews")],
        [InlineKeyboardButton("📬 Контакты", callback_data="menu_contacts")],
        [InlineKeyboardButton("✍️ Написать мне", url=tg_link)],
    ]
    return InlineKeyboardMarkup(buttons)

def services_text() -> str:
    lines = ["<b>Услуги и цены</b>"]
    for s in SERVICES:
        price = f"{s['price']} {s['currency']}"
        lines.append(f"• {s['name']} — <b>{price}</b> ({s['duration']})")
    lines.append("")
    lines.append("Индивидуальный выезд, ранние выезды и срочные бронирования — по договоренности.")
    lines.append("Чтобы записаться, нажмите «📝 Записаться».")
    return "\n".join(lines)

def contacts_text() -> str:
    tg = f"@{BRAND['telegram_username']}" if BRAND.get("telegram_username") else ""
    addr = BRAND.get("address", "")
    lines = [
        "<b>Контакты</b>",
        f"📞 Телефон: {BRAND['phone']}",
        f"📸 Instagram: {BRAND['instagram']}",
        f"✈️ Telegram: {tg}",
        f"💬 WhatsApp: {BRAND['whatsapp']}",
    ]
    if addr:
        lines.append(f"🗺️ Адрес студии: {addr}")
    return "\n".join(lines)

def yandex_map_url() -> str:
    query = "Ярославль, проспект Октября, 42"
    q = urllib.parse.quote_plus(query)
    return f"https://yandex.ru/maps/?text={q}"

def contacts_keyboard() -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton("🗺️ Показать на карте", url=yandex_map_url())],
        [InlineKeyboardButton("◀️ В меню", callback_data="back_menu")],
    ]
    return InlineKeyboardMarkup(buttons)

# ---------- Inline Calendar ----------
MONTHS_RU = [
    "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
    "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"
]

def day_button(year: int, month: int, day: int, today: date_cls) -> InlineKeyboardButton:
    d = date_cls(year, month, day)
    if d < today:
        return InlineKeyboardButton(f"·{day}·", callback_data="cal:noop")
    return InlineKeyboardButton(str(day), callback_data=f"cal:day:{year}-{month:02d}-{day:02d}")

def build_month_keyboard(year: int, month: int, anchor: str | None = None) -> InlineKeyboardMarkup:
    """Инлайн-календарь: навигация по месяцам (без +/-7 дней)."""
    title = f"{MONTHS_RU[month - 1]} {year}"
    week_header = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]

    first_weekday, days_in_month = monthrange(year, month)  # Mon=0..Sun=6

    rows = []
    rows.append([InlineKeyboardButton(title, callback_data="cal:noop")])
    rows.append([InlineKeyboardButton(d, callback_data="cal:noop") for d in week_header])

    # пустые до первого дня
    day = 1
    row = []
    for _ in range(first_weekday):
        row.append(InlineKeyboardButton(" ", callback_data="cal:noop"))

    today = datetime.now(tz=_tz()).date()
    while day <= days_in_month:
        row.append(day_button(year, month, day, today))
        if len(row) == 7:
            rows.append(row)
            row = []
        day += 1
    if row:
        while len(row) < 7:
            row.append(InlineKeyboardButton(" ", callback_data="cal:noop"))
        rows.append(row)

    # якорь для корректной навигации по месяцам
    anchor_date = datetime(year, month, 15).date() if anchor is None else datetime.strptime(anchor, "%Y-%m-%d").date()
    prev_month = (anchor_date - timedelta(days=31)).replace(day=15)
    next_month = (anchor_date + timedelta(days=31)).replace(day=15)

    # только «Пред. месяц / Отмена / След. месяц»
    nav = [
        InlineKeyboardButton("« Пред. месяц", callback_data=f"cal:show:{prev_month.year}-{prev_month.month:02d}"),
        InlineKeyboardButton("Отмена", callback_data="book_cancel"),
        InlineKeyboardButton("След. месяц »", callback_data=f"cal:show:{next_month.year}-{next_month.month:02d}"),
    ]
    rows.append(nav)

    return InlineKeyboardMarkup(rows)

def times_keyboard(date_str: str, slots: list[datetime]) -> InlineKeyboardMarkup:
    """Клавиатура со слотами + 'Другое время' + 'Ближайшие свободные дни'."""
    rows = []
    if not slots:
        rows.append([InlineKeyboardButton("Свободных слотов нет", callback_data="cal:noop")])
    else:
        row = []
        for dt in slots:
            label = dt.strftime("%H:%M")
            row.append(InlineKeyboardButton(label, callback_data=f"time:{date_str}:{label}"))
            if len(row) == 3:
                rows.append(row); row = []
        if row:
            rows.append(row)

    rows.append([InlineKeyboardButton("🕒 Другое время", callback_data=f"time:other:{date_str}")])
    rows.append([InlineKeyboardButton("📅 Ближайшие свободные дни", callback_data=f"free:next:{date_str}")])
    rows.append([InlineKeyboardButton("← Изменить дату", callback_data=f"cal:change_date:{date_str}")])
    rows.append([InlineKeyboardButton("◀️ В меню", callback_data="back_menu")])
    return InlineKeyboardMarkup(rows)

# ---------- Storage helpers ----------
def append_booking_row_csv(row: dict):
    with open(BOOKINGS_CSV, "a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            row.get("timestamp", ""),
            row.get("username", ""),
            row.get("name", ""),
            row.get("phone", ""),
            row.get("service", ""),
            row.get("date", ""),
            row.get("time", ""),
            row.get("notes", ""),
            row.get("eventId", ""),
            row.get("htmlLink", ""),
        ])

def load_reviews() -> list:
    try:
        with open(REVIEWS_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def save_reviews(revs: list):
    with open(REVIEWS_JSON, "w", encoding="utf-8") as f:
        json.dump(revs, f, ensure_ascii=False, indent=2)

def reviews_text(limit: int = 10) -> str:
    revs = list(reversed(load_reviews()))
    if not revs:
        return "Пока отзывов нет. Будем рады, если вы поделитесь впечатлением! Нажмите «✍️ Оставить отзыв»."
    cut = revs[:limit]
    lines = ["<b>Отзывы клиентов</b>"]
    for r in cut:
        name = r.get("name") or "Гость"
        text = r.get("text") or ""
        date_s = r.get("date") or ""
        lines.append(f"— <b>{name}</b> ({date_s}): {text}")
    if len(revs) > limit:
        lines.append(f"\nПоказано {limit} из {len(revs)}. Напишите /reviews, чтобы увидеть больше.")
    return "\n".join(lines)

# ---------- Handlers: main menu ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "Я бот стилиста — <b>Кириллова Татьяна</b>.\n"
        "Давай знакомиться! Меня зовут Татьяна, я стилист и визажист.\n"
        "Делаю макияж и укладки для фотосессий, свадеб, выпускных и других мероприятий.\n"
        "А ещё я разработала уникальные курсы по самостоятельному макияжу «для себя».\n\n"
        "Выбирай раздел ниже:"
    )
    await update.effective_message.reply_text(
        text, reply_markup=main_menu_keyboard(), parse_mode="HTML"
    )

async def on_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await safe_answer_callback(query)
    data = query.data

    if data == "menu_services":
        await safe_edit_message_text(
            query,
            services_text(),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📝 Записаться", callback_data="menu_book")],
                [InlineKeyboardButton("◀️ В меню", callback_data="back_menu")]
            ]),
            parse_mode="HTML"
        )

    elif data == "menu_contacts":
        await safe_edit_message_text(
            query,
            contacts_text(),
            reply_markup=contacts_keyboard(),
            parse_mode="HTML"
        )

    elif data == "menu_reviews":
        await safe_edit_message_text(
            query,
            reviews_text(),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✍️ Оставить отзыв", callback_data="review_add")],
                [InlineKeyboardButton("◀️ В меню", callback_data="back_menu")]
            ]),
            parse_mode="HTML"
        )

    elif data == "back_menu":
        await safe_edit_message_text(query, "Главное меню:", reply_markup=main_menu_keyboard())

# ---------- Booking flow ----------
SEL_SERVICE, ASK_DATE, ASK_TIME, ASK_TIME_OTHER, ASK_NAME, ASK_PHONE, ASK_NOTES, BOOK_CONFIRM = range(8)

def services_keyboard() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(s["name"], callback_data=f"svc:{i}")] for i, s in enumerate(SERVICES)]
    rows.append([InlineKeyboardButton("◀️ Назад", callback_data="back_menu")])
    return InlineKeyboardMarkup(rows)

def _valid_date(s: str) -> bool:
    try:
        datetime.strptime(s.strip(), "%d.%m.%Y")
        return True
    except Exception:
        return False

def _valid_time(s: str) -> bool:
    try:
        datetime.strptime(s.strip(), "%H:%M")
        return True
    except Exception:
        return False

# entry point от КНОПКИ "📝 Записаться"
async def book_start_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback(query)
    await safe_edit_message_text(query, "Выберите услугу:", reply_markup=services_keyboard())
    return SEL_SERVICE

# entry point от КОМАНДЫ /book
async def book_start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text("Выберите услугу:", reply_markup=services_keyboard())
    return SEL_SERVICE

async def book_pick_service(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback(query)
    if not query.data.startswith("svc:"):
        return ConversationHandler.END
    idx = int(query.data.split(":")[1])
    context.user_data["booking"] = {"service": SERVICES[idx]["name"]}
    # Показ календаря
    today = datetime.now(tz=_tz()).date()
    await safe_edit_message_text(
        query,
        "Выберите дату:",
        reply_markup=build_month_keyboard(today.year, today.month, today.strftime("%Y-%m-%d"))
    )
    return ASK_DATE

# --- Календарь: навигация/выбор дня ---
async def calendar_handle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback(query)
    data = query.data

    # cal:show:YYYY-MM | cal:show:today | cal:day:YYYY-MM-DD | cal:change_date:YYYY-MM-DD | cal:noop
    if data == "cal:show:today":
        t = datetime.now(tz=_tz()).date()
        await safe_edit_message_text(
            query, "Выберите дату:",
            reply_markup=build_month_keyboard(t.year, t.month, t.strftime("%Y-%m-%d"))
        )
        return ASK_DATE
    if data.startswith("cal:show:"):
        y, m = map(int, data.split(":")[2].split("-"))
        anchor = f"{y}-{m:02d}-15"
        await safe_edit_message_text(
            query, "Выберите дату:",
            reply_markup=build_month_keyboard(y, m, anchor)
        )
        return ASK_DATE
    elif data.startswith("cal:change_date:"):
        d = datetime.strptime(data.split(":")[2], "%Y-%m-%d").date()
        await safe_edit_message_text(
            query, "Выберите дату:",
            reply_markup=build_month_keyboard(d.year, d.month, d.strftime("%Y-%m-%d"))
        )
        return ASK_DATE
    elif data.startswith("cal:day:"):
        d = datetime.strptime(data.split(":")[2], "%Y-%m-%d").date()
        context.user_data.setdefault("booking", {})
        context.user_data["booking"]["date"] = d.strftime("%d.%m.%Y")
        svc = context.user_data["booking"].get("service")
        minutes = _service_duration_minutes(svc) if svc else 60
        slots = compute_free_slots_for_date(d, minutes)
        if not slots:
            # Нет слотов — сразу предложим ближайшие свободные дни
            kb = nearest_free_days_keyboard(d, minutes)
            await safe_edit_message_text(
                query,
                f"На {d.strftime('%d.%m.%Y')} свободных слотов нет.\nВыберите ближайшую свободную дату:",
                reply_markup=kb
            )
            return ASK_DATE
        await safe_edit_message_text(
            query,
            f"Дата: {d.strftime('%d.%m.%Y')}\nВыберите время:",
            reply_markup=times_keyboard(d.strftime("%Y-%m-%d"), slots)
        )
        return ASK_TIME
    else:
        return ASK_DATE

# --- Выбор времени из предложенных слотов ---
async def time_pick_from_slots(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback(query)
    # time:YYYY-MM-DD:HH:MM
    _, d_str, t_str = query.data.split(":", 2)  # maxsplit=2, чтобы "HH:MM" не развалилось
    context.user_data.setdefault("booking", {})
    context.user_data["booking"]["date"] = datetime.strptime(d_str, "%Y-%m-%d").strftime("%d.%m.%Y")
    context.user_data["booking"]["time"] = t_str
    context.user_data["booking"]["allow_overlap"] = False
    await safe_edit_message_text(query, "Твоё имя и фамилия:")
    return ASK_NAME

# --- Ручной ввод времени ("Другое время") ---
async def time_other_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback(query)
    # time:other:YYYY-MM-DD
    _, _, d_str = query.data.split(":", 2)
    context.user_data.setdefault("booking", {})
    context.user_data["booking"]["date"] = datetime.strptime(d_str, "%Y-%m-%d").strftime("%d.%m.%Y")
    context.user_data["booking"]["allow_overlap"] = True  # допускаем пересечение
    await safe_edit_message_text(query, "Укажи удобное время в формате ЧЧ:ММ (например, 18:00):")
    return ASK_TIME_OTHER

async def book_time_other(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.effective_message.text.strip()
    if not _valid_time(text):
        await safe_reply_text(update.effective_message, "Пожалуйста, введи время в формате ЧЧ:ММ (например, 18:00):")
        return ASK_TIME_OTHER
    # запретим прошлое время
    try:
        dt = _tz().localize(datetime.strptime(f"{context.user_data['booking']['date']} {text}", "%d.%m.%Y %H:%M"))
        if dt <= datetime.now(tz=_tz()):
            await safe_reply_text(update.effective_message, "Это время уже прошло. Укажи, пожалуйста, будущее время.")
            return ASK_TIME_OTHER
    except Exception:
        pass
    context.user_data["booking"]["time"] = text
    await safe_reply_text(update.effective_message, "Твоё имя и фамилия:")
    return ASK_NAME

# --- Ближайшие свободные дни ---
def nearest_free_days_keyboard(anchor_date: date_cls, service_minutes: int) -> InlineKeyboardMarkup:
    """Клавиатура со списком ближайших свободных дней (до 7 шт)."""
    rows = []
    d = anchor_date
    found = 0
    while found < 7 and d <= anchor_date + timedelta(days=30):
        d += timedelta(days=1)
        slots = compute_free_slots_for_date(d, service_minutes)
        if slots:
            label = f"{d.strftime('%d.%m')} — {len(slots)} сл."
            rows.append([InlineKeyboardButton(label, callback_data=f"cal:day:{d.strftime('%Y-%m-%d')}")])
            found += 1
    if found == 0:
        rows.append([InlineKeyboardButton("Свободных дней в ближайший месяц нет", callback_data="cal:noop")])
    rows.append([InlineKeyboardButton("← Назад ко времени", callback_data=f"cal:day:{anchor_date.strftime('%Y-%m-%d')}")])
    return InlineKeyboardMarkup(rows)

async def nearest_free_days_handle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback(query)
    # free:next:YYYY-MM-DD
    _, _, d_str = query.data.split(":", 2)
    anchor = datetime.strptime(d_str, "%Y-%m-%d").date()
    svc = context.user_data.get("booking", {}).get("service")
    minutes = _service_duration_minutes(svc) if svc else 60
    kb = nearest_free_days_keyboard(anchor, minutes)
    await safe_edit_message_text(query, "Ближайшие свободные дни:", reply_markup=kb)
    return ASK_DATE

# --- Универсальные кнопки: Отмена и В меню ---
async def book_cancel_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback(query)
    context.user_data.pop("booking", None)
    await safe_edit_message_text(query, "Заявка отменена.", reply_markup=main_menu_keyboard())
    return ConversationHandler.END

async def back_to_menu_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback(query)
    context.user_data.pop("booking", None)
    await safe_edit_message_text(query, "Главное меню:", reply_markup=main_menu_keyboard())
    return ConversationHandler.END

# --- Fallback-ы (текстом) ---
async def book_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.effective_message.text.strip()
    if not _valid_date(text):
        await safe_reply_text(update.effective_message, "Пожалуйста, введи дату в формате ДД.ММ.ГГГГ (например, 05.10.2025):")
        return ASK_DATE
    context.user_data.setdefault("booking", {})
    context.user_data["booking"]["date"] = text
    # раз пользователь ввёл дату вручную — предложим вручную время (с допуском пересечений)
    context.user_data["booking"]["allow_overlap"] = True
    await safe_reply_text(update.effective_message, "Во сколько тебе удобно? (например, 10:30). Допускаются пересечения — я уточню лично.")
    return ASK_TIME_OTHER

async def book_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.effective_message.text.strip()
    if not _valid_time(text):
        await safe_reply_text(update.effective_message, "Пожалуйста, введи время в формате ЧЧ:ММ (например, 10:30):")
        return ASK_TIME
    context.user_data.setdefault("booking", {})
    context.user_data["booking"]["time"] = text
    context.user_data["booking"]["allow_overlap"] = False
    await safe_reply_text(update.effective_message, "Твоё имя и фамилия:")
    return ASK_NAME

async def book_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["booking"]["name"] = update.effective_message.text.strip()
    await safe_reply_text(update.effective_message, "Телефон (для подтверждения):")
    return ASK_PHONE

async def book_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["booking"]["phone"] = update.effective_message.text.strip()
    await safe_reply_text(update.effective_message, "Пожелания/комментарии (или «-», если нет):")
    return ASK_NOTES

async def book_notes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    notes = update.effective_message.text.strip()
    if notes == "-":
        notes = ""
    context.user_data["booking"]["notes"] = notes

    b = context.user_data["booking"]
    duration_min = _service_duration_minutes(b["service"])
    summary = (
        "<b>Проверь заявку:</b>\n"
        f"Услуга: {b['service']}\n"
        f"Дата/время: {b['date']} {b['time']} (≈ {duration_min} мин)\n"
        f"Имя: {b['name']}\n"
        f"Телефон: {b['phone']}\n"
        f"Комментарий: {b['notes'] or '—'}\n\n"
        "Отправить заявку?"
    )
    await update.effective_message.reply_text(
        summary,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Отправить", callback_data="book_send")],
            [InlineKeyboardButton("❌ Отменить", callback_data="book_cancel")]
        ])
    )
    return BOOK_CONFIRM

async def book_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback(query)
    if query.data == "book_cancel":
        await safe_edit_message_text(query, "Заявка отменена.", reply_markup=main_menu_keyboard())
        context.user_data.pop("booking", None)
        return ConversationHandler.END

    if query.data != "book_send":
        return BOOK_CONFIRM

    b = context.user_data.get("booking", {})
    b.update({
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "username": update.effective_user.username or str(update.effective_user.id),
    })

    created = await create_event_on_calendar(b)
    if created and created.get("conflict"):
        d = datetime.strptime(b["date"], "%d.%m.%Y").date()
        minutes = _service_duration_minutes(b["service"])
        slots = compute_free_slots_for_date(d, minutes)
        await safe_edit_message_text(
            query,
            "Упс, это время уже занято. Выберите другое:",
            reply_markup=times_keyboard(d.strftime("%Y-%m-%d"), slots)
        )
        return ASK_TIME

    overlapped = bool(created and created.get("overlapped"))

    # Проставим поля календаря для локальной и GSheets записи
    if isinstance(created, dict):
        b["eventId"] = created.get("eventId", "")
        b["htmlLink"] = created.get("htmlLink", "")
        calendar_note = ""
    else:
        b["eventId"] = ""
        b["htmlLink"] = ""
        calendar_note = "\nℹ️ Внутренний календарь временно недоступен, но мы получили вашу заявку."

    # Сохраняем локально (CSV) и в Google Sheets
    append_booking_row_csv(b)
    append_gsheet_row(b, created if isinstance(created, dict) else None)

    # Уведомление админам
    user_tag = f"@{update.effective_user.username}" if update.effective_user.username else f"id:{update.effective_user.id}"
    msg = (
        "🆕 Новая заявка:\n"
        f"Услуга: {b['service']}\n"
        f"Дата/время: {b['date']} {b['time']}\n"
        f"Имя: {b['name']}\n"
        f"Телефон: {b['phone']}\n"
        f"Комментарий: {b['notes'] or '—'}\n"
        f"От: {user_tag}"
    )
    if b.get("htmlLink"):
        msg += f"\n📅 В календаре: {b['htmlLink']}"
    if overlapped:
        msg += "\n⚠️ Внимание: ВЫБРАНО РУЧНОЕ ВРЕМЯ, ЕСТЬ ПЕРЕСЕЧЕНИЕ с другой записью на эту дату. Свяжусь с клиентом для уточнения."

    await send_admin(context, msg)

    thanks = "Спасибо! Заявка отправлена. Мы свяжемся с тобой для подтверждения."
    if overlapped:
        thanks += "\n⚠️ Обрати внимание: на эту дату уже есть запись другого клиента. Я свяжусь с тобой в личных сообщениях и предложу варианты."
    if calendar_note:
        thanks += calendar_note

    await safe_edit_message_text(query, thanks, reply_markup=main_menu_keyboard())

    context.user_data.pop("booking", None)
    return ConversationHandler.END

# ---------- Reviews flow ----------
ASK_REVIEW_NAME, ASK_REVIEW_TEXT = range(2)

async def review_start_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback(query)
    await safe_edit_message_text(query, "Как тебя зовут? (Имя, можно без фамилии)")
    return ASK_REVIEW_NAME

async def review_start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text("Как тебя зовут? (Имя, можно без фамилии)")
    return ASK_REVIEW_NAME

async def review_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["review"] = {"name": update.effective_message.text.strip()}
    await update.effective_message.reply_text("Оставь, пожалуйста, отзыв (пара предложений):")
    return ASK_REVIEW_TEXT

async def review_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = update.effective_message.text.strip()
    r = context.user_data.get("review", {})
    r.update({
        "text": txt,
        "date": datetime.now().strftime("%d.%m.%Y"),
        "username": update.effective_user.username or str(update.effective_user.id)
    })
    reviews = load_reviews()
    reviews.append(r)
    save_reviews(reviews)

    msg = (
        "⭐ Новый отзыв:\n"
        f"Имя: {r.get('name','')}\n"
        f"Текст: {r['text']}\n"
        f"От: @{r['username']}\n"
        f"Дата: {r['date']}"
    )
    await send_admin(context, msg)

    await update.effective_message.reply_text(
        "Спасибо за отзыв! Он очень важен для нас ❤️",
        reply_markup=main_menu_keyboard()
    )
    context.user_data.pop("review", None)
    return ConversationHandler.END

# ---- Команда /reviews ----
async def cmd_reviews(update: Update, context: ContextTypes.DEFAULT_TYPE):
    revs = load_reviews()
    if not revs:
        await update.effective_message.reply_text("Отзывов пока нет.")
        return
    lines = ["<b>Отзывы клиентов</b>"]
    for r in reversed(revs[-10:]):
        name = r.get("name") or "Гость"
        text = r.get("text") or ""
        date_s = r.get("date") or ""
        lines.append(f"— <b>{name}</b> ({date_s}): {text}")
    await update.effective_message.reply_text("\n".join(lines), parse_mode="HTML")

# ---------- Diagnostic & error ----------
async def myid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(f"Ваш chat_id: {update.effective_user.id}")

async def hereid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(f"ID этого чата: {update.effective_chat.id}")

async def admin_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_admin(context, "🔔 Тестовое уведомление администратору.")
    await update.effective_message.reply_text("Тестовое уведомление отправлено (смотрите логи, если не пришло).")

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Ожидаемые «сетевые» ошибки, которые PTB сам переразруливает — логируем пониженными уровнями и выходим
    transient = (
        TimedOut,
        NetworkError,
    )
    # httpx.* не импортируется напрямую из PTB; импортируем локально, чтобы не делать верхние импорты обязательными
    try:
        import httpx
        httpx_errors = (httpx.RemoteProtocolError, httpx.ReadError, httpx.ConnectError)
    except Exception:
        httpx_errors = tuple()

    if isinstance(context.error, transient) or isinstance(context.error, httpx_errors):
        logger.warning("Transient network issue during polling: %r", context.error)
        return

    # Всё остальное — реально логируем как ошибку
    logger.exception("Unhandled error: %s", context.error)
    try:
        if update and hasattr(update, "effective_message") and update.effective_message:
            await update.effective_message.reply_text("Похоже, временные проблемы со связью. Попробуй ещё раз, пожалуйста.")
    except Exception:
        pass


# ---------- Entrypoint ----------
def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не найден. Создайте .env и добавьте BOT_TOKEN=<токен>")

    proxy_url = os.getenv("TG_PROXY", "").strip() or None
    request = HTTPXRequest(
        connect_timeout=15.0,
        read_timeout=70.0,  # было 30 — дайте запас для long polling
        write_timeout=15.0,
        pool_timeout=20.0,
        proxy=proxy_url,
        http_version="1.1",  # ключевая настройка: форсируем HTTP/1.1
    )

    app = Application.builder().token(BOT_TOKEN).request(request).build()

    # Conversation: booking
    booking_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(book_start_cb, pattern=r"^menu_book$"),
            CommandHandler("book", book_start_cmd),
        ],
        states={
            SEL_SERVICE: [
                CallbackQueryHandler(book_pick_service, pattern=r"^svc:\d+$"),
                CallbackQueryHandler(book_cancel_cb, pattern=r"^book_cancel$"),
                CallbackQueryHandler(back_to_menu_cb, pattern=r"^back_menu$"),
            ],
            ASK_DATE: [
                CallbackQueryHandler(calendar_handle, pattern=r"^cal:(show|day|change_date|noop)(:.*)?$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, book_date),  # fallback текстом
                CallbackQueryHandler(book_cancel_cb, pattern=r"^book_cancel$"),
                CallbackQueryHandler(back_to_menu_cb, pattern=r"^back_menu$"),
            ],
            ASK_TIME: [
                CallbackQueryHandler(calendar_handle, pattern=r"^cal:(show|day|change_date|noop)(:.*)?$"),
                CallbackQueryHandler(time_pick_from_slots, pattern=r"^time:\d{4}-\d{2}-\d{2}:\d{2}:\d{2}$"),
                CallbackQueryHandler(time_other_click, pattern=r"^time:other:\d{4}-\d{2}-\d{2}$"),
                CallbackQueryHandler(nearest_free_days_handle, pattern=r"^free:next:\d{4}-\d{2}-\d{2}$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, book_time),  # fallback текстом
                CallbackQueryHandler(book_cancel_cb, pattern=r"^book_cancel$"),
                CallbackQueryHandler(back_to_menu_cb, pattern=r"^back_menu$"),
            ],
            ASK_TIME_OTHER: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, book_time_other),
                CallbackQueryHandler(book_cancel_cb, pattern=r"^book_cancel$"),
                CallbackQueryHandler(back_to_menu_cb, pattern=r"^back_menu$"),
            ],
            ASK_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, book_name),
                CallbackQueryHandler(book_cancel_cb, pattern=r"^book_cancel$"),
                CallbackQueryHandler(back_to_menu_cb, pattern=r"^back_menu$"),
            ],
            ASK_PHONE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, book_phone),
                CallbackQueryHandler(book_cancel_cb, pattern=r"^book_cancel$"),
                CallbackQueryHandler(back_to_menu_cb, pattern=r"^back_menu$"),
            ],
            ASK_NOTES: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, book_notes),
                CallbackQueryHandler(book_cancel_cb, pattern=r"^book_cancel$"),
                CallbackQueryHandler(back_to_menu_cb, pattern=r"^back_menu$"),
            ],
            BOOK_CONFIRM: [
                CallbackQueryHandler(book_confirm, pattern=r"^book_(send|cancel)$"),
                CallbackQueryHandler(back_to_menu_cb, pattern=r"^back_menu$"),
            ],
        },
        fallbacks=[
            CommandHandler("start", start),
            CallbackQueryHandler(book_cancel_cb, pattern=r"^book_cancel$"),
            CallbackQueryHandler(back_to_menu_cb, pattern=r"^back_menu$"),
        ],
        name="booking_conv",
        persistent=False,
    )
    app.add_handler(booking_conv)

    # Conversation: reviews
    reviews_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(review_start_cb, pattern=r"^review_add$"),
            CommandHandler("review", review_start_cmd),
        ],
        states={
            ASK_REVIEW_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, review_name)],
            ASK_REVIEW_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, review_text)],
        },
        fallbacks=[CommandHandler("start", start)],
        name="reviews_conv",
        persistent=False,
    )
    app.add_handler(reviews_conv)

    # Commands & callbacks
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("reviews", cmd_reviews))
    app.add_handler(CommandHandler("myid", myid))
    app.add_handler(CommandHandler("hereid", hereid))
    app.add_handler(CommandHandler("admin_test", admin_test))
    app.add_handler(CallbackQueryHandler(on_menu))

    app.add_error_handler(error_handler)

    print("Bot is running. Press Ctrl+C to stop.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
    print("Bot is running. Press Ctrl+C to stop.")
    # drop_pending_updates — чтобы при рестарте не обрабатывать старые «залежавшиеся» апдейты
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)
