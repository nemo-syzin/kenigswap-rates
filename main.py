
import asyncio
import html
import logging
import os
import random
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, Dict, Any, List

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from bs4 import BeautifulSoup
from telegram.ext import ApplicationBuilder, CommandHandler
from dotenv import load_dotenv

load_dotenv()

# ───────────────────── CONFIG ───────────────────────
TOKEN            = os.getenv("TG_BOT_TOKEN")
PASSWORD         = os.getenv("TG_BOT_PASS")
CHAT_ID          = os.getenv("TG_CHAT_ID", "@KaliningradCryptoRatesKenigSwap")
ALERT_CHAT_ID    = os.getenv("ALERT_CHAT_ID", "").strip()
KAL_TZ           = timezone(timedelta(hours=2))

# Оффсеты — только для Rapira-fallback. Основные оффсеты — в supabase-worker.
KENIG_ASK_OFFSET = float(os.getenv("KENIG_ASK_OFFSET", "0.8"))
KENIG_BID_OFFSET = float(os.getenv("KENIG_BID_OFFSET", "-0.9"))

MAX_RETRIES      = 3
RETRY_DELAY      = 5
AUTHORIZED_USERS: set[int] = set()
RAPIRA_PROXY     = os.getenv("RAPIRA_PROXY")
SHOW_SOURCES_META = os.getenv("SHOW_SOURCES_META", "0") == "1"
APP = None

# Supabase
SUPABASE_URL       = (os.getenv("SUPABASE_URL") or "").rstrip("/")
SUPABASE_KEY       = os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY") or ""
USE_DB_AS_PRIMARY  = (os.getenv("USE_DB_AS_PRIMARY", "1") or "").lower() in ("1", "true", "yes")

# Fallback переключатели — все ВЫКЛЮЧЕНЫ по умолчанию
ENABLE_RAPIRA_FALLBACK         = os.getenv("ENABLE_RAPIRA_FALLBACK", "0") == "1"
ENABLE_ENERGO_DIRECT_FALLBACK  = os.getenv("ENABLE_ENERGO_DIRECT_FALLBACK", "0") == "1"

# Алерты
ALERT_COOLDOWN_SEC = int(os.getenv("ALERT_COOLDOWN_SEC", "900"))

# ───────────────────── LOGGER ───────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ───────────────── ALERT MANAGER ────────────────────
class AlertManager:
    """
    Антиспам-алерты в Telegram. Каждый источник имеет cooldown:
    второй алерт по тому же ключу не уйдёт раньше ALERT_COOLDOWN_SEC.
    Recovery-алерт отправляется когда источник возвращается после падения.
    """
    def __init__(self):
        self.last_alert_at: Dict[str, datetime] = {}
        self.is_failing: Dict[str, bool] = {}

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    async def fail(self, app, key: str, reason: str) -> None:
        """Источник упал. Логируем всегда, в Telegram — с cooldown."""
        log.error("ALERT[%s]: %s", key, reason)
        self.is_failing[key] = True

        if not ALERT_CHAT_ID or app is None:
            return

        last = self.last_alert_at.get(key)
        if last and (self._now() - last).total_seconds() < ALERT_COOLDOWN_SEC:
            return  # ещё рано

        self.last_alert_at[key] = self._now()
        ts = datetime.now(KAL_TZ).strftime("%d.%m.%Y %H:%M:%S")
        text = (
            f"🚨 <b>Курсы недоступны</b>\n"
            f"<b>Источник:</b> {html.escape(key)}\n"
            f"<b>Причина:</b> {html.escape(reason)}\n"
            f"<b>Время:</b> {ts}\n\n"
            f"В канал будет отправлено «— нет данных —»."
        )
        try:
            await app.bot.send_message(
                chat_id=ALERT_CHAT_ID, text=text, parse_mode="HTML",
                disable_web_page_preview=True,
            )
        except Exception as e:
            log.error("Failed to send alert to %s: %s", ALERT_CHAT_ID, e)

    async def recover(self, app, key: str, src: str) -> None:
        """Источник восстановился. Шлём recovery-алерт только если до этого падал."""
        if not self.is_failing.get(key):
            return
        self.is_failing[key] = False
        log.info("RECOVERY[%s]: source=%s", key, src)

        if not ALERT_CHAT_ID or app is None:
            return

        ts = datetime.now(KAL_TZ).strftime("%d.%m.%Y %H:%M:%S")
        text = (
            f"✅ <b>Курсы восстановлены</b>\n"
            f"<b>Источник:</b> {html.escape(key)} (via {html.escape(src)})\n"
            f"<b>Время:</b> {ts}"
        )
        try:
            await app.bot.send_message(
                chat_id=ALERT_CHAT_ID, text=text, parse_mode="HTML",
                disable_web_page_preview=True,
            )
        except Exception as e:
            log.error("Failed to send recovery to %s: %s", ALERT_CHAT_ID, e)

ALERTS = AlertManager()

# ───────────────── SUPABASE READER ─────────────────
def _sb_headers() -> Dict[str, str]:
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Accept": "application/json",
    }

def _classify_http_error(status: int) -> str:
    """Человекочитаемое описание HTTP-ошибки от Supabase."""
    if status == 401:
        return "401 Unauthorized: неверный SUPABASE_KEY (проверьте значение в Render env)"
    if status == 403:
        return "403 Forbidden: RLS блокирует чтение (нужна политика SELECT для anon/authenticated роли)"
    if status == 404:
        return "404 Not Found: таблица kenig_rates не существует или URL неверный"
    if status == 500:
        return "500 Internal Server Error: ошибка на стороне Supabase"
    if status == 503:
        return "503 Service Unavailable: проект Supabase приостановлен или перегружен"
    return f"HTTP {status}"

async def fetch_kenig_from_db() -> Tuple[Optional[float], Optional[float], str, Optional[str]]:
    """
    Читаем готовые kenig_sell/kenig_buy USDT/RUB из Supabase.
    Возвращает (sell, buy, updated_at, error_reason).
    error_reason заполняется только при ошибке. None = всё ок (включая «нет данных»).
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None, None, "", "SUPABASE_URL / SUPABASE_KEY не заданы в env"

    url = f"{SUPABASE_URL}/rest/v1/kenig_rates"
    params = {
        "select": "sell,buy,updated_at,is_active",
        "source": "eq.kenig",
        "base":   "eq.USDT",
        "quote":  "eq.RUB",
        "limit":  "1",
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as cli:
            r = await cli.get(url, params=params, headers=_sb_headers())
            if r.status_code >= 400:
                return None, None, "", _classify_http_error(r.status_code)
            rows = r.json() or []
            if not rows:
                return None, None, "", "в БД нет строки для source=kenig base=USDT quote=RUB"
            row = rows[0]
            if not row.get("is_active", True):
                return None, None, "", "строка в БД помечена is_active=false"
            sell = float(row["sell"]) if row.get("sell") is not None else None
            buy  = float(row["buy"])  if row.get("buy")  is not None else None
            ts   = str(row.get("updated_at") or "")
            if sell is None or buy is None or sell <= 0 or buy <= 0:
                return None, None, ts, "sell/buy в БД пустые или нулевые"
            return sell, buy, ts, None
    except httpx.TimeoutException:
        return None, None, "", "timeout при запросе к Supabase"
    except Exception as e:
        return None, None, "", f"{type(e).__name__}: {e}"

async def fetch_energo_from_db() -> Tuple[Optional[float], Optional[float], Optional[float], str, Optional[str]]:
    """
    Читаем USD/RUB из Supabase (source=energo).
    Возвращает (sell, buy, cbr, updated_at, error_reason).
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None, None, None, "", "SUPABASE_URL / SUPABASE_KEY не заданы в env"

    url = f"{SUPABASE_URL}/rest/v1/kenig_rates"
    params = {
        "select": "sell,buy,last_price,updated_at,is_active",
        "source": "eq.energo",
        "base":   "eq.USD",
        "quote":  "eq.RUB",
        "limit":  "1",
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as cli:
            r = await cli.get(url, params=params, headers=_sb_headers())
            if r.status_code >= 400:
                return None, None, None, "", _classify_http_error(r.status_code)
            rows = r.json() or []
            if not rows:
                return None, None, None, "", "в БД нет строки для source=energo base=USD quote=RUB"
            row = rows[0]
            if not row.get("is_active", True):
                return None, None, None, "", "строка в БД помечена is_active=false"
            sell  = float(row["sell"])       if row.get("sell")       is not None else None
            buy   = float(row["buy"])        if row.get("buy")        is not None else None
            cbr   = float(row["last_price"]) if row.get("last_price") is not None else None
            ts    = str(row.get("updated_at") or "")
            if sell is None or buy is None or sell <= 0 or buy <= 0:
                return None, None, None, ts, "sell/buy в БД пустые или нулевые"
            return sell, buy, (cbr or (sell + buy) / 2), ts, None
    except httpx.TimeoutException:
        return None, None, None, "", "timeout при запросе к Supabase"
    except Exception as e:
        return None, None, None, "", f"{type(e).__name__}: {e}"

# ──────────────── RAPIRA FALLBACK (опциональный) ────
RAPIRA_RATES_URL = "https://api.rapira.net/open/market/rates"
RAPIRA_OB_URL    = "https://api.rapira.net/market/exchange-plate-mini?symbol=USDT/RUB"

class RapiraBlockedError(Exception):
    pass

def _backoff(attempt: int, base: float = 1.6, jitter: float = 0.6) -> float:
    return (base ** (attempt - 1)) + random.uniform(0, jitter)

async def _rapira_get_json(url: str, timeout: float = 15.0) -> Optional[Dict[str, Any]]:
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    transport = httpx.AsyncHTTPTransport(proxy=RAPIRA_PROXY) if RAPIRA_PROXY else None
    for att in range(1, MAX_RETRIES + 1):
        try:
            async with httpx.AsyncClient(
                headers=headers, timeout=timeout,
                follow_redirects=True, transport=transport,
            ) as cli:
                r = await cli.get(url)
                if r.status_code == 451:
                    raise RapiraBlockedError("HTTP 451 (blocked)")
                r.raise_for_status()
                return r.json()
        except RapiraBlockedError:
            raise
        except Exception as e:
            log.warning("Rapira GET %s attempt %s failed: %s", url, att, e)
            if att < MAX_RETRIES:
                await asyncio.sleep(_backoff(att))
    return None

def _first_number_from_level(level) -> Optional[float]:
    try:
        if isinstance(level, (list, tuple)) and level:
            return float(level[0])
        if isinstance(level, dict) and "price" in level:
            return float(level["price"])
        return float(level)
    except Exception:
        return None

def _safe_float(v) -> Optional[float]:
    try:
        return float(v)
    except Exception:
        return None

async def fetch_rapira_usdtrub_best() -> Tuple[Optional[float], Optional[float]]:
    try:
        data = await _rapira_get_json(RAPIRA_OB_URL)
        if data:
            container = data.get("data", data)
            asks = container.get("asks")
            bids = container.get("bids")
            best_ask = _first_number_from_level(asks[0]) if asks else None
            best_bid = _first_number_from_level(bids[0]) if bids else None
            if best_ask is not None or best_bid is not None:
                return best_ask, best_bid
        rates = await _rapira_get_json(RAPIRA_RATES_URL)
        items = (
            (rates.get("data") or rates.get("rates") or rates.get("result") or rates.get("items"))
            if isinstance(rates, dict) else rates
        )
        if isinstance(items, list):
            target = next(
                (x for x in items
                 if (x.get("symbol") or x.get("pair") or x.get("name") or "")
                    .upper().replace("_", "/") == "USDT/RUB"),
                None
            )
            if target:
                ask = _safe_float(target.get("ask") or target.get("offer") or target.get("askPrice") or target.get("sell"))
                bid = _safe_float(target.get("bid") or target.get("bidPrice") or target.get("buy"))
                return ask, bid
        return (None, None)
    except RapiraBlockedError:
        return (None, None)

# ────────── ENERGO direct fallback (опциональный) ──────────
async def fetch_energo() -> Tuple[Optional[float], Optional[float], Optional[float]]:
    url = "https://ru.myfin.by/bank/energotransbank/currency/kaliningrad"
    for att in range(1, MAX_RETRIES + 1):
        try:
            async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}, timeout=15) as cli:
                r = await cli.get(url)
                r.raise_for_status()
                soup = BeautifulSoup(r.text, "html.parser")
                row = soup.select_one("table.table-best.white_bg tr:has(td.title)")
                if not row:
                    raise RuntimeError("row not found")
                buy, sell, cbr = [
                    float(td.text.replace(",", "."))
                    for td in row.find_all("td")[1:4]
                ]
                return sell, buy, cbr
        except Exception as e:
            log.warning("Energo direct attempt %s/%s: %s", att, MAX_RETRIES, e)
            if att < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)
    return None, None, None

# ──────────────── UNIFIED FETCHERS ─────────────────
async def get_kenig_usdt_rub(app=None) -> Tuple[Optional[float], Optional[float], str]:
    """
    Возвращает (kenig_sell, kenig_buy, источник).
    Источник: 'db' / 'rapira' / 'none'.
    При неудаче — шлёт алерт и возвращает (None, None, 'none').
    """
    # 1. Из БД (приоритет)
    if USE_DB_AS_PRIMARY:
        sell, buy, _ts, err = await fetch_kenig_from_db()
        if err is None and sell is not None and buy is not None:
            log.info("USDT/RUB from DB: sell=%.4f buy=%.4f", sell, buy)
            await ALERTS.recover(app, "USDT/RUB", "db")
            return sell, buy, "db"
        if err:
            log.warning("DB read failed for USDT/RUB: %s", err)
            db_err = err
        else:
            db_err = "DB вернул пустоту"
    else:
        db_err = "USE_DB_AS_PRIMARY=0 в env"

    # 2. Rapira напрямую (опционально)
    if ENABLE_RAPIRA_FALLBACK:
        ask, bid = await fetch_rapira_usdtrub_best()
        if ask is not None and bid is not None:
            sell = ask + KENIG_ASK_OFFSET
            buy  = bid + KENIG_BID_OFFSET
            log.info("USDT/RUB from Rapira direct: sell=%.4f buy=%.4f", sell, buy)
            await ALERTS.recover(app, "USDT/RUB", "rapira")
            return sell, buy, "rapira"
        rapira_err = "Rapira API недоступен (HTTP error или прокси)"
    else:
        rapira_err = "ENABLE_RAPIRA_FALLBACK=0"

    # 3. Сдаёмся → алерт
    reason = f"БД: {db_err}; Rapira: {rapira_err}"
    await ALERTS.fail(app, "USDT/RUB", reason)
    return None, None, "none"

async def get_energo_usd_rub(app=None) -> Tuple[Optional[float], Optional[float], Optional[float], str]:
    """
    Возвращает (sell, buy, cbr, источник).
    Источник: 'db' / 'direct' / 'none'.
    """
    # 1. Из БД
    if USE_DB_AS_PRIMARY:
        sell, buy, cbr, _ts, err = await fetch_energo_from_db()
        if err is None and sell is not None and buy is not None:
            log.info("USD/RUB from DB: sell=%.2f buy=%.2f cbr=%.2f", sell, buy, cbr or 0)
            await ALERTS.recover(app, "USD/RUB", "db")
            return sell, buy, cbr, "db"
        if err:
            log.warning("DB read failed for USD/RUB: %s", err)
            db_err = err
        else:
            db_err = "DB вернул пустоту"
    else:
        db_err = "USE_DB_AS_PRIMARY=0 в env"

    # 2. Прямой парсер myfin.by (опционально)
    if ENABLE_ENERGO_DIRECT_FALLBACK:
        sell, buy, cbr = await fetch_energo()
        if sell is not None and buy is not None:
            log.info("USD/RUB from myfin.by direct: sell=%.2f buy=%.2f", sell, buy)
            await ALERTS.recover(app, "USD/RUB", "myfin")
            return sell, buy, cbr, "direct"
        direct_err = "myfin.by парсер не вернул данные"
    else:
        direct_err = "ENABLE_ENERGO_DIRECT_FALLBACK=0"

    reason = f"БД: {db_err}; myfin: {direct_err}"
    await ALERTS.fail(app, "USD/RUB", reason)
    return None, None, None, "none"

# ────────────── TELEGRAM COMMANDS ───────────────────
def auth_ok(uid: int) -> bool:
    return uid in AUTHORIZED_USERS

async def cmd_auth(u, ctx):
    if len(ctx.args) != 1:
        return await u.message.reply_text("Введите пароль: /auth <пароль>")
    if ctx.args[0] == PASSWORD:
        AUTHORIZED_USERS.add(u.effective_user.id)
        await u.message.reply_text("✅ Доступ разрешён.")
    else:
        await u.message.reply_text("❌ Неверный пароль.")

async def cmd_start(u, _):
    await u.message.reply_text("Бот активен. /help")

async def cmd_help(u, _):
    await u.message.reply_text(
        "/start /auth /check /change /show_offsets /diagnose /help\n\n"
        "Примечание: /change в этом боте меняет только локальные "
        "Rapira-fallback оффсеты (по умолчанию fallback выключен).\n"
        "Реальные оффсеты для сайта и Exnode живут в supabase-worker."
    )

async def cmd_check(u, _):
    if not auth_ok(u.effective_user.id):
        return await u.message.reply_text("Нет доступа. /auth <пароль>")
    await send_rates_message(APP)
    await u.message.reply_text("Отчёт выслан.")

async def cmd_change(u, ctx):
    if not auth_ok(u.effective_user.id):
        return await u.message.reply_text("Нет доступа.")
    try:
        global KENIG_ASK_OFFSET, KENIG_BID_OFFSET
        KENIG_ASK_OFFSET, KENIG_BID_OFFSET = map(float, ctx.args[:2])
        await u.message.reply_text(
            f"Локальные Rapira-fallback оффсеты: ask +{KENIG_ASK_OFFSET}  bid {KENIG_BID_OFFSET}\n"
            f"⚠️ Применяются ТОЛЬКО если ENABLE_RAPIRA_FALLBACK=1 и БД недоступна.\n"
            f"Для production-курсов меняйте в supabase-worker."
        )
    except Exception:
        await u.message.reply_text("Пример: /change 1.0 -0.5")

async def cmd_show(u, _):
    if not auth_ok(u.effective_user.id):
        return await u.message.reply_text("Нет доступа.")
    await u.message.reply_text(
        f"Локальные Rapira-fallback: Ask +{KENIG_ASK_OFFSET}  Bid {KENIG_BID_OFFSET}\n"
        f"USE_DB_AS_PRIMARY: {USE_DB_AS_PRIMARY}\n"
        f"ENABLE_RAPIRA_FALLBACK: {ENABLE_RAPIRA_FALLBACK}\n"
        f"ENABLE_ENERGO_DIRECT_FALLBACK: {ENABLE_ENERGO_DIRECT_FALLBACK}\n"
        f"ALERT_CHAT_ID: {ALERT_CHAT_ID or '(не задан)'}"
    )

async def cmd_diagnose(u, _):
    """Диагностика всех источников. Не публикует ничего в канал."""
    if not auth_ok(u.effective_user.id):
        return await u.message.reply_text("Нет доступа.")

    lines = ["🔍 <b>Диагностика источников</b>\n"]

    # USDT/RUB DB
    sell, buy, ts, err = await fetch_kenig_from_db()
    if err:
        lines.append(f"❌ <b>USDT/RUB БД:</b> {html.escape(err)}")
    elif sell and buy:
        lines.append(f"✅ <b>USDT/RUB БД:</b> sell={sell:.4f} buy={buy:.4f}")
        lines.append(f"   <i>updated:</i> {html.escape(ts)}")
    else:
        lines.append("⚠️ <b>USDT/RUB БД:</b> пустой ответ")

    # USD/RUB DB
    e_sell, e_buy, e_cbr, e_ts, e_err = await fetch_energo_from_db()
    if e_err:
        lines.append(f"❌ <b>USD/RUB БД:</b> {html.escape(e_err)}")
    elif e_sell and e_buy:
        lines.append(f"✅ <b>USD/RUB БД:</b> sell={e_sell:.2f} buy={e_buy:.2f} cbr={e_cbr or 0:.2f}")
        lines.append(f"   <i>updated:</i> {html.escape(e_ts)}")
    else:
        lines.append("⚠️ <b>USD/RUB БД:</b> пустой ответ")

    lines.append("")
    lines.append(f"<b>SUPABASE_URL:</b> {'✅ задан' if SUPABASE_URL else '❌ пусто'}")
    lines.append(f"<b>SUPABASE_KEY:</b> {'✅ задан (' + str(len(SUPABASE_KEY)) + ' симв.)' if SUPABASE_KEY else '❌ пусто'}")
    lines.append(f"<b>USE_DB_AS_PRIMARY:</b> {USE_DB_AS_PRIMARY}")
    lines.append(f"<b>ENABLE_RAPIRA_FALLBACK:</b> {ENABLE_RAPIRA_FALLBACK}")
    lines.append(f"<b>ENABLE_ENERGO_DIRECT_FALLBACK:</b> {ENABLE_ENERGO_DIRECT_FALLBACK}")
    lines.append(f"<b>ALERT_CHAT_ID:</b> {ALERT_CHAT_ID or '(не задан)'}")

    await u.message.reply_text("\n".join(lines), parse_mode="HTML",
                               disable_web_page_preview=True)

# ────────────── FORMATTING ──────────────
NUM_W = 6
SEG_W = 20

def _seg(lbl: str, val: Optional[float], unit: str = "₽", comma: bool = True) -> str:
    n = "—" if val is None else f"{val:.2f}"
    s = f"{lbl}: {n.rjust(NUM_W)} {unit}"
    if comma:
        s += ","
    return s.ljust(SEG_W)

def _inline_line(segments: List[str]) -> str:
    return "".join(segments).rstrip()

# ────────────── MESSAGE SENDER ──────────────────────
async def send_rates_message(app):
    kenig_sell, kenig_buy, kenig_src = await get_kenig_usdt_rub(app)
    en_sell, en_buy, en_cbr, en_src  = await get_energo_usd_rub(app)

    ts = datetime.now(KAL_TZ).strftime("%d.%m.%Y %H:%M:%S")
    lines: List[str] = [ts, ""]

    # KenigSwap
    lines.append("KenigSwap rate USDT/RUB")
    if kenig_sell is not None and kenig_buy is not None:
        lines.append(_inline_line([
            _seg("Покупка", kenig_buy,  "₽", comma=True),
            _seg("Продажа", kenig_sell, "₽", comma=False),
        ]))
    else:
        lines.append("— нет данных —")
    lines.append("")

    # EnergoTransBank
    lines.append("EnergoTransBank rate USD/RUB")
    if en_sell is not None and en_buy is not None:
        lines.append(_inline_line([
            _seg("Покупка", en_buy,  "₽", comma=True),
            _seg("Продажа", en_sell, "₽", comma=True),
            _seg("ЦБ",      en_cbr,  "₽", comma=False),
        ]))
    else:
        lines.append("— нет данных —")

    if SHOW_SOURCES_META:
        lines += ["", f"Источники: kenig={kenig_src}, energo={en_src}"]

    msg = "<pre>" + html.escape("\n".join(lines)) + "</pre>"
    try:
        await app.bot.send_message(
            chat_id=CHAT_ID,
            text=msg,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception as e:
        log.error("Send error: %s", e)

# ───────────────────── MAIN ─────────────────────────
def main() -> None:
    global APP
    if not TOKEN:
        raise RuntimeError("TG_BOT_TOKEN is not set")
    APP = ApplicationBuilder().token(TOKEN).build()
    APP.add_handler(CommandHandler("start", cmd_start))
    APP.add_handler(CommandHandler("help", cmd_help))
    APP.add_handler(CommandHandler("auth", cmd_auth))
    APP.add_handler(CommandHandler("check", cmd_check))
    APP.add_handler(CommandHandler("change", cmd_change))
    APP.add_handler(CommandHandler("show_offsets", cmd_show))
    APP.add_handler(CommandHandler("diagnose", cmd_diagnose))
    sched = AsyncIOScheduler(timezone=KAL_TZ)
    sched.add_job(send_rates_message, "interval", minutes=2, seconds=30, args=[APP])
    sched.start()
    log.info(
        "Bot started (V3 strict). "
        "USE_DB_AS_PRIMARY=%s, Supabase=%s, "
        "ENABLE_RAPIRA_FALLBACK=%s, ENABLE_ENERGO_DIRECT_FALLBACK=%s, "
        "ALERT_CHAT_ID=%s",
        USE_DB_AS_PRIMARY,
        bool(SUPABASE_URL and SUPABASE_KEY),
        ENABLE_RAPIRA_FALLBACK,
        ENABLE_ENERGO_DIRECT_FALLBACK,
        ALERT_CHAT_ID or "(off)",
    )
    APP.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
SUPABASE_URL       = (os.getenv("SUPABASE_URL") or "").rstrip("/")
SUPABASE_KEY       = os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY") or ""
USE_DB_AS_PRIMARY  = (os.getenv("USE_DB_AS_PRIMARY", "1") or "").lower() in ("1", "true", "yes")

# ───────────────────── LOGGER ───────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ───────────────── SUPABASE READER ─────────────────
def _sb_headers() -> Dict[str, str]:
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Accept": "application/json",
    }

async def fetch_kenig_from_db() -> Optional[Tuple[float, float, str]]:
    """
    Читаем готовые kenig_sell/kenig_buy USDT/RUB из Supabase.
    Возвращает (kenig_sell, kenig_buy, updated_at) или None.
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None
    url = f"{SUPABASE_URL}/rest/v1/kenig_rates"
    params = {
        "select": "sell,buy,updated_at,is_active",
        "source": "eq.kenig",
        "base":   "eq.USDT",
        "quote":  "eq.RUB",
        "limit":  "1",
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as cli:
            r = await cli.get(url, params=params, headers=_sb_headers())
            r.raise_for_status()
            rows = r.json() or []
            if not rows:
                return None
            row = rows[0]
            if not row.get("is_active", True):
                return None
            sell = float(row["sell"]) if row.get("sell") is not None else None
            buy  = float(row["buy"])  if row.get("buy")  is not None else None
            ts   = str(row.get("updated_at") or "")
            if sell is None or buy is None or sell <= 0 or buy <= 0:
                return None
            return sell, buy, ts
    except Exception as e:
        log.warning("Supabase fetch_kenig_from_db failed: %s", e)
        return None

async def fetch_energo_from_db() -> Optional[Tuple[float, float, float, str]]:
    """
    Читаем USD/RUB из Supabase (source=energo).
    Возвращает (sell, buy, last_price_as_cbr, updated_at) или None.
    last_price используется как заменитель ЦБ.
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None
    url = f"{SUPABASE_URL}/rest/v1/kenig_rates"
    params = {
        "select": "sell,buy,last_price,updated_at,is_active",
        "source": "eq.energo",
        "base":   "eq.USD",
        "quote":  "eq.RUB",
        "limit":  "1",
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as cli:
            r = await cli.get(url, params=params, headers=_sb_headers())
            r.raise_for_status()
            rows = r.json() or []
            if not rows:
                return None
            row = rows[0]
            if not row.get("is_active", True):
                return None
            sell  = float(row["sell"])       if row.get("sell")       is not None else None
            buy   = float(row["buy"])        if row.get("buy")        is not None else None
            cbr   = float(row["last_price"]) if row.get("last_price") is not None else None
            ts    = str(row.get("updated_at") or "")
            if sell is None or buy is None or sell <= 0 or buy <= 0:
                return None
            return sell, buy, (cbr or (sell + buy) / 2), ts
    except Exception as e:
        log.warning("Supabase fetch_energo_from_db failed: %s", e)
        return None

# ──────────────── RAPIRA FALLBACK ────────────────────
RAPIRA_RATES_URL = "https://api.rapira.net/open/market/rates"
RAPIRA_OB_URL    = "https://api.rapira.net/market/exchange-plate-mini?symbol=USDT/RUB"

class RapiraBlockedError(Exception):
    pass

def _backoff(attempt: int, base: float = 1.6, jitter: float = 0.6) -> float:
    return (base ** (attempt - 1)) + random.uniform(0, jitter)

async def _rapira_get_json(url: str, timeout: float = 15.0) -> Optional[Dict[str, Any]]:
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    transport = None
    if RAPIRA_PROXY:
        transport = httpx.AsyncHTTPTransport(proxy=RAPIRA_PROXY)
    for att in range(1, MAX_RETRIES + 1):
        try:
            async with httpx.AsyncClient(
                headers=headers,
                timeout=timeout,
                follow_redirects=True,
                transport=transport,
            ) as cli:
                r = await cli.get(url)
                if r.status_code == 451:
                    raise RapiraBlockedError("HTTP 451 (blocked)")
                r.raise_for_status()
                return r.json()
        except RapiraBlockedError:
            raise
        except Exception as e:
            log.warning("Rapira GET %s attempt %s failed: %s", url, att, e)
            if att < MAX_RETRIES:
                await asyncio.sleep(_backoff(att))
    return None

def _first_number_from_level(level) -> Optional[float]:
    try:
        if isinstance(level, (list, tuple)) and level:
            return float(level[0])
        if isinstance(level, dict) and "price" in level:
            return float(level["price"])
        return float(level)
    except Exception:
        return None

def _safe_float(v) -> Optional[float]:
    try:
        return float(v)
    except Exception:
        return None

async def fetch_rapira_usdtrub_best() -> Tuple[Optional[float], Optional[float]]:
    try:
        data = await _rapira_get_json(RAPIRA_OB_URL)
        if data:
            container = data.get("data", data)
            asks = container.get("asks")
            bids = container.get("bids")
            best_ask = _first_number_from_level(asks[0]) if asks else None
            best_bid = _first_number_from_level(bids[0]) if bids else None
            if best_ask is not None or best_bid is not None:
                return best_ask, best_bid
        rates = await _rapira_get_json(RAPIRA_RATES_URL)
        items = (
            (rates.get("data") or rates.get("rates") or rates.get("result") or rates.get("items"))
            if isinstance(rates, dict) else rates
        )
        if isinstance(items, list):
            target = next(
                (x for x in items
                 if (x.get("symbol") or x.get("pair") or x.get("name") or "")
                    .upper().replace("_", "/") == "USDT/RUB"),
                None
            )
            if target:
                ask = _safe_float(target.get("ask") or target.get("offer") or target.get("askPrice") or target.get("sell"))
                bid = _safe_float(target.get("bid") or target.get("bidPrice") or target.get("buy"))
                return ask, bid
        return (None, None)
    except RapiraBlockedError:
        return (None, None)

# ──────────────── COINGECKO FALLBACK ────────────────
async def fetch_coingecko_usdtrub() -> Optional[float]:
    """USDT/RUB mid-price с CoinGecko. Без прокси, доступно отовсюду."""
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {"ids": "tether", "vs_currencies": "rub"}
    for att in range(1, MAX_RETRIES + 1):
        try:
            async with httpx.AsyncClient(timeout=10.0) as cli:
                r = await cli.get(url, params=params, headers={"User-Agent": "Mozilla/5.0"})
                r.raise_for_status()
                data = r.json()
                price = (data.get("tether") or {}).get("rub")
                if price and price > 0:
                    return float(price)
        except Exception as e:
            log.warning("CoinGecko attempt %s failed: %s", att, e)
            if att < MAX_RETRIES:
                await asyncio.sleep(_backoff(att))
    return None

# ────────── ENERGO direct fallback ──────────
async def fetch_energo() -> Tuple[Optional[float], Optional[float], Optional[float]]:
    url = "https://ru.myfin.by/bank/energotransbank/currency/kaliningrad"
    for att in range(1, MAX_RETRIES + 1):
        try:
            async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}, timeout=15) as cli:
                r = await cli.get(url)
                r.raise_for_status()
                soup = BeautifulSoup(r.text, "html.parser")
                row = soup.select_one("table.table-best.white_bg tr:has(td.title)")
                if not row:
                    raise RuntimeError("row not found")
                buy, sell, cbr = [
                    float(td.text.replace(",", "."))
                    for td in row.find_all("td")[1:4]
                ]
                return sell, buy, cbr
        except Exception as e:
            log.warning("Energo direct attempt %s/%s: %s", att, MAX_RETRIES, e)
            if att < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)
    return None, None, None

# ──────────────── UNIFIED FETCHERS ─────────────────
async def get_kenig_usdt_rub() -> Tuple[Optional[float], Optional[float], str]:
    """
    Возвращает (kenig_sell, kenig_buy, источник).
    Источник: 'db' / 'rapira' / 'coingecko' / 'none'.
    """
    # 1. Из БД (если включено)
    if USE_DB_AS_PRIMARY:
        db = await fetch_kenig_from_db()
        if db:
            sell, buy, _ts = db
            log.info("USDT/RUB from DB: sell=%.4f buy=%.4f", sell, buy)
            return sell, buy, "db"

    # 2. Rapira напрямую
    ask, bid = await fetch_rapira_usdtrub_best()
    if ask is not None and bid is not None:
        sell = ask + KENIG_ASK_OFFSET
        buy  = bid + KENIG_BID_OFFSET
        log.info("USDT/RUB from Rapira direct: sell=%.4f buy=%.4f", sell, buy)
        return sell, buy, "rapira"

    # 3. CoinGecko mid + симметричный спред
    mid = await fetch_coingecko_usdtrub()
    if mid is not None:
        sell = mid + COINGECKO_FALLBACK_OFFSET
        buy  = mid - COINGECKO_FALLBACK_OFFSET
        log.info("USDT/RUB from CoinGecko mid=%.4f → sell=%.4f buy=%.4f",
                 mid, sell, buy)
        return sell, buy, "coingecko"

    log.error("USDT/RUB: все источники недоступны")
    return None, None, "none"

async def get_energo_usd_rub() -> Tuple[Optional[float], Optional[float], Optional[float], str]:
    """
    Возвращает (sell, buy, cbr, источник).
    Источник: 'db' / 'direct' / 'none'.
    """
    # 1. Из БД
    if USE_DB_AS_PRIMARY:
        db = await fetch_energo_from_db()
        if db:
            sell, buy, cbr, _ts = db
            log.info("USD/RUB from DB: sell=%.2f buy=%.2f cbr=%.2f", sell, buy, cbr)
            return sell, buy, cbr, "db"

    # 2. Прямой парсер myfin.by
    sell, buy, cbr = await fetch_energo()
    if sell is not None and buy is not None:
        log.info("USD/RUB from myfin.by direct: sell=%.2f buy=%.2f", sell, buy)
        return sell, buy, cbr, "direct"

    return None, None, None, "none"

# ────────────── TELEGRAM COMMANDS ───────────────────
def auth_ok(uid: int) -> bool:
    return uid in AUTHORIZED_USERS

async def cmd_auth(u, ctx):
    if len(ctx.args) != 1:
        return await u.message.reply_text("Введите пароль: /auth <пароль>")
    if ctx.args[0] == PASSWORD:
        AUTHORIZED_USERS.add(u.effective_user.id)
        await u.message.reply_text("✅ Доступ разрешён.")
    else:
        await u.message.reply_text("❌ Неверный пароль.")

async def cmd_start(u, _):
    await u.message.reply_text("Бот активен. /help")

async def cmd_help(u, _):
    await u.message.reply_text(
        "/start /auth /check /change /show_offsets /help\n\n"
        "Примечание: /change в этом боте меняет только локальные fallback-оффсеты. "
        "Реальные оффсеты для сайта и Exnode живут в supabase-worker."
    )

async def cmd_check(u, _):
    if not auth_ok(u.effective_user.id):
        return await u.message.reply_text("Нет доступа. /auth <пароль>")
    await send_rates_message(APP)
    await u.message.reply_text("Отчёт выслан.")

async def cmd_change(u, ctx):
    if not auth_ok(u.effective_user.id):
        return await u.message.reply_text("Нет доступа.")
    try:
        global KENIG_ASK_OFFSET, KENIG_BID_OFFSET
        KENIG_ASK_OFFSET, KENIG_BID_OFFSET = map(float, ctx.args[:2])
        await u.message.reply_text(
            f"Локальные fallback-оффсеты: ask +{KENIG_ASK_OFFSET}  bid {KENIG_BID_OFFSET}\n"
            f"⚠️ Применяются только если БД недоступна. "
            f"Для production-курсов меняйте в supabase-worker."
        )
    except Exception:
        await u.message.reply_text("Пример: /change 1.0 -0.5")

async def cmd_show(u, _):
    if not auth_ok(u.effective_user.id):
        return await u.message.reply_text("Нет доступа.")
    await u.message.reply_text(
        f"Локальные fallback: Ask +{KENIG_ASK_OFFSET}  Bid {KENIG_BID_OFFSET}\n"
        f"USE_DB_AS_PRIMARY: {USE_DB_AS_PRIMARY}"
    )

# ────────────── FORMATTING ──────────────
NUM_W = 6
SEG_W = 20

def _seg(lbl: str, val: Optional[float], unit: str = "₽", comma: bool = True) -> str:
    n = "—" if val is None else f"{val:.2f}"
    s = f"{lbl}: {n.rjust(NUM_W)} {unit}"
    if comma:
        s += ","
    return s.ljust(SEG_W)

def _inline_line(segments: List[str]) -> str:
    return "".join(segments).rstrip()

# ────────────── MESSAGE SENDER ──────────────────────
async def send_rates_message(app):
    kenig_sell, kenig_buy, kenig_src = await get_kenig_usdt_rub()
    en_sell, en_buy, en_cbr, en_src  = await get_energo_usd_rub()

    ts = datetime.now(KAL_TZ).strftime("%d.%m.%Y %H:%M:%S")
    lines: List[str] = [ts, ""]

    # KenigSwap
    lines.append("KenigSwap rate USDT/RUB")
    if kenig_sell is not None and kenig_buy is not None:
        lines.append(_inline_line([
            _seg("Покупка", kenig_buy,  "₽", comma=True),
            _seg("Продажа", kenig_sell, "₽", comma=False),
        ]))
    else:
        lines.append("— нет данных —")
    lines.append("")

    # EnergoTransBank
    lines.append("EnergoTransBank rate USD/RUB")
    if en_sell is not None and en_buy is not None:
        lines.append(_inline_line([
            _seg("Покупка", en_buy,  "₽", comma=True),
            _seg("Продажа", en_sell, "₽", comma=True),
            _seg("ЦБ",      en_cbr,  "₽", comma=False),
        ]))
    else:
        lines.append("— нет данных —")

    if SHOW_RAPIRA_META:
        lines += ["", f"Источники: kenig={kenig_src}, energo={en_src}"]

    msg = "<pre>" + html.escape("\n".join(lines)) + "</pre>"
    try:
        await app.bot.send_message(
            chat_id=CHAT_ID,
            text=msg,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception as e:
        log.error("Send error: %s", e)

# ───────────────────── MAIN ─────────────────────────
def main() -> None:
    global APP
    if not TOKEN:
        raise RuntimeError("TG_BOT_TOKEN is not set")
    APP = ApplicationBuilder().token(TOKEN).build()
    APP.add_handler(CommandHandler("start", cmd_start))
    APP.add_handler(CommandHandler("help", cmd_help))
    APP.add_handler(CommandHandler("auth", cmd_auth))
    APP.add_handler(CommandHandler("check", cmd_check))
    APP.add_handler(CommandHandler("change", cmd_change))
    APP.add_handler(CommandHandler("show_offsets", cmd_show))
    sched = AsyncIOScheduler(timezone=KAL_TZ)
    sched.add_job(send_rates_message, "interval", minutes=2, seconds=30, args=[APP])
    sched.start()
    log.info(
        "Bot started. USE_DB_AS_PRIMARY=%s, Supabase=%s",
        USE_DB_AS_PRIMARY, bool(SUPABASE_URL and SUPABASE_KEY),
    )
    APP.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
