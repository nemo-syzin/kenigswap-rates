# ───────────────────── IMPORTS ──────────────────────
import asyncio, contextlib, html, logging, os, subprocess, random
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, Dict, Any

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from telegram.ext import ApplicationBuilder, CommandHandler
from dotenv import load_dotenv

load_dotenv()

# ───────────────────── CONFIG ───────────────────────
TOKEN            = os.getenv("TG_BOT_TOKEN")
PASSWORD         = os.getenv("TG_BOT_PASS")
CHAT_ID          = os.getenv("TG_CHAT_ID", "@KaliningradCryptoRatesKenigSwap")
KAL_TZ           = timezone(timedelta(hours=2))

KENIG_ASK_OFFSET = 0.8   # +₽ к ask → KenigSwap
KENIG_BID_OFFSET = -0.9  # –₽ к bid → KenigSwap

MAX_RETRIES      = 3
RETRY_DELAY      = 5
AUTHORIZED_USERS: set[int] = set()

# Rapira прокси (опционально)
RAPIRA_PROXY     = os.getenv("RAPIRA_PROXY")

SHOW_RAPIRA_META = False

# ───────────────────── LOGGER ───────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ────────────────── PLAYWRIGHT CHROMIUM ─────────────
def install_chromium() -> None:
    try:
        subprocess.run(["playwright", "install", "chromium"], check=True)
    except Exception as exc:
        log.warning("Playwright install error (игнорируем): %s", exc)

# ───────────────────── SCRAPERS ─────────────────────
GRINEX_URL = "https://grinex.io/trading/usdta7a5?lang=en"
TIMEOUT_MS = 60_000

# ====== RAPIRA PARSER ========================================
RAPIRA_RATES_URL = "https://api.rapira.net/open/market/rates"
RAPIRA_OB_URL    = "https://api.rapira.net/market/exchange-plate-mini?symbol=USDT/RUB"

_last_rapira_usdtrub: Tuple[Optional[float], Optional[float]] = (None, None)
_last_rapira_ts: Optional[datetime] = None
_last_rapira_status: str = ""

def _backoff(attempt: int, base: float = 1.6, jitter: float = 0.6) -> float:
    return (base ** (attempt - 1)) + random.uniform(0, jitter)

class RapiraBlockedError(Exception):
    pass

async def _rapira_get_json(url: str, timeout: float = 15.0) -> Optional[Dict[str, Any]]:
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    proxies = None
    if RAPIRA_PROXY:
        proxies = {"http://": RAPIRA_PROXY, "https://": RAPIRA_PROXY}

    for att in range(1, MAX_RETRIES + 1):
        try:
            async with httpx.AsyncClient(
                headers=headers, timeout=timeout,
                follow_redirects=True, proxies=proxies
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
    except:
        return None

def _safe_float(v):
    try: return float(v)
    except: return None

async def fetch_rapira_usdtrub_best() -> Tuple[Optional[float], Optional[float]]:
    global _last_rapira_usdtrub, _last_rapira_ts, _last_rapira_status

    try:
        # 1) Mini orderbook
        data = await _rapira_get_json(RAPIRA_OB_URL)
        if data:
            container = data.get("data", data)
            asks = container.get("asks")
            bids = container.get("bids")
            best_ask = _first_number_from_level(asks[0]) if asks else None
            best_bid = _first_number_from_level(bids[0]) if bids else None
            if best_ask or best_bid:
                _last_rapira_usdtrub = (best_ask, best_bid)
                _last_rapira_ts = datetime.now(KAL_TZ)
                _last_rapira_status = "ok"
                return best_ask, best_bid

        # 2) Fallback — rates
        rates = await _rapira_get_json(RAPIRA_RATES_URL)
        items = (rates.get("data") or rates.get("rates")
                 or rates.get("result") or rates.get("items")
                 if isinstance(rates, dict) else rates)

        if isinstance(items, list):
            target = next(
                (x for x in items
                 if (x.get("symbol") or x.get("pair") or x.get("name") or "")
                    .upper().replace("_","/") == "USDT/RUB"),
                None
            )
            if target:
                ask = _safe_float(target.get("ask") or target.get("offer")
                                  or target.get("askPrice") or target.get("sell"))
                bid = _safe_float(target.get("bid") or target.get("bidPrice")
                                  or target.get("buy"))
                _last_rapira_usdtrub = (ask, bid)
                _last_rapira_ts = datetime.now(KAL_TZ)
                _last_rapira_status = "fallback"
                return ask, bid

        _last_rapira_status = "error"
        return (None, None)

    except RapiraBlockedError:
        _last_rapira_status = "blocked"
        return (None, None)

# ========= BESTCHANGE ============================================
async def fetch_bestchange_sell() -> Optional[float]:
    url = "https://www.bestchange.com/cash-ruble-to-tether-trc20-in-klng.html"
    for att in range(1, MAX_RETRIES + 1):
        try:
            async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}, timeout=15) as cli:
                r = await cli.get(url)
                r.raise_for_status()
                soup = BeautifulSoup(r.text, "html.parser")
                val  = soup.select_one("div.fs")
                if val:
                    txt = "".join(c for c in val.text if c.isdigit() or c in ",.")
                    return float(txt.replace(",", "."))
        except Exception as e:
            log.warning("BestChange sell attempt %s/%s: %s", att, MAX_RETRIES, e)
            if att < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)
    return None

async def fetch_bestchange_buy() -> Optional[float]:
    url = "https://www.bestchange.com/tether-trc20-to-cash-ruble-in-klng.html"
    for att in range(1, MAX_RETRIES + 1):
        try:
            async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}, timeout=15) as cli:
                r = await cli.get(url)
                r.raise_for_status()
                soup  = BeautifulSoup(r.text, "html.parser")
                row   = soup.select_one("table#content_table tr[onclick]")
                td    = row.find_all("td", class_="bi")[1] if row else None
                if td and "RUB Cash" in td.text:
                    txt = "".join(c for c in td.text if c.isdigit() or c in ",.")
                    return float(txt.replace(",", "."))
        except Exception as e:
            log.warning("BestChange buy attempt %s/%s: %s", att, MAX_RETRIES, e)
            if att < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)
    return None

# ============= ENERGOTRANSBANK (исправленный URL) ==================
async def fetch_energo() -> Tuple[Optional[float], Optional[float], Optional[float]]:
    url = "https://ru.myfin.by/bank/energotransbank/currency/kaliningrad"

    for att in range(1, MAX_RETRIES + 1):
        try:
            async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}, timeout=15) as cli:
                r = await cli.get(url)
                r.raise_for_status()

                soup = BeautifulSoup(r.text, "html.parser")
                row  = soup.select_one("table.table-best.white_bg tr:has(td.title)")
                if not row:
                    raise RuntimeError("row not found")

                buy, sell, cbr = [
                    float(td.text.replace(",", "."))
                    for td in row.find_all("td")[1:4]
                ]

                return sell, buy, cbr

        except Exception as e:
            log.warning("Energo attempt %s/%s: %s", att, MAX_RETRIES, e)
            if att < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)

    return None, None, None

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
    await u.message.reply_text("/start /auth /check /change /show_offsets /help")

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
        await u.message.reply_text(f"Новые оффсеты: ask +{KENIG_ASK_OFFSET}  bid {KENIG_BID_OFFSET}")
    except:
        await u.message.reply_text("Пример: /change 1.0 -0.5")

async def cmd_show(u, _):
    if not auth_ok(u.effective_user.id):
        return await u.message.reply_text("Нет доступа.")
    await u.message.reply_text(f"Ask +{KENIG_ASK_OFFSET}  Bid {KENIG_BID_OFFSET}")

# ────────────── MESSAGE SENDER ──────────────────────
async def send_rates_message(app):
    gr_ask, gr_bid = await fetch_rapira_usdtrub_best()

    bc_sell = await fetch_bestchange_sell()
    bc_buy  = await fetch_bestchange_buy()

    en_sell, en_buy, en_cbr = await fetch_energo()

    ts = datetime.now(KAL_TZ).strftime("%d.%m.%Y %H:%M:%S")
    lines = [ts, ""]

    # KenigSwap
    lines += ["KenigSwap rate USDT/RUB"]
    if gr_ask is not None and gr_bid is not None:
        lines.append(
            f"Продажа: {gr_ask + KENIG_ASK_OFFSET:.2f} ₽, "
            f"Покупка: {gr_bid + KENIG_BID_OFFSET:.2f} ₽"
        )
    else:
        lines.append("— нет данных —")

    lines.append("")

    # BestChange
    lines += ["BestChange rate USDT/RUB"]
    if bc_sell is not None and bc_buy is not None:
        lines.append(f"Продажа: {bc_sell:.2f} ₽, Покупка: {bc_buy:.2f} ₽")
    else:
        lines.append("— нет данных —")

    lines.append("")

    # Energo
    lines += ["EnergoTransBank rate USD/RUB"]
    if en_sell and en_buy and en_cbr:
        lines.append(
            f"Продажа: {en_sell:.2f} ₽, Покупка: {en_buy:.2f} ₽, ЦБ: {en_cbr:.2f} ₽"
        )
    else:
        lines.append("— нет данных —")

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
    install_chromium()

    global APP
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

    log.info("Bot started.")
    APP.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
