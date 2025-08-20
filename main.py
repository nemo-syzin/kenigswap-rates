# ───────────────────── IMPORTS ──────────────────────
import asyncio, contextlib, html, logging, os, subprocess, random, re
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
CHAT_ID          = os.getenv("TG_CHAT_ID", "@KaliningradCryptoRatesKenigSwap")  # можно переопределить в .env
KAL_TZ           = timezone(timedelta(hours=2))

KENIG_ASK_OFFSET = 0.8   # +₽ к ask → KenigSwap
KENIG_BID_OFFSET = -0.9  # –₽ к bid → KenigSwap

MAX_RETRIES      = 3
RETRY_DELAY      = 5
AUTHORIZED_USERS: set[int] = set()

# Rapira прокси (опционально): http://user:pass@host:port или socks5://host:port
RAPIRA_PROXY     = os.getenv("RAPIRA_PROXY")

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

# ====== RAPIRA PARSER (новый, с 451/прокси/кэшом) ==========================
RAPIRA_RATES_URL = "https://api.rapira.net/open/market/rates"
RAPIRA_OB_URL    = "https://api.rapira.net/market/exchange-plate-mini?symbol=USDT/RUB"

# Глобальный кэш последних удачных USDT/RUB (ask, bid)
_last_rapira_usdtrub: Tuple[Optional[float], Optional[float]] = (None, None)
_last_rapira_ts: Optional[datetime] = None
_last_rapira_status: str = ""  # 'ok' | 'fallback' | 'blocked' | 'error'

def _backoff(attempt: int, base: float = 1.6, jitter: float = 0.6) -> float:
    return (base ** (attempt - 1)) + random.uniform(0, jitter)

class RapiraBlockedError(Exception):
    """HTTP 451: гео/IP-блок — не ретраим бессмысленно."""

async def _rapira_get_json(url: str, timeout: float = 15.0) -> Optional[Dict[str, Any]]:
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    proxies = None
    if RAPIRA_PROXY:
        proxies = {"http://": RAPIRA_PROXY, "https://": RAPIRA_PROXY}

    for att in range(1, MAX_RETRIES + 1):
        try:
            async with httpx.AsyncClient(
                headers=headers, timeout=timeout, follow_redirects=True, proxies=proxies
            ) as cli:
                r = await cli.get(url)
                if r.status_code == 451:
                    # Юрисдикционная/гео-блокировка — выходим сразу
                    raise RapiraBlockedError("HTTP 451 (geo/IP blocked)")
                r.raise_for_status()
                return r.json()
        except RapiraBlockedError:
            log.warning("Rapira blocked (451) at %s", url)
            raise
        except Exception as e:
            log.warning("Rapira GET %s attempt %s failed: %s", url, att, e)
            if att < MAX_RETRIES:
                await asyncio.sleep(_backoff(att))
    return None

def _first_number_from_level(level) -> Optional[float]:
    """Уровень может быть [price, amount] или dict с ключом price."""
    try:
        if isinstance(level, (list, tuple)) and level:
            return float(level[0])
        if isinstance(level, dict) and "price" in level:
            return float(level["price"])
        return float(level)  # вдруг пришло число/строка
    except Exception:
        return None

def _safe_float(val) -> Optional[float]:
    try:
        return float(val)
    except Exception:
        return None

async def fetch_rapira_usdtrub_best() -> Tuple[Optional[float], Optional[float]]:
    """
    Возвращает (ask, bid) для USDT/RUB:
    - сперва из мини-стакана exchange-plate-mini;
    - фоллбек к open/market/rates;
    - при 451 возвращает кэш, если есть.
    """
    global _last_rapira_usdtrub, _last_rapira_ts, _last_rapira_status

    try:
        # 1) Мини-стакан
        data = await _rapira_get_json(RAPIRA_OB_URL)
        if data:
            container = data.get("data", data)
            asks = container.get("asks")
            bids = container.get("bids")
            best_ask = _first_number_from_level(asks[0]) if isinstance(asks, list) and asks else None
            best_bid = _first_number_from_level(bids[0]) if isinstance(bids, list) and bids else None
            if best_ask is not None or best_bid is not None:
                _last_rapira_usdtrub = (best_ask, best_bid)
                _last_rapira_ts = datetime.now(KAL_TZ)
                _last_rapira_status = "ok"
                return best_ask, best_bid

        # 2) Фоллбек — общий список курсов
        rates = await _rapira_get_json(RAPIRA_RATES_URL)
        items = (rates.get("data") or rates.get("rates") or rates.get("result") or rates.get("items")) if isinstance(rates, dict) else rates
        if isinstance(items, list):
            target = next(
                (it for it in items if (it.get("symbol") or it.get("pair") or it.get("name") or "").upper().replace("_","/") == "USDT/RUB"),
                None
            )
            if target:
                ask = _safe_float(target.get("ask") or target.get("offer") or target.get("askPrice") or target.get("sell"))
                bid = _safe_float(target.get("bid") or target.get("bidPrice") or target.get("buy"))
                _last_rapira_usdtrub = (ask, bid)
                _last_rapira_ts = datetime.now(KAL_TZ)
                _last_rapira_status = "fallback"
                return ask, bid

        _last_rapira_status = "error"
        return _last_rapira_usdtrub  # вернём последнюю удачную, если была
    except RapiraBlockedError:
        _last_rapira_status = "blocked"
        return _last_rapira_usdtrub  # вернём кэш, чтобы не «пусто»

async def fetch_rapira_rates_all() -> Dict[str, Dict[str, Optional[float]]]:
    """
    Словарь по всем парам:
      {"USDT/RUB": {"ask": 123.45, "bid": 122.90, "last": 123.10}, ...}
    """
    out: Dict[str, Dict[str, Optional[float]]] = {}
    try:
        rates = await _rapira_get_json(RAPIRA_RATES_URL)
    except RapiraBlockedError:
        # Если заблокировано, вернуть то, что есть (скорее всего пусто)
        return out

    if not rates:
        return out

    items = None
    if isinstance(rates, dict):
        items = rates.get("data") or rates.get("rates") or rates.get("result") or rates.get("items")
    elif isinstance(rates, list):
        items = rates

    if not isinstance(items, list):
        return out

    for it in items:
        sym = (it.get("symbol") or it.get("pair") or it.get("name") or "").upper().replace("_", "/")
        if not sym:
            continue
        ask = _safe_float(it.get("ask") or it.get("offer") or it.get("askPrice") or it.get("sell"))
        bid = _safe_float(it.get("bid") or it.get("bidPrice") or it.get("buy"))
        last = _safe_float(it.get("last") or it.get("price") or it.get("close"))
        out[sym] = {"ask": ask, "bid": bid, "last": last}
    return out
# ===========================================================================

# ====== GRINEX PARSER (оставлен, но ЗАКОММЕНТИРОВАН) =======================
# async def fetch_grinex_rate() -> Tuple[Optional[float], Optional[float]]:
#     for att in range(1, MAX_RETRIES + 1):
#         try:
#             async with async_playwright() as p:
#                 browser = await p.chromium.launch(
#                     headless=True,
#                     args=[
#                         "--disable-blink-features=AutomationControlled",
#                         "--proxy-server='direct://'",
#                         "--proxy-bypass-list=*",
#                     ],
#                 )
#                 ctx  = await browser.new_context(
#                     user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#                                 "AppleWebKit/537.36 (KHTML, like Gecko) "
#                                 "Chrome/123.0.0.0 Safari/537.36")
#                 )
#                 page = await ctx.new_page()
#                 await page.goto(GRINEX_URL, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
#                 with contextlib.suppress(Exception):
#                     await page.locator("button:text('Accept')").click(timeout=3_000)
#
#                 ask_sel = "tbody.usdta7a5_ask.asks tr[data-price]"
#                 bid_sel = "tbody.usdta7a5_bid.bids tr[data-price]"
#                 await page.wait_for_selector(ask_sel, timeout=TIMEOUT_MS)
#                 await page.wait_for_selector(bid_sel, timeout=TIMEOUT_MS)
#
#                 ask = float(await page.locator(ask_sel).first.get_attribute("data-price"))
#                 bid = float(await page.locator(bid_sel).first.get_attribute("data-price"))
#                 await browser.close()
#                 return ask, bid
#         except Exception as e:
#             log.warning("Grinex attempt %s/%s failed: %s", att, MAX_RETRIES, e)
#             if att < MAX_RETRIES:
#                 await asyncio.sleep(RETRY_DELAY)
#     return None, None
# ===========================================================================

# ────────────── ПРОЧИЕ СКРАПЕРЫ (как были) ────────────────────
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
                buy, sell, cbr = [float(td.text.replace(",", ".")) for td in row.find_all("td")[1:4]]
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

async def cmd_start(u, _): await u.message.reply_text("Бот активен. /help")
async def cmd_help(u, _):  await u.message.reply_text("/start /auth /check /change /show_offsets /help")

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
    except Exception:
        await u.message.reply_text("Пример: /change 1.0 -0.5")

async def cmd_show(u, _):
    if not auth_ok(u.effective_user.id):
        return await u.message.reply_text("Нет доступа.")
    await u.message.reply_text(f"Ask +{KENIG_ASK_OFFSET}  Bid {KENIG_BID_OFFSET}")

# ────────────── MESSAGE SENDER ──────────────────────
async def send_rates_message(app):
    # Rapira вместо Grinex
    gr_ask, gr_bid = await fetch_rapira_usdtrub_best()

    bc_sell = await fetch_bestchange_sell()
    bc_buy  = await fetch_bestchange_buy()
    en_sell, en_buy, en_cbr = await fetch_energo()

    ts = datetime.now(KAL_TZ).strftime("%d.%m.%Y %H:%M:%S")
    lines = [ts, ""]

    # KenigSwap (Rapira + оффсеты)
    lines += ["KenigSwap rate USDT/RUB"]
    if gr_ask is not None and gr_bid is not None:
        lines.append(f"Продажа: {gr_ask + KENIG_ASK_OFFSET:.2f} ₽, "
                     f"Покупка: {gr_bid + KENIG_BID_OFFSET:.2f} ₽")
    else:
        lines.append("— нет данных —")

    # пометка источника Rapira
    src_note = {
        "ok": "Rapira (orderbook)",
        "fallback": "Rapira (rates)",
        "blocked": "Rapira: 451 blocked — показаны последние значения" if _last_rapira_usdtrub != (None, None) else "Rapira: 451 blocked",
        "error": "Rapira: нет данных — показаны последние значения" if _last_rapira_usdtrub != (None, None) else "Rapira: нет данных",
    }.get(_last_rapira_status, "Rapira")
    lines.append(f"Источник: {src_note}")
    if _last_rapira_ts:
        lines.append(f"Обновлено: {_last_rapira_ts.strftime('%d.%m.%Y %H:%M:%S')}")
    lines.append("")

    # BestChange
    lines += ["BestChange rate USDT/RUB"]
    if bc_sell is not None and bc_buy is not None:
        lines.append(f"Продажа: {bc_sell:.2f} ₽, Покупка: {bc_buy:.2f} ₽")
    else:
        lines.append("— нет данных —")
    lines.append("")

    # EnergoTransBank
    lines += ["EnergoTransBank rate USD/RUB"]
    if en_sell is not None and en_buy is not None and en_cbr is not None:
        lines.append(f"Продажа: {en_sell:.2f} ₽, "
                     f"Покупка: {en_buy:.2f} ₽, ЦБ: {en_cbr:.2f} ₽")
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
    install_chromium()  # не обязателен для Rapira, но пусть остаётся

    global APP
    APP = ApplicationBuilder().token(TOKEN).build()
    APP.add_handler(CommandHandler("start",        cmd_start))
    APP.add_handler(CommandHandler("help",         cmd_help))
    APP.add_handler(CommandHandler("auth",         cmd_auth))
    APP.add_handler(CommandHandler("check",        cmd_check))
    APP.add_handler(CommandHandler("change",       cmd_change))
    APP.add_handler(CommandHandler("show_offsets", cmd_show))

    sched = AsyncIOScheduler(timezone=KAL_TZ)
    sched.add_job(send_rates_message, trigger="interval", minutes=2, seconds=30, args=[APP])
    sched.start()

    log.info("Bot started.")
    APP.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
