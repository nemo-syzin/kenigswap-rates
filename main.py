# ───────────────────── IMPORTS ──────────────────────
import asyncio, html, logging, os, subprocess
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from playwright.async_api import async_playwright
from supabase import create_client, Client
from telegram.ext import ApplicationBuilder, CommandHandler

load_dotenv()

# ─────────────── CONSTANTS / CONFIG ────────────────
CRYPTOS = [
    "BTC", "ETH", "SOL", "XRP", "LTC", "ADA", "DOGE", "TRX", "DOT", "LINK",
    "AVAX", "MATIC", "BCH", "ATOM", "NEAR", "ETC", "FIL", "UNI", "ARB", "APT",
]
ASSETS        = CRYPTOS + ["USDT", "RUB"]
BYBIT_SYMBOLS = [f"{c}USDT" for c in CRYPTOS]

_SOURCE_PAIR = {
    "kenig":      ("USDT", "RUB"),
    "bestchange": ("USDT", "RUB"),
    "energo":     ("USD",  "RUB"),
}

TOKEN          = os.getenv("TG_BOT_TOKEN")
PASSWORD       = os.getenv("TG_BOT_PASS")
SUPABASE_URL   = os.getenv("SUPABASE_URL")
SUPABASE_KEY   = os.getenv("SUPABASE_KEY")
CHAT_ID        = "@KaliningradCryptoRatesKenigSwap"
KAL_TZ         = timezone(timedelta(hours=2))

KENIG_ASK_OFFSET = 0.8
KENIG_BID_OFFSET = -0.9
DERIVED_SELL_FEE = 0.01
DERIVED_BUY_FEE  = -0.01

MIN_EQ_USDT, MAX_EQ_USDT, RESERVE_EQ_USDT = 1_000, 1_000_000, 1_000_000
MAX_RETRIES, RETRY_DELAY                  = 3, 5
AUTHORIZED_USERS: set[int]                = set()

# ───────────────────── LOGGER ───────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("kenig_bot")

# ────────────────── SUPABASE ────────────────────────
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ────────────── PLAYWRIGHT (headless) ───────────────
def install_chromium() -> None:
    """При первом старте докачиваем Chromium в локальный кеш (≈150 МБ)."""
    try:
        subprocess.run(["playwright", "install", "chromium"], check=True)
        os.environ["PLAYWRIGHT_BROWSERS_PATH"] = "0"
        log.info("Playwright browser installed")
    except Exception as exc:  # уже скачан — ничего страшного
        log.warning("Playwright install error: %s", exc)

# ─────────────────── HELPERS ────────────────────────
async def upsert_rate(source: str, sell: float, buy: float) -> None:
    base, quote = _SOURCE_PAIR[source]
    record = {
        "source":           source,
        "exchange_source":  source,
        "base":             base,
        "quote":            quote,
        "sell":             round(sell, 2),
        "buy":              round(buy, 2),
        "last_price":       round((sell + buy) / 2, 4),
        "min_amount":       None,
        "max_amount":       None,
        "reserve":          None,
        "conditions":       "KYC",
        "working_hours":    "24/7",
        "operational_mode": "manual",
        "is_active":        True,
        "updated_at":       datetime.utcnow().isoformat(),
    }
    await asyncio.get_running_loop().run_in_executor(
        None,
        lambda: sb.table("kenig_rates")
                  .upsert(record, on_conflict="source,base,quote")
                  .execute(),
    )
    log.info("Supabase upsert OK: %s", source)

async def mark_inactive(source: str) -> None:
    base, quote = _SOURCE_PAIR[source]
    sb.table("kenig_rates") \
      .update({"is_active": False, "updated_at": datetime.utcnow().isoformat()}) \
      .eq("source", source).eq("base", base).eq("quote", quote) \
      .execute()

# ─────────────── BYBIT SPOT API ────────────────
async def fetch_bybit_prices() -> dict[str, float]:
    url, params = "https://api.bybit.com/v5/market/tickers", {"category": "spot"}
    prices: dict[str, float] = {"USDT": 1.0}

    async with httpx.AsyncClient(timeout=10) as cli:
        r = await cli.get(url, params=params)
        r.raise_for_status()
        for itm in r.json().get("result", {}).get("list", []):
            sym = itm["symbol"]
            if sym in BYBIT_SYMBOLS:
                prices[sym[:-4]] = float(itm["lastPrice"])

    log.info("Bybit prices fetched: %s / 20", len(prices) - 1)
    return prices

async def get_usdt_rub_fiat() -> float:
    return (await fetch_bestchange_sell()) or 80.0

# ─────────────── FULL DERIVED MATRIX ────────────────
async def build_matrix() -> list[dict]:
    basics        = await fetch_bybit_prices()
    basics["RUB"] = 1 / await get_usdt_rub_fiat()
    now_iso       = datetime.utcnow().isoformat()
    rows: list[dict] = []

    for base in ASSETS:
        for quote in ASSETS:
            if base == quote or base not in basics or quote not in basics:
                continue
            rate = (1 / basics[quote]) if base == "USDT" \
                else basics[base] if quote == "USDT" \
                else basics[base] / basics[quote]
            last = round(rate, 8)
            rows.append({
                "source":           "derived",
                "exchange_source":  "bybit",
                "base":             base,
                "quote":            quote,
                "last_price":       last,
                "sell":             round(last * (1 + DERIVED_SELL_FEE), 8),
                "buy":              round(last * (1 + DERIVED_BUY_FEE),  8),
                "min_amount":       None,
                "max_amount":       None,
                "reserve":          None,
                "conditions":       "KYC",
                "working_hours":    "24/7",
                "operational_mode": "manual",
                "is_active":        True,
                "updated_at":       now_iso,
            })
    return rows

async def refresh_full_matrix() -> None:
    rows = await build_matrix()
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, lambda: sb.table("kenig_rates")
                                            .delete().eq("source", "derived").execute())
    await loop.run_in_executor(None, lambda: sb.table("kenig_rates")
                                            .insert(rows).execute())
    log.info("Full matrix refreshed: %s rows", len(rows))

# ─────── LIMITS / RESERVE / is_active пересчёт ──────
async def update_limits_dynamic() -> None:
    prices = await fetch_bybit_prices()
    prices.update({"RUB": 1 / await get_usdt_rub_fiat(), "USDT": 1.0})

    rows = sb.table("kenig_rates").select("source,base,quote").execute().data or []
    if not rows:
        return
    now, patched = datetime.utcnow().isoformat(), []

    for r in rows:
        pb, pq = prices.get(r["base"]), prices.get(r["quote"])
        if not pb or not pq:
            patched.append({**r, "is_active": False, "updated_at": now})
            continue
        patched.append({
            **r,
            "min_amount": round(MIN_EQ_USDT / pb, 8),
            "max_amount": round(MAX_EQ_USDT / pb, 8),
            "reserve":    round(RESERVE_EQ_USDT / pq, 8),
            "conditions": "KYC",
            "working_hours": "24/7",
            "operational_mode": "manual",
            "is_active": True,
            "updated_at": now,
        })

    sb.table("kenig_rates").upsert(patched, on_conflict="source,base,quote").execute()
    log.info("✔ limits updated for %s pairs", len(patched))

# ──────────────── SCRAPERS ────────────────
GRINEX_URL, TIMEOUT_MS = "https://grinex.io/trading/usdta7a5?lang=en", 60_000

async def fetch_grinex_rate() -> Tuple[Optional[float], Optional[float]]:
    for att in range(1, MAX_RETRIES + 1):
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
                page = await (await browser.new_context()).new_page()
                await page.goto(GRINEX_URL, timeout=TIMEOUT_MS, wait_until="domcontentloaded")
                ask_sel = "tbody.usdta7a5_ask.asks tr[data-price]"
                bid_sel = "tbody.usdta7a5_bid.bids tr[data-price]"
                await page.wait_for_selector(ask_sel, timeout=TIMEOUT_MS)
                await page.wait_for_selector(bid_sel, timeout=TIMEOUT_MS)
                ask = float(await page.locator(ask_sel).first.get_attribute("data-price"))
                bid = float(await page.locator(bid_sel).first.get_attribute("data-price"))
                await browser.close()
                return ask, bid
        except Exception as e:
            log.warning("Grinex attempt %s/%s failed: %s", att, MAX_RETRIES, e)
            if att < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)
    return None, None

async def fetch_bestchange_sell() -> Optional[float]:
    url = "https://www.bestchange.com/cash-ruble-to-tether-trc20-in-klng.html"
    for att in range(1, MAX_RETRIES + 1):
        try:
            async with httpx.AsyncClient(timeout=15) as cli:
                res = await cli.get(url)
                res.raise_for_status()
                soup = BeautifulSoup(res.text, "html.parser")
                val = soup.select_one("div.fs")
                if val:
                    return float("".join(c for c in val.text if c.isdigit() or c in ",.")
                                   .replace(",", "."))
        except Exception as e:
            log.warning("BestChange sell attempt %s/%s: %s", att, MAX_RETRIES, e)
            if att < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)
    return None

async def fetch_bestchange_buy() -> Optional[float]:
    url = "https://www.bestchange.com/tether-trc20-to-cash-ruble-in-klng.html"
    for att in range(1, MAX_RETRIES + 1):
        try:
            async with httpx.AsyncClient(timeout=15) as cli:
                res = await cli.get(url)
                res.raise_for_status()
                soup = BeautifulSoup(res.text, "html.parser")
                row = soup.select_one("table#content_table tr[onclick]")
                td  = row.find_all("td", class_="bi")[1] if row else None
                if td and "RUB Cash" in td.text:
                    return float("".join(c for c in td.text if c.isdigit() or c in ",.")
                                   .replace(",", "."))
        except Exception as e:
            log.warning("BestChange buy attempt %s/%s: %s", att, MAX_RETRIES, e)
            if att < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)
    return None

async def fetch_energo() -> Tuple[Optional[float], Optional[float], Optional[float]]:
    url = "https://ru.myfin.by/bank/energotransbank/currency/kaliningrad"
    headers = {"User-Agent": "Mozilla/5.0"}
    for att in range(1, MAX_RETRIES + 1):
        try:
            async with httpx.AsyncClient(timeout=15) as cli:
                res = await cli.get(url, headers=headers)
                res.raise_for_status()
                soup = BeautifulSoup(res.text, "html.parser")
                row  = soup.select_one("table.table-best.white_bg tr:has(td.title)")
                if not row:
                    raise ValueError("table row not found")
                buy, sell, cbr = [float(td.text.replace(",", ".")) for td in row.find_all("td")[1:4]]
                return sell, buy, cbr
        except Exception as e:
            log.warning("Energo attempt %s/%s: %s", att, MAX_RETRIES, e)
            if att < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)
    return None, None, None

# ──────────────── TELEGRAM BOT ────────────────
def _auth_ok(uid: int) -> bool:
    return uid in AUTHORIZED_USERS

async def cmd_auth(u, ctx):
    if len(ctx.args) != 1:
        await u.message.reply_text("Используйте: /auth <пароль>")
    elif ctx.args[0] == PASSWORD:
        AUTHORIZED_USERS.add(u.effective_user.id)
        await u.message.reply_text("✅ Доступ разрешён.")
    else:
        await u.message.reply_text("❌ Неверный пароль.")

async def cmd_start(u, _):  await u.message.reply_text("Бот активен. /help")
async def cmd_help(u, _):   await u.message.reply_text("/start /auth /check /change /show_offsets /help")

async def cmd_check(u, ctx):
    if not _auth_ok(u.effective_user.id):
        await u.message.reply_text("Нет доступа. /auth <пароль>")
        return
    await send_rates_message(ctx.application)
    await u.message.reply_text("Отчёт выслан.")

async def cmd_change(u, ctx):
    if not _auth_ok(u.effective_user.id):
        await u.message.reply_text("Нет доступа."); return
    try:
        global KENIG_ASK_OFFSET, KENIG_BID_OFFSET
        KENIG_ASK_OFFSET, KENIG_BID_OFFSET = map(float, ctx.args[:2])
        await u.message.reply_text(f"Новые оффсеты: ask +{KENIG_ASK_OFFSET}, bid {KENIG_BID_OFFSET}")
    except Exception:
        await u.message.reply_text("Пример: /change 1.0 -0.5")

async def cmd_show(u, _):
    if not _auth_ok(u.effective_user.id):
        await u.message.reply_text("Нет доступа."); return
    await u.message.reply_text(f"Ask +{KENIG_ASK_OFFSET}  Bid {KENIG_BID_OFFSET}")

# ───────────────  SUMMARY MESSAGE  ────────────────
async def send_rates_message(app):
    bc_sell = await fetch_bestchange_sell()
    bc_buy  = await fetch_bestchange_buy()
    en_sell, en_buy, en_cbr = await fetch_energo()
    gr_ask, gr_bid          = await fetch_grinex_rate()

    ts = datetime.now(KAL_TZ).strftime("%d.%m.%Y %H:%M:%S")
    lines = [ts, "",
             "KenigSwap USDT/RUB"]
    if gr_ask and gr_bid:
        lines.append(f"Продажа: {gr_ask + KENIG_ASK_OFFSET:,.2f} ₽ | "
                     f"Покупка: {gr_bid + KENIG_BID_OFFSET:,.2f} ₽")
    else:
        lines.append("— нет данных —")
    lines += ["", "BestChange USDT/RUB",
              f"Продажа: {bc_sell:,.2f} ₽ | Покупка: {bc_buy:,.2f} ₽"
              if bc_sell and bc_buy else "— нет данных —", "",
              "EnergoTransBank USD/RUB",
              f"Продажа: {en_sell:,.2f} ₽ | Покупка: {en_buy:,.2f} ₽ | ЦБ: {en_cbr:,.2f} ₽"
              if en_sell and en_buy and en_cbr else "— нет данных —"]

    try:
        await app.bot.send_message(
            chat_id=CHAT_ID,
            text="<pre>" + html.escape("\n".join(lines)) + "</pre>",
            parse_mode="HTML",
            disable_web_page_preview=True)
    except Exception as e:
        log.error("Send error: %s", e)

    # Обновляем live-источники в Supabase
    if gr_ask and gr_bid:   await upsert_rate("kenig", gr_ask + KENIG_ASK_OFFSET, gr_bid + KENIG_BID_OFFSET)
    else:                   await mark_inactive("kenig")

    if bc_sell and bc_buy:  await upsert_rate("bestchange", bc_sell, bc_buy)
    else:                   await mark_inactive("bestchange")

    if en_sell and en_buy:  await upsert_rate("energo", en_sell, en_buy)
    else:                   await mark_inactive("energo")

# ────────────────────────── MAIN ────────────────────
async def main() -> None:
    install_chromium()

    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start",        cmd_start))
    app.add_handler(CommandHandler("help",         cmd_help))
    app.add_handler(CommandHandler("auth",         cmd_auth))
    app.add_handler(CommandHandler("check",        cmd_check))
    app.add_handler(CommandHandler("change",       cmd_change))
    app.add_handler(CommandHandler("show_offsets", cmd_show))

    sched = AsyncIOScheduler(event_loop=asyncio.get_running_loop(), timezone=KAL_TZ)
    sched.add_job(refresh_full_matrix,  trigger="interval", minutes=1)
    sched.add_job(update_limits_dynamic, trigger="interval", minutes=1, seconds=5)
    sched.add_job(send_rates_message,    trigger="interval", minutes=2, seconds=30, args=[app])
    sched.start()

    log.info("Bot started.")
    await app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    asyncio.run(main())
