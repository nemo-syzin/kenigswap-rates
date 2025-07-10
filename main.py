
# ───────────────────── IMPORTS ──────────────────────
import asyncio, html, logging, os, subprocess, contextlib
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, Dict, List

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from supabase import Client, create_client
from telegram.ext import ApplicationBuilder, CommandHandler
from dotenv import load_dotenv

load_dotenv()

# ──────────────── CONSTANTS ─────────────────
CRYPTOS = [
    "BTC","ETH","SOL","XRP","LTC","ADA","DOGE","TRX","DOT","LINK",
    "AVAX","MATIC","BCH","ATOM","NEAR","ETC","FIL","UNI","ARB","APT",
]
ASSETS: List[str] = CRYPTOS + ["USDT", "RUB"]
BYBIT_SYMBOLS = [f"{c}USDT" for c in CRYPTOS]

# маппинг «эталонных» источников к парам
_SOURCE_PAIR: Dict[str, Tuple[str, str]] = {
    "kenig":      ("USDT", "RUB"),
    "bestchange": ("USDT", "RUB"),
    "energo":     ("USD",  "RUB"),
}

# ──────────────── FEES / OFFSETS ────────────
KENIG_ASK_OFFSET = 0.7   # к Grinex ask
KENIG_BID_OFFSET = -1.0  # к Grinex bid
DERIVED_SELL_FEE = 0.01  # +1 %
DERIVED_BUY_FEE  = -0.01 # −1 %

# ──────────────── CONFIG / ENV ──────────────
TOKEN          = os.getenv("TG_BOT_TOKEN")
PASSWORD       = os.getenv("TG_BOT_PASS")
SUPABASE_URL   = os.getenv("SUPABASE_URL")
SUPABASE_KEY   = os.getenv("SUPABASE_KEY")
CHAT_ID        = "@KaliningradCryptoKenigSwap"
KALININGRAD_TZ = timezone(timedelta(hours=2))
MAX_RETRIES    = 3
RETRY_DELAY    = 5

if not all([TOKEN, PASSWORD, SUPABASE_URL, SUPABASE_KEY]):
    raise SystemExit("❌ Set all required ENV vars (TG_BOT_TOKEN, TG_BOT_PASS, SUPABASE_URL, SUPABASE_KEY)")

# ───────────────── LOGGER ───────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)-8s | %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger("kenigbot")

# ──────────────── SUPABASE  ────────────────
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ========== MARKET DATA HELPERS =============
async def _fetch_bybit_basics() -> Dict[str, float]:
    prices = {"USDT": 1.0}
    url = "https://api.bybit.com/v5/market/tickers"
    params = {"category": "spot"}
    proxy = os.getenv("BYBIT_PROXY")

    async with httpx.AsyncClient(headers={
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
    }, proxies=proxy, trust_env=False, timeout=10) as cli:
        r = await cli.get(url, params=params)
        if r.status_code != 200:
            logger.warning("Bybit ticker HTTP %s", r.status_code)
            return prices
        for item in r.json().get("result", {}).get("list", []):
            sym = item["symbol"]
            if sym in BYBIT_SYMBOLS:
                prices[sym[:-4]] = float(item["lastPrice"])
    logger.info("Bybit prices fetched: %s / 20", len(prices) - 1)
    return prices

async def _get_usdt_rub() -> float:
    return (await fetch_bestchange_sell()) or 80.0

# ========== FULL MATRIX BUILDERS ============
async def _build_full_rows() -> List[dict]:
    base = await _fetch_bybit_basics()    # COIN → USDT
    base["RUB"] = 1 / await _get_usdt_rub()  # RUB → USDT

    now = datetime.utcnow().isoformat()
    rows: List[dict] = []

    for b in ASSETS:
        for q in ASSETS:
            if b == q or b not in base or q not in base:
                continue
            price = 1 / base[q] if b == "USDT" else base[b] if q == "USDT" else base[b] / base[q]
            last = round(price, 8)
            rows.append({
                "source": "derived",
                "base": b,
                "quote": q,
                "last_price": last,
                "sell": round(last * (1 + DERIVED_SELL_FEE), 8),
                "buy":  round(last * (1 + DERIVED_BUY_FEE),  8),
                "updated_at": now,
            })
    return rows

async def refresh_full_matrix() -> None:
    """Перестраиваем derived-матрицу каждую минуту."""
    rows = await _build_full_rows()
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, lambda: (
        sb.table("kenig_rates").delete().eq("source", "derived").execute()
    ))
    await loop.run_in_executor(None, lambda: (
        sb.table("kenig_rates").insert(rows).execute()
    ))
    logger.info("Full matrix refreshed: %s rows", len(rows))

# ========== RATE UPSERT (KENIG / ETC) ========
async def upsert_rate(source: str, sell: float, buy: float) -> None:
    base, quote = _SOURCE_PAIR[source]
    rec = {
        "source": source,
        "base": base,
        "quote": quote,
        "sell": round(sell, 2),
        "buy":  round(buy, 2),
        "last_price": round((sell + buy) / 2, 4),
        "updated_at": datetime.utcnow().isoformat(),
    }
    loop = asyncio.get_running_loop()
    try:
        # 1) Обновляем/добавляем в market_rates (ровно 1 строка на источник)
        await loop.run_in_executor(None, lambda: (
            sb.table("market_rates").upsert(rec, on_conflict="source").execute()
        ))
        # 2) Обновляем/добавляем в kenig_rates (строка для kenig, для других not needed)
        await loop.run_in_executor(None, lambda: (
            sb.table("kenig_rates").upsert(rec, on_conflict="source,base,quote").execute()
        ))
        logger.info("Supabase upsert OK: %s", source)
    except Exception as e:
        logger.warning("Supabase upsert failed (%s): %s", source, e)

# ========== SCRAPERS (Grinex / BestChange / Energo) =========
GRINEX_URL = "https://grinex.io/trading/usdta7a5?lang=en"
TIMEOUT_MS = 60_000

async def fetch_grinex_rate() -> Tuple[Optional[float], Optional[float]]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
                context = await browser.new_context(user_agent="Mozilla/5.0")
                page = await context.new_page()
                await page.goto(GRINEX_URL, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
                with contextlib.suppress(Exception):
                    await page.locator("button:text('Accept')").click(timeout=3_000)
                await page.wait_for_selector("tbody.usdta7a5_ask.asks tr[data-price]", timeout=TIMEOUT_MS)
                await page.wait_for_selector("tbody.usdta7a5_bid.bids tr[data-price]", timeout=TIMEOUT_MS)
                ask = float(await page.locator("tbody.usdta7a5_ask.asks tr[data-price]").first.get_attribute("data-price"))
                bid = float(await page.locator("tbody.usdta7a5_bid.bids tr[data-price]").first.get_attribute("data-price"))
                await browser.close()
                return ask, bid
        except Exception as e:
            logger.warning("Grinex attempt %s/%s failed: %s", attempt, MAX_RETRIES, e)
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)
    return None, None

async def _scrape_bestchange(url: str, selector: str) -> Optional[float]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}, timeout=15, trust_env=False) as cli:
                res = await cli.get(url)
                if res.status_code != 200:
                    raise RuntimeError(f"HTTP {res.status_code}")
                soup = BeautifulSoup(res.text, "html.parser")
                el = soup.select_one(selector)
                if not el:
                    raise RuntimeError("price not found")
                raw = "".join(ch for ch in el.text if (ch.isdigit() or ch in ",."))
                return float(raw.replace(",", "."))
        except Exception as e:
            logger.warning("BestChange attempt %s/%s: %s", attempt, MAX_RETRIES, e)
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)
    return None

async def fetch_bestchange_sell() -> Optional[float]:
    url = "https://www.bestchange.com/cash-ruble-to-tether-trc20-in-klng.html"
    return await _scrape_bestchange(url, "div.fs")

async def fetch_bestchange_buy() -> Optional[float]:
    url = "https://www.bestchange.com/tether-trc20-to-cash-ruble-in-klng.html"
    return await _scrape_bestchange(url, "table#content_table tr[onclick] td.bi:nth-child(5)")

async def fetch_energo() -> Tuple[Optional[float], Optional[float], Optional[float]]:
    url = "https://ru.myfin.by/bank/energotransbank/currency/kaliningrad"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}, timeout=15, trust_env=False) as cli:
                res = await cli.get(url)
                if res.status_code != 200:
                    raise RuntimeError(f"HTTP {res.status_code}")
                soup = BeautifulSoup(res.text, "html.parser")
                row = soup.select_one("table.table-best.white_bg tr:has(td.title)")
                if not row:
                    raise RuntimeError("row not found")
                buy, sell, cbr = [float(td.text.replace(",", ".")) for td in row.find_all("td")[1:4]]
                return sell, buy, cbr
        except Exception as e:
            logger.warning("Energo attempt %s/%s: %s", attempt, MAX_RETRIES, e)
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)
    return None, None, None

# ────────────── TELEGRAM HANDLERS ───────────
AUTHORIZED_USERS: set[int] = set()

def is_authorized(uid: int) -> bool:
    return uid in AUTHORIZED_USERS

async def auth(update, context):
    if len(context.args) != 1:
        await update.message.reply_text("Введите пароль: /auth <пароль>")
        return
    if context.args[0] == PASSWORD:
        AUTHORIZED_USERS.add(update.effective_user.id)
        await update.message.reply_text("Доступ разрешён.")
    else:
        await update.message.reply_text("Неверный пароль.")

async def start(update, context):
    await update.message.reply_text("Бот активен. Используйте /auth <пароль>.")

async def help_command(update, context):
    await update.message.reply_text("/start /auth /check /change /show_offsets /help")

async def check(update, context):
    if not is_authorized(update.effective_user.id):
        await update.message.reply_text("Нет доступа. /auth <пароль>")
        return
    await send_rates_message(context.application)
    await update.message.reply_text("Курсы отправлены.")

async def change_offsets(update, context):
    if not is_authorized(update.effective_user.id):
        await update.message.reply_text("Нет доступа.")
        return
    try:
        global KENIG_ASK_OFFSET, KENIG_BID_OFFSET
        KENIG_ASK_OFFSET, KENIG_BID_OFFSET = map(float, context.args[:2])
        await update.message.reply_text(f"Ask +{KENIG_ASK_OFFSET}  Bid {KENIG_BID_OFFSET}")
    except Exception:
        await update.message.reply_text("Пример: /change 1.0 -0.5")

async def show_offsets(update, context):
    if not is_authorized(update.effective_user.id):
        await update.message.reply_text("Нет доступа.")
        return
    await update.message.reply_text(f"Ask +{KENIG_ASK_OFFSET}  Bid {KENIG_BID_OFFSET}")

# ────────────── MESSAGE SENDER ─────────────
async def send_rates_message(app):
    bc_sell, bc_buy   = await fetch_bestchange_sell(), await fetch_bestchange_buy()
    en_sell, en_buy, en_cbr = await fetch_energo()
    gr_ask, gr_bid   = await fetch_grinex_rate()

    ts = datetime.now(KALININGRAD_TZ).strftime("%d.%m.%Y %H:%M:%S")
    lines = [ts, ""]

    lines += ["KenigSwap rate USDT/RUB"]
    if gr_ask and gr_bid:
        lines.append(f"Продажа: {gr_ask + KENIG_ASK_OFFSET:.2f} ₽, Покупка: {gr_bid + KENIG_BID_OFFSET:.2f} ₽")
    else:
        lines.append("Нет данных с Grinex.")
    lines.append("")

    lines += ["BestChange rate USDT/RUB"]
    if bc_sell and bc_buy:
        lines.append(f"Продажа: {bc_sell:.2f} ₽, Покупка: {bc_buy:.2f} ₽")
    else:
        lines.append("Нет данных с BestChange.")
    lines.append("")

    lines += ["EnergoTransBank rate USD/RUB"]
    if en_sell and en_buy and en_cbr:
        lines.append(f"Продажа: {en_sell:.2f} ₽, Покупка: {en_buy:.2f} ₽, ЦБ: {en_cbr:.2f} ₽")
    else:
        lines.append("Нет данных с EnergoTransBank.")

    msg = "<pre>" + html.escape("\n".join(lines)) + "</pre>"

    try:
        await app.bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="HTML", disable_web_page_preview=True)
    except Exception as e:
        logger.error("Send error: %s", e)

    # Upsert эталонных курсов
    tasks = []
    if gr_ask and gr_bid:
        tasks.append(upsert_rate("kenig", gr_ask + KENIG_ASK_OFFSET, gr_bid + KENIG_BID_OFFSET))
    if bc_sell and bc_buy:
        tasks.append(upsert_rate("bestchange", bc_sell, bc_buy))
    if en_sell and en_buy:
        tasks.append(upsert_rate("energo", en_sell, en_buy))
    if tasks:
        await asyncio.gather(*tasks)

# ────────────── PLAYWRIGHT INSTALL (Render build) ─
def install_chromium_for_playwright() -> None:
    try:
        subprocess.run(["playwright", "install", "chromium"], check=True)
    except Exception as exc:
        logger.warning("Playwright install error: %s", exc)

# ────────────── MAIN ─────────────────────────
def main() -> None:
    install_chromium_for_playwright()

    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("auth", auth))
    app.add_handler(CommandHandler("check", check))
    app.add_handler(CommandHandler("change", change_offsets))
    app.add_handler(CommandHandler("show_offsets", show_offsets))

    scheduler = AsyncIOScheduler()

    scheduler.add_job(send_rates_message, "interval", minutes=2, seconds=30, timezone=KALININGRAD_TZ, args=[app])
    scheduler.add_job(refresh_full_matrix, "interval", minutes=1, timezone=KALININGRAD_TZ)

    scheduler.start()
    logger.info("Bot started.")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
