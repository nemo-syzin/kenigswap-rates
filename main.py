# ───────────────────── IMPORTS ──────────────────────
import asyncio, html, logging, os, subprocess, contextlib
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from supabase import Client, create_client
from telegram.ext import ApplicationBuilder, CommandHandler
from dotenv import load_dotenv

load_dotenv()

# ───────── FULL-MATRIX HELPERS ──────────────────────
CRYPTOS = [
    "BTC","ETH","SOL","XRP","LTC","ADA","DOGE","TRX","DOT","LINK",
    "AVAX","MATIC","BCH","ATOM","NEAR","ETC","FIL","UNI","ARB","APT"
]
ASSETS          = CRYPTOS + ["USDT", "RUB"]
BYBIT_SYMBOLS   = [f"{c}USDT" for c in CRYPTOS]

_SOURCE_PAIR = {
    "kenig":      ("USDT", "RUB"),
    "bestchange": ("USDT", "RUB"),
    "energo":     ("USD",  "RUB"),
}

# ───────────────────── CONFIG ───────────────────────
TOKEN          = os.getenv("TG_BOT_TOKEN")
PASSWORD       = os.getenv("TG_BOT_PASS")
SUPABASE_URL   = os.getenv("SUPABASE_URL")
SUPABASE_KEY   = os.getenv("SUPABASE_KEY")
CHAT_ID        = "@KaliningradCryptoRatesKenigSwap"
KALININGRAD_TZ = timezone(timedelta(hours=2))

KENIG_ASK_OFFSET = 0.8
KENIG_BID_OFFSET = -0.9
DERIVED_SELL_FEE =  0.01   # +1 %
DERIVED_BUY_FEE  = -0.01   # −1 %

MIN_EQ_USDT      = 1_000          # min = экв. 1000 USDT
MAX_EQ_USDT      = 1_000_000      # max = экв. 1 000 000 USDT
RESERVE_EQ_USDT  = 1_000_000

MAX_RETRIES = 3
RETRY_DELAY = 5
AUTHORIZED_USERS: set[int] = set()

# ───────────────────── LOGGER ───────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ────────────────── SUPABASE ────────────────────────
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ────────────────── PLAYWRIGHT SETUP ────────────────
def install_chromium_for_playwright() -> None:
    """Устанавливаем Chromium при первом запуске (± 150 МБ)."""
    try:
        subprocess.run(
            ["playwright", "install", "--with-deps", "chromium"],
            check=True,
        )
        logger.info("Playwright browser installed")
    except Exception as exc:
        logger.warning("Playwright install error: %s", exc)

# ───────────────────── SUPABASE HELPERS ─────────────
async def upsert_rate(source: str, sell: float, buy: float) -> None:
    """
    Записывает «живой» курс (kenig / bestchange / energo).
    """
    base, quote = _SOURCE_PAIR[source]
    record = {
        "source":           source,                # уникальная подпись в системе
        "exchange_source":  source,                # человеко-читаемо
        "base":             base,
        "quote":            quote,
        "sell":             round(sell, 2),
        "buy":              round(buy, 2),
        "last_price":       round((sell + buy) / 2, 4),
        "min_amount":       None,                  # проставит update_limits_dynamic
        "max_amount":       None,
        "reserve":          None,
        "conditions":       "KYC",
        "working_hours":    "24/7",
        "operational_mode": "manual",
        "is_active":        True,                  # получили курс → активно
        "updated_at":       datetime.utcnow().isoformat(),
    }

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None,
        lambda: (
            sb.table("kenig_rates")
              .upsert(record, on_conflict="source,base,quote")
              .execute()
        ),
    )
    logger.info("Supabase upsert OK: %s", source)

async def mark_pair_inactive(source: str) -> None:
    """
    Если курс не удалось получить — выставляем is_active = False.
    """
    base, quote = _SOURCE_PAIR[source]
    sb.table("kenig_rates") \
      .update({"is_active": False, "updated_at": datetime.utcnow().isoformat()}) \
      .eq("source", source).eq("base", base).eq("quote", quote) \
      .execute()

# ─────────────── BYBIT (основа для derived) ─────────
async def _fetch_bybit_basics() -> dict[str, float]:
    prices = {"USDT": 1.0}
    url     = "https://api.bybit.com/v5/market/tickers"
    params  = {"category": "spot"}
    proxy   = os.getenv("BYBIT_PROXY") or None

    async with httpx.AsyncClient(
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept":     "application/json",
            "Referer":    "https://www.bybit.com/",
        },
        proxies=proxy,
        timeout=10,
        follow_redirects=True,
        trust_env=False,
    ) as cli:
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

# ─────────────── FULL-MATRIX (420 derived) ──────────
async def _build_full_rows() -> list[dict]:
    base_rates           = await _fetch_bybit_basics()     # COIN → USDT
    base_rates["RUB"]    = 1 / await _get_usdt_rub()       #  RUB → USDT
    now                  = datetime.utcnow().isoformat()
    rows: list[dict]     = []

    for b in ASSETS:
        for q in ASSETS:
            if b == q or b not in base_rates or q not in base_rates:
                continue
            if b == "USDT":
                price = 1 / base_rates[q]                 # USDT → x
            elif q == "USDT":
                price = base_rates[b]                     # x → USDT
            else:
                price = base_rates[b] / base_rates[q]     # x ↔ y

            last = round(price, 8)
            rows.append(
                {
                    "source":           "derived",
                    "exchange_source":  "bybit",
                    "base":             b,
                    "quote":            q,
                    "last_price":       last,
                    "sell":             round(last * (1 + DERIVED_SELL_FEE), 8),
                    "buy":              round(last * (1 + DERIVED_BUY_FEE),  8),
                    "min_amount":       None,          # позже
                    "max_amount":       None,          # позже
                    "reserve":          None,          # позже
                    "conditions":       "KYC",
                    "working_hours":    "24/7",
                    "operational_mode": "manual",
                    "is_active":        True,
                    "updated_at":       now,
                }
            )
    return rows

async def refresh_full_matrix() -> None:
    """
    Каждую минуту: удаляем старые derived и вставляем свежие 420 пар.
    """
    rows = await _build_full_rows()
    loop = asyncio.get_running_loop()

    await loop.run_in_executor(
        None,
        lambda: (
            sb.table("kenig_rates")
              .delete().eq("source", "derived").execute()
        ),
    )
    await loop.run_in_executor(
        None,
        lambda: sb.table("kenig_rates").insert(rows).execute(),
    )
    logger.info("Full matrix refreshed: %s rows", len(rows))

# ──────────── LIMITS / RESERVE / is_active ──────────
async def update_limits_dynamic() -> None:
    """
    Вычисляем min_amount / max_amount / reserve
    для **всех** уже существующих записей.
    """
    prices = await _fetch_bybit_basics()
    prices["RUB"]  = 1 / await _get_usdt_rub()
    prices["USDT"] = 1.0

    rows = sb.table("kenig_rates").select("source,base,quote").execute().data
    if not rows:
        return

    now      = datetime.utcnow().isoformat()
    updates  = []

    for row in rows:
        base, quote = row["base"], row["quote"]
        pb, pq      = prices.get(base), prices.get(quote)
        if not pb or not pq:
            # Нет нужных цен — делаем направление неактивным
            updates.append({
                "source": row["source"],
                "base":   base,
                "quote":  quote,
                "is_active": False,
                "updated_at": now,
            })
            continue

        updates.append({
            "source":           row["source"],
            "base":             base,
            "quote":            quote,
            "min_amount":       round(MIN_EQ_USDT / pb, 8),
            "max_amount":       round(MAX_EQ_USDT / pb, 8),
            "reserve":          round(RESERVE_EQ_USDT / pq, 8),
            "operational_mode": "manual",
            "conditions":       "KYC",
            "working_hours":    "24/7",
            "is_active":        True,
            "updated_at":       now,
        })

    sb.table("kenig_rates") \
      .upsert(updates, on_conflict="source,base,quote") \
      .execute()

    logger.info("✔ limits updated for %s pairs", len(updates))

# ────────────────── SCRAPERS (Grinex / etc.) ────────
GRINEX_URL = "https://grinex.io/trading/usdta7a5?lang=en"
TIMEOUT_MS = 60_000

async def fetch_grinex_rate() -> Tuple[Optional[float], Optional[float]]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=["--disable-blink-features=AutomationControlled"],
                )
                context = await browser.new_context(
                    user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                "AppleWebKit/537.36 (KHTML, like Gecko) "
                                "Chrome/123.0.0.0 Safari/537.36")
                )
                page = await context.new_page()
                await page.goto(GRINEX_URL, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
                with contextlib.suppress(Exception):
                    await page.locator("button:text('Accept')").click(timeout=3_000)

                ask_sel = "tbody.usdta7a5_ask.asks tr[data-price]"
                bid_sel = "tbody.usdta7a5_bid.bids tr[data-price]"
                await page.wait_for_selector(ask_sel, timeout=TIMEOUT_MS)
                await page.wait_for_selector(bid_sel, timeout=TIMEOUT_MS)

                ask = float(await page.locator(ask_sel).first.get_attribute("data-price"))
                bid = float(await page.locator(bid_sel).first.get_attribute("data-price"))
                await browser.close()
                return ask, bid

        except Exception as e:
            logger.warning("Grinex attempt %s/%s failed: %s", attempt, MAX_RETRIES, e)
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)
    return None, None

async def fetch_bestchange_sell() -> Optional[float]:
    url = "https://www.bestchange.com/cash-ruble-to-tether-trc20-in-klng.html"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}, timeout=15) as cli:
                res = await cli.get(url)
                if res.status_code != 200:
                    raise RuntimeError(f"HTTP {res.status_code}")
                soup = BeautifulSoup(res.text, "html.parser")
                div  = soup.find("div", class_="fs")
                if div:
                    raw = "".join(ch for ch in div.text if ch.isdigit() or ch in ",.")
                    return float(raw.replace(",", "."))
        except Exception as e:
            logger.warning("BestChange sell attempt %s/%s: %s", attempt, MAX_RETRIES, e)
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)
    return None

async def fetch_bestchange_buy() -> Optional[float]:
    url = "https://www.bestchange.com/tether-trc20-to-cash-ruble-in-klng.html"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}, timeout=15) as cli:
                res = await cli.get(url)
                if res.status_code != 200:
                    raise RuntimeError(f"HTTP {res.status_code}")
                soup  = BeautifulSoup(res.text, "html.parser")
                table = soup.find("table", id="content_table")
                row   = table.find("tr", onclick=True) if table else None
                price_td = next(
                    (td for td in row.find_all("td", class_="bi") if "RUB Cash" in td.text),
                    None,
                ) if row else None
                if price_td:
                    raw = "".join(ch for ch in price_td.text if ch.isdigit() or ch in ",.")
                    return float(raw.replace(",", "."))
        except Exception as e:
            logger.warning("BestChange buy attempt %s/%s: %s", attempt, MAX_RETRIES, e)
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)
    return None

async def fetch_energo() -> Tuple[Optional[float], Optional[float], Optional[float]]:
    url = "https://ru.myfin.by/bank/energotransbank/currency/kaliningrad"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}, timeout=15) as cli:
                res = await cli.get(url)
                if res.status_code != 200:
                    raise RuntimeError(f"HTTP {res.status_code}")
                soup = BeautifulSoup(res.text, "html.parser")
                row  = soup.select_one("table.table-best.white_bg tr:has(td.title)")
                buy, sell, cbr = [float(td.text.replace(",", ".")) for td in row.find_all("td")[1:4]]
                return sell, buy, cbr
        except Exception as e:
            logger.warning("Energo attempt %s/%s: %s", attempt, MAX_RETRIES, e)
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)
    return None, None, None

# ─────────────── TELEGRAM HANDLERS ───────────────────
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

# ──────────────── SEND SUMMARY MESSAGE ──────────────
async def send_rates_message(app):
    bc_sell  = await fetch_bestchange_sell()
    bc_buy   = await fetch_bestchange_buy()
    en_sell, en_buy, en_cbr = await fetch_energo()
    gr_ask, gr_bid = await fetch_grinex_rate()

    ts = datetime.now(KALININGRAD_TZ).strftime("%d.%m.%Y %H:%M:%S")
    lines = [ts, ""]

    # KenigSwap (Grinex)
    lines += ["KenigSwap rate USDT/RUB"]
    if gr_ask and gr_bid:
        lines.append(f"Продажа: {gr_ask + KENIG_ASK_OFFSET:.2f} ₽, "
                     f"Покупка: {gr_bid + KENIG_BID_OFFSET:.2f} ₽")
    else:
        lines.append("Нет данных с Grinex.")
    lines.append("")

    # BestChange
    lines += ["BestChange rate USDT/RUB"]
    if bc_sell and bc_buy:
        lines.append(f"Продажа: {bc_sell:.2f} ₽, Покупка: {bc_buy:.2f} ₽")
    else:
        lines.append("Нет данных с BestChange.")
    lines.append("")

    # Energo
    lines += ["EnergoTransBank rate USD/RUB"]
    if en_sell and en_buy and en_cbr:
        lines.append(f"Продажа: {en_sell:.2f} ₽, "
                     f"Покупка: {en_buy:.2f} ₽, ЦБ: {en_cbr:.2f} ₽")
    else:
        lines.append("Нет данных с EnergoTransBank.")

    msg = "<pre>" + html.escape("\n".join(lines)) + "</pre>"

    # Telegram
    try:
        await app.bot.send_message(
            chat_id=CHAT_ID, text=msg,
            parse_mode="HTML", disable_web_page_preview=True,
        )
    except Exception as e:
        logger.error("Send error: %s", e)

    # Supabase (live sources)
    if gr_ask and gr_bid:
        await upsert_rate(
            "kenig", gr_ask + KENIG_ASK_OFFSET, gr_bid + KENIG_BID_OFFSET
        )
    else:
        await mark_pair_inactive("kenig")

    if bc_sell and bc_buy:
        await upsert_rate("bestchange", bc_sell, bc_buy)
    else:
        await mark_pair_inactive("bestchange")

    if en_sell and en_buy:
        await upsert_rate("energo", en_sell, en_buy)
    else:
        await mark_pair_inactive("energo")

# ───────────────────────── MAIN ─────────────────────
def main() -> None:
    install_chromium_for_playwright()          # первый запуск ≈ 30-40 с

    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start",         start))
    app.add_handler(CommandHandler("help",          help_command))
    app.add_handler(CommandHandler("auth",          auth))
    app.add_handler(CommandHandler("check",         check))
    app.add_handler(CommandHandler("change",        change_offsets))
    app.add_handler(CommandHandler("show_offsets",  show_offsets))

    scheduler = AsyncIOScheduler()

    # 00 сек — полная матрица
    scheduler.add_job(
        refresh_full_matrix,
        trigger="interval", minutes=1, seconds=0,
        timezone=KALININGRAD_TZ,
    )
    # 05 сек — лимиты / резерв
    scheduler.add_job(
        update_limits_dynamic,
        trigger="interval", minutes=1, seconds=5,
        timezone=KALININGRAD_TZ,
    )
    # каждые 2 мин 30 сек — витрина
    scheduler.add_job(
        send_rates_message,
        trigger="interval", minutes=2, seconds=30,
        timezone=KALININGRAD_TZ,
        args=[app],
    )

    scheduler.start()
    logger.info("Bot started.")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
