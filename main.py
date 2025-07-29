"""
KenigSwap rates bot — исправленная версия.

• Playwright ищет браузер в /opt/render/.cache/ms-playwright  
• refresh_full_matrix → upsert (больше нет дубликатов)  
• update_limits_dynamic запускается на 30 с позже, чтобы не конфликтовать  
• Telegram-бот запущен с concurrent_updates(False) и перехватывает Conflict  
"""

# ───────────────────── IMPORTS ──────────────────────
from __future__ import annotations

import asyncio
import contextlib
import html
import logging
import os
import subprocess
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from playwright.async_api import async_playwright
from supabase import Client, create_client
from telegram.error import Conflict
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
)

load_dotenv()

# ───────── CONSTANTS / CONFIG ─────────
CRYPTOS = [
    "BTC", "ETH", "SOL", "XRP", "LTC", "ADA", "DOGE", "TRX", "DOT", "LINK",
    "AVAX", "MATIC", "BCH", "ATOM", "NEAR", "ETC", "FIL", "UNI", "ARB", "APT",
]
ASSETS = CRYPTOS + ["USDT", "RUB"]
BYBIT_SYMBOLS = [f"{c}USDT" for c in CRYPTOS]

_SOURCE_PAIR = {
    "kenig": ("USDT", "RUB"),
    "bestchange": ("USDT", "RUB"),
    "energo": ("USD", "RUB"),
}

MIN_EQ_USDT = 1_000
MAX_EQ_USDT = 1_000_000
RESERVE_EQ_USDT = 1_000_000

DERIVED_SELL_FEE = 0.01
DERIVED_BUY_FEE = -0.01

MAX_RETRIES = 3
RETRY_DELAY = 5
TIMEOUT_MS = 60_000

TOKEN = os.getenv("TG_BOT_TOKEN")
PASSWORD = os.getenv("TG_BOT_PASS")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
CHAT_ID = "@KaliningradCryptoRatesKenigSwap"
KALININGRAD_TZ = timezone(timedelta(hours=2))

AUTHORIZED_USERS: set[int] = set()

# ───────── LOGGING ─────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ───────── SUPABASE ─────────
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ───────── ENV FIX FOR PLAYWRIGHT ─────────
os.environ.setdefault(
    "PLAYWRIGHT_BROWSERS_PATH",
    "/opt/render/.cache/ms-playwright"
)

# ───────── BYBIT PRICES ─────────
async def _fetch_bybit_basics() -> dict[str, float]:
    prices: dict[str, float] = {"USDT": 1.0}
    async with httpx.AsyncClient(timeout=10) as cli:
        r = await cli.get(
            "https://api.bybit.com/v5/market/tickers",
            params={"category": "spot"},
        )
        r.raise_for_status()
        for item in r.json()["result"]["list"]:
            sym = item["symbol"]
            if sym in BYBIT_SYMBOLS:
                prices[sym[:-4]] = float(item["lastPrice"])
    logger.info("Bybit prices fetched: %s / 20", len(prices) - 1)
    return prices


async def _get_usdt_rub() -> float:
    return (await fetch_bestchange_sell()) or 80.0

# ───────── FULL MATRIX BUILDERS ─────────
async def _build_full_rows() -> list[dict]:
    base = await _fetch_bybit_basics()
    base["RUB"] = 1 / await _get_usdt_rub()
    now = datetime.utcnow().isoformat()

    rows: list[dict] = []
    for b in ASSETS:
        for q in ASSETS:
            if b == q or b not in base or q not in base:
                continue
            price = (
                1 / base[q]
                if b == "USDT" else
                base[b] if q == "USDT" else
                base[b] / base[q]
            )
            last = round(price, 8)
            rows.append(
                {
                    "source": "derived",
                    "base": b,
                    "quote": q,
                    "last_price": last,
                    "sell": round(last * (1 + DERIVED_SELL_FEE), 8),
                    "buy": round(last * (1 + DERIVED_BUY_FEE), 8),
                    "updated_at": now,
                }
            )
    return rows


async def refresh_full_matrix() -> None:
    rows = await _build_full_rows()
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None,
        lambda: (
            sb.table("kenig_rates")
            .upsert(rows, on_conflict="source,base,quote")
            .execute()
        ),
    )
    logger.info("Full matrix refreshed: %s rows", len(rows))

# ───────── LIMITS UPDATE ─────────
async def update_limits_dynamic() -> None:
    prices = await _fetch_bybit_basics()
    prices["RUB"] = 1 / await _get_usdt_rub()
    prices["USDT"] = 1.0

    rows = sb.table("kenig_rates").select("source,base,quote").execute().data or []
    updates = []
    for row in rows:
        pb, pq = prices.get(row["base"]), prices.get(row["quote"])
        if not pb or not pq:
            continue
        updates.append(
            {
                **{k: row[k] for k in ("source", "base", "quote")},
                "min_amount": round(MIN_EQ_USDT / pb, 8),
                "max_amount": round(MAX_EQ_USDT / pb, 8),
                "reserve": round(RESERVE_EQ_USDT / pq, 8),
                "updated_at": datetime.utcnow().isoformat(),
            }
        )
    if updates:
        sb.table("kenig_rates").upsert(
            updates, on_conflict="source,base,quote"
        ).execute()
        logger.info("✔ limits updated for %s pairs", len(updates))

# ───────── SCRAPERS ─────────
GRINEX_URL = "https://grinex.io/trading/usdta7a5?lang=en"

async def fetch_grinex_rate() -> Tuple[Optional[float], Optional[float]]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context()
                page = await context.new_page()
                await page.goto(GRINEX_URL, timeout=TIMEOUT_MS)
                ask_sel = "tbody.usdta7a5_ask.asks tr[data-price]"
                bid_sel = "tbody.usdta7a5_bid.bids tr[data-price]"
                await page.wait_for_selector(ask_sel)
                await page.wait_for_selector(bid_sel)
                ask = float(
                    await page.locator(ask_sel).first.get_attribute("data-price")
                )
                bid = float(
                    await page.locator(bid_sel).first.get_attribute("data-price")
                )
                await browser.close()
                return ask, bid
        except Exception as e:
            logger.warning(
                "Grinex attempt %s/%s failed: %s", attempt, MAX_RETRIES, e
            )
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)
    return None, None


async def fetch_bestchange_sell() -> Optional[float]:
    url = (
        "https://www.bestchange.com/cash-ruble-to-tether-trc20-in-klng.html"
    )
    return await _bestchange_generic(url, "div.fs")


async def fetch_bestchange_buy() -> Optional[float]:
    url = (
        "https://www.bestchange.com/tether-trc20-to-cash-ruble-in-klng.html"
    )
    return await _bestchange_generic(
        url, "table#content_table tr[onclick] td.bi"
    )


async def _bestchange_generic(url: str, selector: str) -> Optional[float]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with httpx.AsyncClient(timeout=15) as cli:
                res = await cli.get(url)
                res.raise_for_status()
                soup = BeautifulSoup(res.text, "html.parser")
                node = soup.select_one(selector)
                if node:
                    raw = "".join(
                        ch for ch in node.text if ch.isdigit() or ch in ",."
                    )
                    return float(raw.replace(",", "."))
        except Exception as e:
            logger.warning(
                "BestChange attempt %s/%s: %s", attempt, MAX_RETRIES, e
            )
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)
    return None


async def fetch_energo() -> Tuple[Optional[float], Optional[float], Optional[float]]:
    url = "https://ru.myfin.by/bank/energotransbank/currency/kaliningrad"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with httpx.AsyncClient(timeout=15) as cli:
                res = await cli.get(url)
                res.raise_for_status()
                soup = BeautifulSoup(res.text, "html.parser")
                row = soup.select_one(
                    "table.table-best.white_bg tr:has(td.title)"
                )
                if row:
                    buy, sell, cbr = [
                        float(td.text.replace(",", "."))
                        for td in row.find_all("td")[1:4]
                    ]
                    return sell, buy, cbr
        except Exception as e:
            logger.warning("Energo attempt %s/%s: %s", attempt, MAX_RETRIES, e)
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)
    return None, None, None

# ───────── TELEGRAM HANDLERS ─────────
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


async def start_cmd(update, context):
    await update.message.reply_text(
        "Бот активен. Используйте /auth <пароль>."
    )


async def send_rates_message(app):
    bc_sell, bc_buy = await fetch_bestchange_sell(), await fetch_bestchange_buy()
    en_sell, en_buy, en_cbr = await fetch_energo()
    gr_ask, gr_bid = await fetch_grinex_rate()

    ts = datetime.now(KALININGRAD_TZ).strftime("%d.%m.%Y %H:%M:%S")
    lines = [ts, "", "KenigSwap rate USDT/RUB"]
    if gr_ask and gr_bid:
        lines.append(
            f"Продажа: {gr_ask + 0.8:.2f} ₽, "
            f"Покупка: {gr_bid - 0.9:.2f} ₽"
        )
    else:
        lines.append("Нет данных с Grinex.")
    lines += ["", "BestChange rate USDT/RUB"]
    if bc_sell and bc_buy:
        lines.append(f"Продажа: {bc_sell:.2f} ₽, Покупка: {bc_buy:.2f} ₽")
    else:
        lines.append("Нет данных с BestChange.")
    lines += ["", "EnergoTransBank rate USD/RUB"]
    if en_sell and en_buy and en_cbr:
        lines.append(
            f"Продажа: {en_sell:.2f} ₽, "
            f"Покупка: {en_buy:.2f} ₽, ЦБ: {en_cbr:.2f} ₽"
        )
    else:
        lines.append("Нет данных с EnergoTransBank.")

    msg = "<pre>" + html.escape("\n".join(lines)) + "</pre>"
    await app.bot.send_message(
        chat_id=CHAT_ID,
        text=msg,
        parse_mode="HTML",
        disable_web_page_preview=True,
    )

# ───────── MAIN ─────────
def main() -> None:
    app = (
        ApplicationBuilder()
        .token(TOKEN)
        .concurrent_updates(False)  # один поток polling
        .build()
    )

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("auth", auth))

    sched = AsyncIOScheduler()
    sched.add_job(
        send_rates_message,
        "interval",
        minutes=2,
        seconds=30,
        args=[app],
        timezone=KALININGRAD_TZ,
    )
    sched.add_job(
        refresh_full_matrix,
        "interval",
        minutes=1,
        timezone=KALININGRAD_TZ,
    )
    sched.add_job(
        update_limits_dynamic,
        "interval",
        minutes=1,
        seconds=30,  # сдвиг 30 с
        timezone=KALININGRAD_TZ,
    )
    sched.start()

    logger.info("Bot started.")
    try:
        app.run_polling(drop_pending_updates=True)
    except Conflict:
        logger.error("Другой экземпляр бота уже запущен – завершаюсь.")


if __name__ == "__main__":
    # Для локального запуска (Render ставит Chromium на этапе build)
    if os.environ.get("LOCAL_RUN") == "1":
        subprocess.run(["playwright", "install", "chromium"], check=False)
    main()
