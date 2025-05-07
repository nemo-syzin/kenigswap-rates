
from __future__ import annotations

import asyncio
import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from typing import Tuple

import httpx
from bs4 import BeautifulSoup
from playwright.async_api import Error as PlaywrightError, async_playwright
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    CallbackContext,
    CommandHandler,
    ContextTypes,
)

# ──────────── конфиг ────────────
TOKEN = os.getenv("TOKEN", "7128150617:AAHEMrzGrSOZrLAMYDf8F8MwklSvPDN2IVk")
CHAT_ID = os.getenv("CHAT_ID", "@KaliningradCryptoKenigSwap")
PASSWORD = os.getenv("BOT_PASSWORD", "7128150617")

KALININGRAD_TZ = timezone(timedelta(hours=2))
KENIG_ASK_OFFSET = 1.0   # +к продаже
KENIG_BID_OFFSET = -0.5  # +к покупке

MAX_RETRIES = 3
RETRY_DELAY = 5  # секунд

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("kenigswap")

if os.getenv("RENDER_SECONDARY") == "yes":
    logger.info("Secondary instance → exiting to avoid 409 Conflict")
    sys.exit(0)

# ────────── Playwright ──────────
playwright_ctx = None
page_grinex = None


async def _install_chromium() -> None:
    subprocess.run(["playwright", "install", "chromium"], check=True)


async def init_playwright() -> None:
    """Стартуем браузер один раз и держим вкладку Grinex."""
    global playwright_ctx, page_grinex
    if playwright_ctx and page_grinex:
        return
    try:
        playwright_ctx = await async_playwright().start()
        browser = await playwright_ctx.chromium.launch(
            headless=True, args=["--no-sandbox"]
        )
    except PlaywrightError:
        logger.warning("Chromium not found — installing …")
        await asyncio.to_thread(_install_chromium)
        playwright_ctx = await async_playwright().start()
        browser = await playwright_ctx.chromium.launch(
            headless=True, args=["--no-sandbox"]
        )

    ctx = await browser.new_context(user_agent="Mozilla/5.0")
    page_grinex = await ctx.new_page()
    page_grinex.set_default_timeout(90_000)  # 90 сек.
    await page_grinex.goto("https://grinex.io/trading/usdta7a5")
    await page_grinex.wait_for_load_state("networkidle")
    logger.info("Playwright initialised")

# ────────── helpers ──────────
AUTHORIZED_USERS: set[int] = set()


def _auth_required(user_id: int) -> bool:
    return user_id in AUTHORIZED_USERS


async def _retry(fn, *args):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return await fn(*args)
        except Exception as exc:
            logger.warning("%s attempt %s/%s failed: %s",
                           fn.__name__, attempt, MAX_RETRIES, exc)
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)
    return None


async def _get_html(url: str, headers: dict | None = None) -> str:
    async with httpx.AsyncClient(headers=headers) as client:
        r = await client.get(url, timeout=30)
        r.raise_for_status()
        return r.text

# ────────── курсы ──────────
async def fetch_grinex_rate() -> Tuple[float | None, float | None]:
    await init_playwright()
    try:
        ask_row = await page_grinex.query_selector("tbody.asks tr")
        bid_row = await page_grinex.query_selector("tbody.bids tr")
        ask = float(await ask_row.get_attribute("data-price"))
        bid = float(await bid_row.get_attribute("data-price"))
        return ask, bid
    except Exception as exc:
        logger.warning("Grinex parse error: %s", exc)
        return None, None


async def fetch_bestchange_sell() -> float | None:
    async def _inner():
        html = await _get_html(
            "https://www.bestchange.com/cash-ruble-to-tether-trc20-in-klng.html"
        )
        soup = BeautifulSoup(html, "html.parser")
        div = soup.find("div", class_="fs")
        digits = "".join(c for c in div.text if c.isdigit() or c in ",.")
        return float(digits.replace(",", "."))
    return await _retry(_inner)


async def fetch_bestchange_buy() -> float | None:
    async def _inner():
        html = await _get_html(
            "https://www.bestchange.com/tether-trc20-to-cash-ruble-in-klng.html",
            headers={"User-Agent": "Mozilla/5.0"},
        )
        soup = BeautifulSoup(html, "html.parser")
        row = soup.find("table", id="content_table").find("tr", onclick=True)
        cell = row.find("td", class_="bi")
        digits = "".join(c for c in cell.text if c.isdigit() or c in ",.")
        return float(digits.replace(",", "."))
    return await _retry(_inner)


async def fetch_energotransbank_rate() -> Tuple[float | None, float | None, float | None]:
    async def _inner():
        html = await _get_html(
            "https://ru.myfin.by/bank/energotransbank/currency/kaliningrad",
            headers={"User-Agent": "Mozilla/5.0"},
        )
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table", class_="table-best white_bg")
        usd_cell = table.find("td", class_="title")
        purchase = usd_cell.find_next("td")
        sale = purchase.find_next("td")
        cbr = sale.find_next("td")
        return (
            float(sale.text.replace(",", ".")),
            float(purchase.text.replace(",", ".")),
            float(cbr.text.replace(",", ".")),
        )
    return await _retry(_inner) or (None, None, None)

# ────────── формирование сообщения ──────────
def _build_message(now: str,
                   g_ask, g_bid,
                   bc_sell, bc_buy,
                   e_ask, e_bid, e_cbr) -> str:
    parts: list[str] = [now, "",
                        "KenigSwap USDT/RUB"]
    if g_ask and g_bid:
        parts.append(f"Продажа: {g_ask + KENIG_ASK_OFFSET:.2f} ₽")
        parts.append(f"Покупка: {g_bid + KENIG_BID_OFFSET:.2f} ₽")
    else:
        parts.append("Нет данных.")

    parts += ["", "BestChange USDT/RUB"]
    if bc_sell and bc_buy:
        parts.append(f"Продажа: {bc_sell:.2f} ₽")
        parts.append(f"Покупка: {bc_buy:.2f} ₽")
    else:
        parts.append("Нет данных.")

    parts += ["", "EnergoTransBank USD/RUB"]
    if e_ask and e_bid and e_cbr:
        parts.append(f"Продажа: {e_ask:.2f} ₽")
        parts.append(f"Покупка: {e_bid:.2f} ₽")
        parts.append(f"ЦБ РФ:  {e_cbr:.2f} ₽")
    else:
        parts.append("Нет данных.")

    return "```\\n" + "\\n".join(parts) + "\\n```"

# ────────── Telegram команды ──────────
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Бот активен. Используй /auth <пароль> для доступа."
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "/auth <pwd> – авторизация\n"
        "/check – отправить курсы\n"
        "/change <ask> <bid> – изменить корректировки\n"
        "/show_offsets – показать корректировки"
    )


async def cmd_auth(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) != 1:
        await update.message.reply_text("Введите пароль: /auth <пароль>")
        return
    if context.args[0] == PASSWORD:
        AUTHORIZED_USERS.add(update.effective_user.id)
        await update.message.reply_text("Доступ разрешён.")
    else:
        await update.message.reply_text("Неверный пароль.")


async def cmd_show_offsets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _auth_required(update.effective_user.id):
        await update.message.reply_text("Сначала /auth")
        return
    await update.message.reply_text(
        f"Ask offset: +{KENIG_ASK_OFFSET}\nBid offset: {KENIG_BID_OFFSET}"
    )


async def cmd_change(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _auth_required(update.effective_user.id):
        await update.message.reply_text("Сначала /auth")
        return
    if len(context.args) != 2:
        await update.message.reply_text("Пример: /change 1.2 -0.4")
        return
    global KENIG_ASK_OFFSET, KENIG_BID_OFFSET
    try:
        KENIG_ASK_OFFSET = float(context.args[0])
        KENIG_BID_OFFSET = float(context.args[1])
        await update.message.reply_text("Корректировки обновлены.")
    except ValueError:
        await update.message.reply_text("Некорректные числа.")


async def cmd_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _auth_required(update.effective_user.id):
        await update.message.reply_text("Сначала /auth")
        return
    await send_rates_message(context)
    await update.message.reply_text("Курсы отправлены.")


# ────────── отправка сообщения ──────────
async def send_rates_message(context: CallbackContext | ContextTypes.DEFAULT_TYPE):
    now_str = datetime.now(KALININGRAD_TZ).strftime("%d.%m.%Y %H:%M:%S")

    g_ask, g_bid, bc_sell, bc_buy, (e_ask, e_bid, e_cbr) = await asyncio.gather(
        fetch_grinex_rate(),
        fetch_bestchange_sell(),
        fetch_bestchange_buy(),
        fetch_energotransbank_rate(),
    )

    msg = _build_message(
        now_str, g_ask, g_bid, bc_sell, bc_buy, e_ask, e_bid, e_cbr
    )
    try:
        await context.bot.send_message(
            chat_id=CHAT_ID, text=msg, parse_mode=ParseMode.MARKDOWN
        )
    except Exception as exc:
        logger.error("Ошибка при отправке сообщения: %s", exc)

# ────────── main ──────────
def main() -> None:
    if not TOKEN:
        logger.error("TOKEN env var is empty!")
        sys.exit(1)

    app = (
        ApplicationBuilder()
        .token(TOKEN)
        .build()
    )

    # команды
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("auth", cmd_auth))
    app.add_handler(CommandHandler("check", cmd_check))
    app.add_handler(CommandHandler("change", cmd_change))
    app.add_handler(CommandHandler("show_offsets", cmd_show_offsets))

    # JobQueue — каждые 2:30
    app.job_queue.run_repeating(
        send_rates_message, interval=150, first=0, name="rates"
    )

    logger.info("Bot started")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    asyncio.run(init_playwright())  # pre‑warm браузер
    main()
