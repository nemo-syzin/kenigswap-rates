# ───────────────────── IMPORTS ──────────────────────
import asyncio, contextlib, html, logging, os, subprocess
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

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
CHAT_ID          = "@KaliningradCryptoRatesKenigSwap"
KAL_TZ           = timezone(timedelta(hours=2))

KENIG_ASK_OFFSET = 0.8   # +₽ к ask Grinex → KenigSwap
KENIG_BID_OFFSET = -0.9  # –₽ к bid Grinex → KenigSwap

MAX_RETRIES      = 3
RETRY_DELAY      = 5
AUTHORIZED_USERS: set[int] = set()

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

async def fetch_grinex_rate() -> Tuple[Optional[float], Optional[float]]:
    for att in range(1, MAX_RETRIES + 1):
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=[
                        "--disable-blink-features=AutomationControlled",
                        "--proxy-server='direct://'",
                        "--proxy-bypass-list=*",
                    ],
                )
                ctx  = await browser.new_context(
                    user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                "AppleWebKit/537.36 (KHTML, like Gecko) "
                                "Chrome/123.0.0.0 Safari/537.36")
                )
                page = await ctx.new_page()
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
            log.warning("Grinex attempt %s/%s failed: %s", att, MAX_RETRIES, e)
            if att < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)
    return None, None

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
    bc_sell = await fetch_bestchange_sell()
    bc_buy  = await fetch_bestchange_buy()
    en_sell, en_buy, en_cbr = await fetch_energo()
    gr_ask, gr_bid = await fetch_grinex_rate()

    ts = datetime.now(KAL_TZ).strftime("%d.%m.%Y %H:%M:%S")
    lines = [ts, ""]

    # KenigSwap (Grinex + оффсеты)
    lines += ["KenigSwap rate USDT/RUB"]
    if gr_ask and gr_bid:
        lines.append(f"Продажа: {gr_ask + KENIG_ASK_OFFSET:.2f} ₽, "
                     f"Покупка: {gr_bid + KENIG_BID_OFFSET:.2f} ₽")
    else:
        lines.append("— нет данных —")
    lines.append("")

    # BestChange
    lines += ["BestChange rate USDT/RUB"]
    lines.append(f"Продажа: {bc_sell:.2f} ₽, Покупка: {bc_buy:.2f} ₽"
                 if bc_sell and bc_buy else "— нет данных —")
    lines.append("")

    # EnergoTransBank
    lines += ["EnergoTransBank rate USD/RUB"]
    lines.append(f"Продажа: {en_sell:.2f} ₽, "
                 f"Покупка: {en_buy:.2f} ₽, ЦБ: {en_cbr:.2f} ₽"
                 if en_sell and en_buy and en_cbr else "— нет данных —")

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
