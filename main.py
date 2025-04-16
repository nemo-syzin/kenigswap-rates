from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram.ext import ApplicationBuilder, CommandHandler
import logging
from datetime import datetime, timezone, timedelta
import httpx
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
import subprocess
import asyncio

# ----------------------------------------------------
# Установка Chromium для Playwright
# ----------------------------------------------------
def install_chromium_for_playwright():
    """
    Функция для установки Chromium для Playwright. 
    Запускает команду установки Playwright Chromium в системе.
    """
    try:
        subprocess.run(["playwright", "install", "chromium"], check=True)
        print("✅ Chromium установлен для Playwright.")
    except Exception as e:
        print(f"❌ Не удалось установить Chromium: {e}")

# ----------------------------------------------------
# Логгирование
# ----------------------------------------------------
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ----------------------------------------------------
# Константы
# ----------------------------------------------------
TOKEN = '7128150617:AAHxECcATIDtakWEAy5gp4j2PH-AF80mTAQ'  # Токен для бота Telegram
CHAT_ID = '@KaliningradCryptoKenigSwap'  # Канал для отправки сообщений
KALININGRAD_TZ = timezone(timedelta(hours=2))  # Часовой пояс Калининграда
PASSWORD = "ШУЛЛЕР777"  # Пароль для авторизации

# ----------------------------------------------------
# Переменные корректировки и авторизации
# ----------------------------------------------------
KENIG_ASK_OFFSET = 1.0  # Корректировка для цены продажи
KENIG_BID_OFFSET = -0.5  # Корректировка для цены покупки
AUTHORIZED_USERS = set()  # Сет авторизованных пользователей

# ----------------------------------------------------
# Настройки повторных попыток
# ----------------------------------------------------
MAX_RETRIES = 3  # Максимальное количество попыток для получения данных
RETRY_DELAY = 5  # Задержка между попытками в секундах

# ----------------------------------------------------
# Функции для получения курсов валют
# ----------------------------------------------------

async def fetch_grinex_rate():
    """
    Получает курс с сайта Grinex для валюты USDT/RUB.
    Возвращает курсы покупки и продажи.
    """
    retries = 0
    while retries < MAX_RETRIES:
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context(user_agent="Mozilla/5.0")
                page = await context.new_page()
                await page.goto("https://grinex.io/trading/usdta7a5", timeout=60000)
                await asyncio.sleep(3)  # Подождать, пока страница прогрузится
                await page.wait_for_selector("tbody.asks tr", timeout=30000)
                await page.wait_for_selector("tbody.bids tr", timeout=30000)
                ask_row = await page.query_selector("tbody.asks tr")
                ask_price = float(await ask_row.get_attribute("data-price"))
                bid_row = await page.query_selector("tbody.bids tr")
                bid_price = float(await bid_row.get_attribute("data-price"))
                await browser.close()
                return ask_price, bid_price
        except Exception as e:
            retries += 1
            logger.error(f"Grinex error (attempt {retries}/{MAX_RETRIES}): {str(e)}")
            if retries < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)  # Ожидаем перед повторной попыткой
            else:
                logger.error("Максимальное количество попыток достигнуто. Не удалось получить данные с Grinex.")
                return None, None  # Возвращаем None, если все попытки не удались

async def fetch_bestchange_sell():
    """
    Получает курс продажи с сайта BestChange.
    Возвращает курс продажи в рублях.
    """
    retries = 0
    while retries < MAX_RETRIES:
        try:
            url = "https://www.bestchange.com/cash-ruble-to-tether-trc20-in-klng.html"
            async with httpx.AsyncClient() as client:
                resp = await client.get(url)
                soup = BeautifulSoup(resp.text, 'html.parser')
                div = soup.find('div', class_='fs')
                if div:
                    rate = ''.join(c for c in div.text if c.isdigit() or c in [',', '.']).replace(',', '.')
                    return float(rate)
        except Exception as e:
            retries += 1
            logger.error(f"BestChange sell error (attempt {retries}/{MAX_RETRIES}): {str(e)}")
            if retries < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)  # Ожидаем перед повторной попыткой
            else:
                logger.error("Максимальное количество попыток достигнуто. Не удалось получить данные с BestChange.")
                return None
    return None

async def fetch_bestchange_buy():
    """
    Получает курс покупки с сайта BestChange.
    Возвращает курс покупки в рублях.
    """
    retries = 0
    while retries < MAX_RETRIES:
        try:
            url = "https://www.bestchange.com/tether-trc20-to-cash-ruble-in-klng.html"
            async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}) as client:
                resp = await client.get(url)
                soup = BeautifulSoup(resp.text, "html.parser")
                table = soup.find("table", id="content_table")
                row = table.find("tr", onclick=True)
                cells = row.find_all("td", class_="bi")
                target = next((td for td in cells if "RUB Cash" in td.text), None)
                if target:
                    digits = ''.join(c for c in target.text if c.isdigit() or c in [',', '.']).replace(',', '.')
                    return float(digits)
        except Exception as e:
            retries += 1
            logger.error(f"BestChange buy error (attempt {retries}/{MAX_RETRIES}): {str(e)}")
            if retries < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)  # Ожидаем перед повторной попыткой
            else:
                logger.error("Максимальное количество попыток достигнуто. Не удалось получить данные с BestChange.")
                return None
    return None

async def fetch_energotransbank_rate():
    """
    Получает курс с сайта EnergoTransBank.
    Возвращает курс продажи, покупки и курса ЦБ.
    """
    retries = 0
    while retries < MAX_RETRIES:
        try:
            url = "https://ru.myfin.by/bank/energotransbank/currency/kaliningrad"
            async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}) as client:
                resp = await client.get(url)
                soup = BeautifulSoup(resp.text, 'html.parser')
                table = soup.find('table', class_='table-best white_bg')
                usd_cell = table.find('td', class_='title')
                purchase = usd_cell.find_next('td')
                sale = purchase.find_next('td')
                cbr = sale.find_next('td')
                return float(sale.text.replace(',', '.')), float(purchase.text.replace(',', '.')), float(cbr.text.replace(',', '.'))
        except Exception as e:
            retries += 1
            logger.error(f"EnergoTransBank error (attempt {retries}/{MAX_RETRIES}): {str(e)}")
            if retries < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)  # Ожидаем перед повторной попыткой
            else:
                logger.error("Максимальное количество попыток достигнуто. Не удалось получить данные с EnergoTransBank.")
                return None, None, None
    return None, None, None

# ----------------------------------------------------
# УТИЛИТЫ ДОСТУПА
# ----------------------------------------------------

def is_authorized(user_id):
    """
    Проверка авторизации пользователя.
    Возвращает True, если пользователь авторизован, иначе False.
    """
    return user_id in AUTHORIZED_USERS

# ----------------------------------------------------
# TELEGRAM КОМАНДЫ
# ----------------------------------------------------

async def auth(update, context):
    """
    Команда для авторизации пользователя.
    Требует ввода пароля.
    """
    user_id = update.effective_user.id
    if len(context.args) != 1:
        await update.message.reply_text("Введите пароль. Пример: /auth ШУЛЛЕР")
        return
    if context.args[0] == PASSWORD:
        AUTHORIZED_USERS.add(user_id)
        await update.message.reply_text("Доступ разрешён.")
    else:
        await update.message.reply_text("Неверный пароль.")

async def check(update, context):
    """
    Команда для отправки курсов валют в канал.
    Требует авторизации.
    """
    if not is_authorized(update.effective_user.id):
        await update.message.reply_text("Введите пароль через /auth <пароль>")
        return
    await send_rates_message(context.application)
    await update.message.reply_text("Курсы отправлены в канал.")

async def change_offsets(update, context):
    """
    Команда для изменения корректировок курса.
    Требует авторизации.
    """
    if not is_authorized(update.effective_user.id):
        await update.message.reply_text("Введите пароль через /auth <пароль>")
        return
    try:
        global KENIG_ASK_OFFSET, KENIG_BID_OFFSET
        if len(context.args) != 2:
            raise ValueError("Пример: /change 1.2 -0.4")
        KENIG_ASK_OFFSET = float(context.args[0])
        KENIG_BID_OFFSET = float(context.args[1])
        await update.message.reply_text(f"Обновлено:\nAsk offset: +{KENIG_ASK_OFFSET}\nBid offset: {KENIG_BID_OFFSET}")
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")

async def show_offsets(update, context):
    """
    Команда для отображения текущих корректировок курса.
    Требует авторизации.
    """
    if not is_authorized(update.effective_user.id):
        await update.message.reply_text("Введите пароль через /auth <пароль>")
        return
    await update.message.reply_text(f"Текущие корректировки:\nAsk offset: +{KENIG_ASK_OFFSET}\nBid offset: {KENIG_BID_OFFSET}")

async def start(update, context):
    """
    Стартовая команда для активации бота.
    """
    await update.message.reply_text("Бот активен. Используй /auth <пароль> для доступа к командам.")

async def help_command(update, context):
    """
    Команда для вывода списка всех доступных команд.
    """
    help_text = (
        "Доступные команды:\n\n"
        "/start - Активирует бота\n"
        "/auth <пароль> - Авторизация\n"
        "/check - Отправка курсов валют в канал\n"
        "/change <ask_offset> <bid_offset> - Изменение корректировок курсов\n"
        "/show_offsets - Отображение текущих корректировок\n"
        "/help - Выводит список всех команд"
    )
    await update.message.reply_text(help_text)

# ----------------------------------------------------
# ОТПРАВКА СООБЩЕНИЯ
# ----------------------------------------------------

async def send_rates_message(application):
    """
    Функция для получения курсов с различных источников и отправки сообщений в канал.
    """
    bestchange_sell = await fetch_bestchange_sell()
    bestchange_buy = await fetch_bestchange_buy()
    energo_ask, energo_bid, energo_cbr = await fetch_energotransbank_rate()
    grinex_ask, grinex_bid = await fetch_grinex_rate()

    now = datetime.now(KALININGRAD_TZ).strftime('%d.%m.%Y %H:%M:%S')
    lines = [f"{now}\n"]

    # KenigSwap от Grinex
    lines.append("KenigSwap rate USDT/RUB")
    if grinex_ask is not None and grinex_bid is not None:
        kenig_ask = grinex_ask + KENIG_ASK_OFFSET
        kenig_bid = grinex_bid + KENIG_BID_OFFSET
        lines.append(f"Продажа: {kenig_ask:.2f} ₽, Покупка: {kenig_bid:.2f} ₽")
    else:
        lines.append("Нет данных с Grinex.")
    lines.append("")

    # BestChange
    lines.append("BestChange rate USDT/RUB")
    if bestchange_sell and bestchange_buy:
        lines.append(f"Продажа: {bestchange_sell:.2f} ₽, Покупка: {bestchange_buy:.2f} ₽")
    else:
        lines.append("Нет данных с BestChange.")
    lines.append("")

    # Energo
    lines.append("EnergoTransBank rate USD/RUB")
    if energo_ask and energo_bid and energo_cbr:
        lines.append(f"Продажа: {energo_ask:.2f} ₽, Покупка: {energo_bid:.2f} ₽, ЦБ: {energo_cbr:.2f} ₽")
    else:
        lines.append("Нет данных с EnergoTransBank.")

    message = f"```\n{chr(10).join(lines)}\n```"
    try:
        await application.bot.send_message(chat_id=CHAT_ID, text=message, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения: {e}")

# ----------------------------------------------------
# MAIN
# ----------------------------------------------------

def main():
    """
    Основная функция, которая запускает бота.
    """
    install_chromium_for_playwright()

    application = ApplicationBuilder().token(TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("auth", auth))
    application.add_handler(CommandHandler("check", check))
    application.add_handler(CommandHandler("change", change_offsets))
    application.add_handler(CommandHandler("show_offsets", show_offsets))
    application.add_handler(CommandHandler("help", help_command))

    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        send_rates_message,
        'interval',
        minutes=2, seconds=30,
        timezone=KALININGRAD_TZ,
        args=[application]
    )
    scheduler.start()

    logger.info("Бот запущен.")
    application.run_polling()

if __name__ == '__main__':
    main()
