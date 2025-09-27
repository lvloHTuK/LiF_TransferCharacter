import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes, \
    ConversationHandler
import aiohttp
import re
import pika
import threading
import time
import signal
import sys
import json

# Настройки API
API_BASE_URL = "http://localhost:8001"  # Замените на ваш URL API

# Состояния разговора
SELECT_EVENT, SELECT_SERVER, SELECT_GENERAL, SELECT_LORD, ENTER_PASSWORD, SELECT_PARTICIPANT, ENTER_STEAM_ID, RESTART = range(
    8)

# Настройка логирования
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class RabbitMQManager:
    """Менеджер для управления постоянным подключением к RabbitMQ"""
    
    def __init__(self, host='localhost', port=5672, max_retries=5, retry_delay=5):
        self.host = host
        self.port = port
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.connection = None
        self.channel = None
        self.lock = threading.Lock()
        self.is_connected = False
        self._connect()
    
    def _connect(self):
        """Устанавливает соединение с RabbitMQ"""
        # Закрываем существующие соединения если они есть
        if self.connection and not self.connection.is_closed:
            try:
                self.connection.close()
            except:
                pass
        
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Попытка подключения к RabbitMQ (попытка {attempt + 1}/{self.max_retries})")
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=self.host,
                        port=self.port,
                        heartbeat=600,  # 10 минут
                        blocked_connection_timeout=300,  # 5 минут
                        retry_delay=2
                    )
                )
                self.channel = self.connection.channel()
                self.is_connected = True
                logger.info("Успешно подключились к RabbitMQ")
                return
            except Exception as e:
                logger.error(f"Ошибка подключения к RabbitMQ (попытка {attempt + 1}): {e}")
                # Сбрасываем состояние
                self.is_connected = False
                self.connection = None
                self.channel = None
                
                if attempt < self.max_retries - 1:
                    logger.info(f"Повторная попытка через {self.retry_delay} секунд...")
                    time.sleep(self.retry_delay)
                else:
                    logger.error("Не удалось подключиться к RabbitMQ после всех попыток")
                    self.is_connected = False
    
    def _ensure_connection(self):
        """Проверяет и восстанавливает соединение при необходимости"""
        with self.lock:
            # Проверяем различные условия потери соединения
            connection_lost = (
                not self.is_connected or 
                not self.connection or 
                self.connection.is_closed or 
                not self.channel or 
                self.channel.is_closed
            )
            
            if connection_lost:
                logger.info("Соединение или канал потеряны, пытаемся восстановить...")
                self._connect()
    
    def send_message(self, queue_name, message):
        """Отправляет сообщение в очередь"""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self._ensure_connection()
                
                if not self.is_connected:
                    logger.error("Нет подключения к RabbitMQ, сообщение не отправлено")
                    return False
                
                # Проверяем, существует ли очередь, если нет - создаем
                try:
                    self.channel.queue_declare(queue=queue_name, passive=True)
                    logger.debug(f"Очередь '{queue_name}' уже существует")
                except pika.exceptions.ChannelClosedByBroker as e:
                    if e.reply_code == 404:
                        logger.info(f"Очередь '{queue_name}' не существует, создаем новую")
                        self.channel = self.connection.channel()
                        self.channel.queue_declare(queue=queue_name, durable=True)
                    else:
                        logger.error(f"Ошибка при проверке очереди: {e}")
                        # Принудительно переподключаемся
                        self.is_connected = False
                        retry_count += 1
                        continue
                except Exception as e:
                    logger.error(f"Ошибка при работе с очередью: {e}")
                    self.is_connected = False
                    retry_count += 1
                    continue
                
                # Отправляем сообщение
                self.channel.basic_publish(
                    exchange="",
                    routing_key=queue_name,
                    body=message,
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # Делаем сообщение постоянным
                    )
                )
                
                logger.info(f"Сообщение успешно отправлено в очередь '{queue_name}': {message.strip()}")
                return True
                
            except pika.exceptions.AMQPConnectionError as e:
                logger.error(f"Ошибка подключения к RabbitMQ (попытка {retry_count + 1}): {e}")
                self.is_connected = False
                retry_count += 1
                if retry_count < max_retries:
                    time.sleep(1)  # Небольшая задержка перед повтором
                continue
            except pika.exceptions.ChannelClosedByBroker as e:
                logger.error(f"Канал закрыт брокером (попытка {retry_count + 1}): {e}")
                self.is_connected = False
                retry_count += 1
                if retry_count < max_retries:
                    time.sleep(1)
                continue
            except Exception as e:
                logger.error(f"Неожиданная ошибка при отправке в RabbitMQ (попытка {retry_count + 1}): {e}")
                self.is_connected = False
                retry_count += 1
                if retry_count < max_retries:
                    time.sleep(1)
                continue
        
        logger.error(f"Не удалось отправить сообщение после {max_retries} попыток")
        return False
    
    def close(self):
        """Закрывает соединение с RabbitMQ"""
        with self.lock:
            if self.connection and not self.connection.is_closed:
                try:
                    self.connection.close()
                    logger.info("Соединение с RabbitMQ закрыто")
                except Exception as e:
                    logger.error(f"Ошибка при закрытии соединения: {e}")
                finally:
                    self.is_connected = False


# Глобальный экземпляр менеджера RabbitMQ
rabbitmq_manager = RabbitMQManager()

# Глобальная переменная для корректного завершения
shutdown_requested = False

def signal_handler(signum, frame):
    """Обработчик сигналов для корректного завершения работы"""
    global shutdown_requested
    logger.info(f"Получен сигнал {signum}, инициируем завершение работы...")
    shutdown_requested = True
    sys.exit(0)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # Очищаем данные пользователя при новом запуске
    context.user_data.clear()

    if update.message:
        await update.message.reply_text(
            "Выбери мероприятие:",
            reply_markup=await get_events_keyboard()
        )
    elif update.callback_query:
        await update.callback_query.message.reply_text(
            "Выбери мероприятие:",
            reply_markup=await get_events_keyboard()
        )
        await update.callback_query.answer()

    return SELECT_EVENT


async def get_events_keyboard():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{API_BASE_URL}/events/") as response:
                events = await response.json()
                keyboard = []
                for event in events:
                    keyboard.append([InlineKeyboardButton(event["name"], callback_data=f"event_{event['id']}")])
                return InlineKeyboardMarkup(keyboard)
    except Exception as e:
        logger.error(f"Error getting events: {e}")
        return InlineKeyboardMarkup([])


async def button_events(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    if query.data == "restart":
        # Очищаем данные и начинаем заново
        context.user_data.clear()
        await query.edit_message_text(
            "Выбери мероприятие:",
            reply_markup=await get_events_keyboard()
        )
        return SELECT_EVENT

    event_id = int(query.data.split("_")[1])
    context.user_data["event_id"] = event_id
    await query.edit_message_text(
        "Теперь выбери сервер:",
        reply_markup=await get_servers_keyboard(event_id)
    )
    return SELECT_SERVER


async def get_servers_keyboard(event_id):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{API_BASE_URL}/events/{event_id}/servers/") as response:
                servers = await response.json()
                keyboard = []
                for server in servers:
                    keyboard.append([InlineKeyboardButton(server["name"], callback_data=f"server_{server['id']}")])
                # Добавляем кнопку "Назад"
                keyboard.append([InlineKeyboardButton("« Назад", callback_data="back_to_events")])
                return InlineKeyboardMarkup(keyboard)
    except Exception as e:
        logger.error(f"Error getting servers: {e}")
        return InlineKeyboardMarkup([])


async def button_servers(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    if query.data == "back_to_events":
        # Возвращаемся к выбору мероприятия
        await query.edit_message_text(
            "Выбери мероприятие:",
            reply_markup=await get_events_keyboard()
        )
        return SELECT_EVENT

    server_id = int(query.data.split("_")[1])
    context.user_data["server_id"] = server_id
    await query.edit_message_text(
        "Теперь выбери генерала:",
        reply_markup=await get_generals_keyboard(server_id)
    )
    return SELECT_GENERAL


async def get_generals_keyboard(server_id):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{API_BASE_URL}/servers/{server_id}/generals/") as response:
                generals = await response.json()
                keyboard = []
                for general in generals:
                    keyboard.append([InlineKeyboardButton(general["name"], callback_data=f"general_{general['id']}")])
                # Добавляем кнопку "Назад"
                keyboard.append([InlineKeyboardButton("« Назад", callback_data="back_to_servers")])
                return InlineKeyboardMarkup(keyboard)
    except Exception as e:
        logger.error(f"Error getting generals: {e}")
        return InlineKeyboardMarkup([])


async def button_generals(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    if query.data == "back_to_servers":
        # Возвращаемся к выбору сервера
        event_id = context.user_data["event_id"]
        await query.edit_message_text(
            "Теперь выбери сервер:",
            reply_markup=await get_servers_keyboard(event_id)
        )
        return SELECT_SERVER

    general_id = int(query.data.split("_")[1])
    context.user_data["general_id"] = general_id
    await query.edit_message_text(
        "Теперь выбери лорда:",
        reply_markup=await get_lords_keyboard(general_id)
    )
    return SELECT_LORD


async def get_lords_keyboard(general_id):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{API_BASE_URL}/generals/{general_id}/lords/") as response:
                lords = await response.json()
                keyboard = []
                for lord in lords:
                    keyboard.append([InlineKeyboardButton(lord["name"], callback_data=f"lord_{lord['id']}")])
                # Добавляем кнопку "Назад"
                keyboard.append([InlineKeyboardButton("« Назад", callback_data="back_to_generals")])
                return InlineKeyboardMarkup(keyboard)
    except Exception as e:
        logger.error(f"Error getting lords: {e}")
        return InlineKeyboardMarkup([])


async def button_lords(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    if query.data == "back_to_generals":
        # Возвращаемся к выбору генерала
        server_id = context.user_data["server_id"]
        await query.edit_message_text(
            "Теперь выбери генерала:",
            reply_markup=await get_generals_keyboard(server_id)
        )
        return SELECT_GENERAL

    lord_id = int(query.data.split("_")[1])
    context.user_data["lord_id"] = lord_id
    await query.edit_message_text("Введите пароль доступа:")
    return ENTER_PASSWORD


async def password_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # Обработка команды "Назад"
    if update.message.text.lower() in ["назад", "back", "отмена", "cancel"]:
        # Возвращаемся к выбору лорда
        general_id = context.user_data["general_id"]
        await update.message.reply_text(
            "Теперь выбери лорда:",
            reply_markup=await get_lords_keyboard(general_id)
        )
        return SELECT_LORD

    password = update.message.text
    lord_id = context.user_data["lord_id"]

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{API_BASE_URL}/lords/{lord_id}") as response:
                if response.status == 200:
                    lord = await response.json()
                    if password == lord["password"]:
                        await update.message.reply_text(
                            "Пароль верен. Теперь выбери участника:",
                            reply_markup=await get_participants_keyboard(lord_id)
                        )
                        return SELECT_PARTICIPANT
                    else:
                        await update.message.reply_text(
                            "Неверный пароль. Попробуйте еще раз или введите 'назад' для возврата:")
                        return ENTER_PASSWORD
                else:
                    await update.message.reply_text("Ошибка доступа. Начните сначала /start")
                    return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error verifying password: {e}")
        await update.message.reply_text("Ошибка доступа. Начните сначала /start")
        return ConversationHandler.END


async def get_participants_keyboard(lord_id):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{API_BASE_URL}/lords/{lord_id}/participants/") as response:
                participants = await response.json()
                keyboard = []
                for participant in participants:
                    keyboard.append(
                        [InlineKeyboardButton(participant["fio"], callback_data=f"participant_{participant['id']}")])
                # Изменяем кнопку "Назад" для возврата к выбору лорда
                keyboard.append([InlineKeyboardButton("« Назад", callback_data="back_to_lords")])
                return InlineKeyboardMarkup(keyboard)
    except Exception as e:
        logger.error(f"Error getting participants: {e}")
        return InlineKeyboardMarkup([])


async def button_participants(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    if query.data == "back_to_lords":
        # Возвращаемся к выбору лорда
        general_id = context.user_data["general_id"]
        await query.edit_message_text(
            "Теперь выбери лорда:",
            reply_markup=await get_lords_keyboard(general_id)
        )
        return SELECT_LORD

    participant_id = int(query.data.split("_")[1])
    context.user_data["participant_id"] = participant_id

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{API_BASE_URL}/participants/{participant_id}") as response:
                participant = await response.json()
                context.user_data["player_id"] = participant["player_id"]
                context.user_data["fio"] = participant["fio"]

        await query.edit_message_text("Введите ваш Steam ID (2-4 цифр) или 'назад' для возврата:")
        return ENTER_STEAM_ID
    except Exception as e:
        logger.error(f"Error getting participant: {e}")
        await query.edit_message_text("Ошибка получения данных участника. Начните сначала /start")
        return ConversationHandler.END


async def steam_id_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # Обработка команды "Назад"
    if update.message.text.lower() in ["назад", "back", "отмена", "cancel"]:
        # Возвращаемся к выбору участника
        lord_id = context.user_data["lord_id"]
        await update.message.reply_text(
            "Теперь выбери участника:",
            reply_markup=await get_participants_keyboard(lord_id)
        )
        return SELECT_PARTICIPANT

    steam_id = update.message.text
    if re.match(r"^\d{2,4}$", steam_id):
        # Получаем данные участника из базы
        participant_id = context.user_data["participant_id"]
        server_id = context.user_data["server_id"]

        try:
            # Получаем информацию о сервере
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{API_BASE_URL}/servers/{server_id}") as response:
                    if response.status == 200:
                        server = await response.json()
                        server_name = server["server_name"]
                    else:
                        server_name = "Unknown"

            # Получаем данные участника
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{API_BASE_URL}/participants/{participant_id}") as response:
                    if response.status == 200:
                        participant = await response.json()
                        player_id = participant["player_id"]

                        # Получаем информацию о пользователе Telegram
                        tg_user = update.message.from_user
                        tg_name = tg_user.username if tg_user.username else tg_user.first_name

                        message = {
                            "ServerName": server_name,
                            "PlayerID": player_id,
                            "SteamID": steam_id,
                            "TgName": tg_name
                        }
                        json_message = json.dumps(message, indent=4)

                        # Отправляем в RabbitMQ
                        rabbitmq_success = await rabbitMQSend(server_name, json_message)

                        # Запись в файл в новом формате (резервное копирование)
                        file_success = False
                        try:
                            with open("registrations.txt", "a", encoding="utf-8") as f:
                                f.write(message)
                            file_success = True
                            logger.info("Данные сохранены в файл registrations.txt")
                        except Exception as e:
                            logger.error(f"Ошибка при записи в файл: {e}")

                        # Создаем клавиатуру с кнопкой "Продолжить"
                        continue_keyboard = InlineKeyboardMarkup([
                            [InlineKeyboardButton("Продолжить", callback_data="restart")]
                        ])

                        # Формируем сообщение о результате
                        if rabbitmq_success and file_success:
                            result_message = "Запрос на перенос завершен успешно! Немного подождите.\n\n"
                        elif rabbitmq_success:
                            result_message = "Запрос на перенос завершен! Немного подождите.\n\n"
                        elif file_success:
                            result_message = "Запрос на перенос завершен! Обратитесь к администратору (Брокер недоступен).\n\n"
                        else:
                            result_message = "Запрос на перенос завершен, но возникли проблемы с сохранением данных. Обратитесь к администратору\n\n"

                        await update.message.reply_text(
                            result_message + "Нажмите 'Продолжить' для новой регистрации.",
                            reply_markup=continue_keyboard
                        )
                        return RESTART
                    else:
                        await update.message.reply_text(
                            "Ошибка при получении данных участника. Попробуйте снова /start")
                        return ConversationHandler.END
        except Exception as e:
            logger.error(f"Error completing registration: {e}")
            await update.message.reply_text("Ошибка при завершении регистрации. Попробуйте снова /start")
            return ConversationHandler.END
    else:
        await update.message.reply_text("Неверный формат Steam ID. Введите 2-4 цифр или 'назад' для возврата:")
        return ENTER_STEAM_ID


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Регистрация отменена.")
    return ConversationHandler.END

async def rabbitMQSend(server_name, message):
    try:
        logger.info(f"Попытка отправки сообщения в очередь '{server_name}': {message.strip()}")
        
        # Проверяем состояние соединения перед отправкой
        if not rabbitmq_manager.is_connected:
            logger.warning("RabbitMQ соединение не активно, пытаемся восстановить...")
        
        # Используем глобальный менеджер RabbitMQ
        success = rabbitmq_manager.send_message(server_name, message)
        
        if success:
            logger.info(f"Сообщение успешно отправлено в очередь '{server_name}'")
        else:
            logger.error(f"Не удалось отправить сообщение в очередь '{server_name}'")
            
        return success
        
    except Exception as e:
        logger.error(f"Неожиданная ошибка при отправке в RabbitMQ: {e}")
        logger.error(f"Тип ошибки: {type(e).__name__}")
        return False

def main() -> None:
    # Регистрируем обработчики сигналов
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        application = Application.builder().token("7510246230:AAEPScVlKoePbGM61uO5lwTF886MuOXLhBI").build()

        conv_handler = ConversationHandler(
            entry_points=[CommandHandler("start", start)],
            states={
                SELECT_EVENT: [CallbackQueryHandler(button_events, pattern="^(event_|restart)")],
                SELECT_SERVER: [CallbackQueryHandler(button_servers, pattern="^(server_|back_to_events)")],
                SELECT_GENERAL: [CallbackQueryHandler(button_generals, pattern="^(general_|back_to_servers)")],
                SELECT_LORD: [CallbackQueryHandler(button_lords, pattern="^(lord_|back_to_generals)")],
                ENTER_PASSWORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, password_input)],
                SELECT_PARTICIPANT: [CallbackQueryHandler(button_participants, pattern="^(participant_|back_to_lords)")],
                ENTER_STEAM_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, steam_id_input)],
                RESTART: [CallbackQueryHandler(button_events, pattern="^(event_|restart)")],
            },
            fallbacks=[CommandHandler("cancel", cancel)],
        )

        application.add_handler(conv_handler)
        
        logger.info("Запуск Telegram бота...")
        logger.info("RabbitMQ подключение установлено при запуске")
        application.run_polling()
        
    except KeyboardInterrupt:
        logger.info("Получен сигнал завершения работы...")
    except Exception as e:
        logger.error(f"Ошибка при работе бота: {e}")
    finally:
        # Корректно закрываем соединение с RabbitMQ
        logger.info("Закрытие соединения с RabbitMQ...")
        rabbitmq_manager.close()
        logger.info("Бот завершил работу")


if __name__ == "__main__":
    main()