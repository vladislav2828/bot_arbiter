'''
    Этот модуль отвечает за работу бота.
    Запускает вебхук через ngrok, при завершении работы удаляет вебхук.
    А также при запуске бота запускается модуль logoc отвечающий за всю логику работы бота.
'''

import os
import asyncio
import asyncpg
import time

from pyngrok import ngrok
from dotenv import load_dotenv
from aiogram import Bot, types
from aiogram.dispatcher import Dispatcher
from aiogram.dispatcher.webhook import SendMessage
from aiogram.utils.executor import start_webhook

from logic import run_logic, PASSWORD_DB, USER_DB, NAME_DB
from db import ClientDataBase, dict_fetch_query, dict_execute_query
from logger_ import logger

load_dotenv()

client_db = ClientDataBase() # клиент бд

API_TOKEN = os.getenv('token_bot') # ваш токен

WEBAPP_HOST = '127.0.0.1' # хост приложения
WEBAPP_PORT = 5000 # порт

bot = Bot(token=API_TOKEN, parse_mode='HTML') # объект бота
dp = Dispatcher(bot) # диспетчер

def return_url_ngrok():
    '''
    Функция возвращает туннелированный url с помощью ngrok
    '''
    try:
        return ngrok.connect(5000, bind_tls=True).public_url
    except Exception as exception:
        logger.warning('При получении ngrok туннеля произошла ошибка в функции return_url_ngrok!', exception)
        while True:
            time.sleep(2)
            return ngrok.connect(5000, bind_tls=True).public_url

@dp.message_handler(commands=['start'])
async def start_response(message: types.Message):

        async with asyncpg.create_pool(user=USER_DB, password=PASSWORD_DB, database=NAME_DB, port='5432', host='postgres') as pool:
            if await client_db.run_fetch(pool, dict_fetch_query['user_by_id'], message.chat.id): # проверка регистрации пользователя.
                return SendMessage(message.chat.id, 'Я про вас не забыл, ищу связки!')

            else: # если пользователь не зарегестрирован
                logger.info(f'Создаю нового пользователя: ID={message.chat.id}   FIRST_NAME={message.chat.first_name}')
                await client_db.run_execute(pool, dict_execute_query['add_user'], message.chat.id, message.chat.first_name, int(time.time()))
                logger.info('Пользователь создан')
                return SendMessage(message.chat.id, 'Рад приветствовать тебя мой друг!\nТеперь я буду присылать тебе связки для арбитража.\nНемного ожидание...')


@dp.message_handler()
async def response(message: types.Message):
    '''
    Ответ на прочие сообщения.
    '''
    return SendMessage(message.chat.id, 'Упс, я такой команды не знаю!')

async def on_startup(dp):
    try:
        '''
        Функция отрабатывает при активации бота.
        Запускает логику поиска связок.
        '''
        await bot.set_webhook(return_url_ngrok())
        return asyncio.create_task(run_logic(bot))
    except Exception as exception:
        logger.warning('При запуске бота произошла ошибка в функции on_startup!', exception)


async def on_shutdown(dp):
    '''
    Функция отрабатывает при отключении бота.
    '''
    await bot.delete_webhook()
    await dp.storage.close()
    await dp.storage.wait_closed()
    logger.info('Бот отключается')


if __name__ == '__main__':
    try:
        start_webhook(
            dispatcher=dp,
            webhook_path='',
            on_startup=on_startup,
            on_shutdown=on_shutdown,
            skip_updates=False,
            host=WEBAPP_HOST,
            port=WEBAPP_PORT,
        )
    except Exception as exception:
        logger.warning(f'Ошибка при запуске бота: {exception}')

