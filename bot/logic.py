"""
   Данный модуль содержит в себе всю логику бота.
   1. Обходим все биржи и асинхронно запускаем соеденение websocket для всех валютных пар.
   2. Запускаем функционал арбитража которых находит выгодные связки и затем отправляет их пользователем тг
   3. Тажке запускается функция напоминание пользователю о том что бот работет, в том случае если давно не находились связки,
      напоминание отправляются каждые 2 часа.
"""

from __future__ import annotations
import asyncio
import os

import asyncpg
import aioschedule as schedule
from dotenv import load_dotenv

from logger_ import logger
from db import ClientDataBase, dict_fetch_query
from ws_client import WSClient
from handler import Handler, HandlerWS, Arbitrator

load_dotenv()

PASSWORD_DB = os.getenv('password_db')
USER_DB = os.getenv('user_db')
NAME_DB = os.getenv('name_db')


async def for_users(bot: aiogram.bot.bot.Bot, client_db: object, pool: object):
    '''Функция отправки напоминаний'''
    for i in await client_db.run_fetch(pool, dict_fetch_query['all_id']):
        logger.info('Отправляю напоминание')
        await bot.send_message(i[0], 'Я про вас не забыл!\nПросто сейчас нет подходящих связок')

async def run_schedule(func, *args):
    '''Функция запуска напоминание в определенное время'''
    schedule.every(2).hours.do(func, args[0], args[1], args[2])
    while True:
        await schedule.run_pending()
        await asyncio.sleep(0.1)

async def run_logic(bot: aiogram.bot.bot.Bot=None):
    await asyncio.sleep(10)
    try:

        pool = await asyncpg.create_pool(user=USER_DB,
                                         password=PASSWORD_DB,
                                         database=NAME_DB,
                                         port='5432',
                                         host='postgres', # здесь необходимо указать имя контейнера
                                         max_size=1000
                                         ) # создаю пул подключений к бд

        client_db = ClientDataBase() # создаю экземпляр клиента бд
        tasks = [] # список для сбора задач

        for exchange in await client_db.run_fetch(pool, dict_fetch_query['all_exchanges']):
            logger.info(f'Запустилась обработка на биржей: {exchange[0]}')
            for obj_price in await client_db.run_fetch(pool, dict_fetch_query['obj_price_by_name_exchange'], exchange[0]):
                logger.info(f'Обработка валютной пары: {obj_price[1]}')
                handler = Handler(exchange[3], obj_price[2], obj_price[0])

                logger.info(f'Обработка валютной пары: {obj_price[1]} завершена')
                ws = WSClient(exchange[2], handler.return_handler(), exchange[4], HandlerWS, client_db, pool, obj_price[1], obj_price[0])

                logger.info(f'Запуск WebSocket: {obj_price[0]}     {obj_price[1]}')
                tasks.append(ws.run_socket())
                logger.info(f'Корутина run_socket добавлена в таск: {obj_price[0]}      {obj_price[1]}')

        logger.info('Создал список с валютными парами')
        set_pairs = set([i for i in await client_db.run_fetch(pool, dict_fetch_query['all_actual_pair']) for i in i])

        logger.info('Сейчас будет запушен Arbitrator')

        for pair in set_pairs:
            arbiter = Arbitrator(client_db, pool, pair, bot)
            logger.info(f'Arbiter запущен для {pair}')
            tasks.append(arbiter.calculation())
            logger.info(f'Arbiter для {pair} добавлен в таск')

        logger.info('Сейчас будет добавлена функция напоминание в таск')
        tasks.append(run_schedule(for_users, bot, client_db, pool))
        await asyncio.gather(*tasks)
        logger.info('Gather')

    except Exception as exception:
        logger.warning(f'В ходе работы run_logic произошел сбой: {exception}')



